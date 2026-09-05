#!/usr/bin/env python3
"""
Futu (OpenD) market-data bridge for the gtv `market::FutuProvider`.

Contract (JSON on stdout):
    --action klines|ticks  --code HK.00700  --period 1d
    [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--max N] [--adjust qfq|hfq|none]
    [--host 127.0.0.1] [--port 11111]

Output:
    {"ok": true, "rows": [ ... ] }     rows: [{ts_ns, open, high, ...}] (klines)
                                       or    [{ts_ns, price, volume, turnover, direction, sequence}] (ticks)
    {"ok": false, "error": "..."}

* All timestamps are converted to UTC epoch nanoseconds. Futu reports K-line
  / tick timestamps in Beijing time (HK/A) or US Eastern time (US); we apply
  the market-appropriate offset for sub-daily granularity so the values are
  comparable across providers in the temporal engine. Bars at daily-and-coarser
  granularity keep their calendar date (UTC midnight of the label date).
* Non-finite floats are emitted as JSON null (Rust decodes them to NaN in the
  Arrow column) — plain JSON cannot carry NaN.
* Historical K-lines come from OpenD `request_history_kline` with keyset
  pagination (`page_req_key`); ticks are the current session's prints via a
  `TICKER` subscription (`get_rt_ticker`) — Futu does not expose historical
  trade-by-trade dumps, so tick rows only exist while the market is open.

Run standalone for debugging:
    python3 futu_bridge.py --action klines --code HK.00700 --period 1d \
        --start 2024-06-01 --end 2024-06-30
"""
import argparse
import json
import math
import os
import sys
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------------------
# Time handling
# ---------------------------------------------------------------------------

SEC_DAY = 86400

def _us_utc_offset(dt):
    """US Eastern: EST (UTC-5) outside DST, EDT (UTC-4) inside DST.
    DST: 02:00 on the 2nd Sunday of March .. 02:00 on the 1st Sunday of Nov."""
    def nth_sunday(year, month, n):
        d = date(year, month, 1)
        first_sun = d + timedelta(days=(6 - d.weekday()) % 7)
        return first_sun + timedelta(weeks=n - 1)
    dst_start = nth_sunday(dt.year, 3, 2)
    dst_end = nth_sunday(dt.year, 11, 1)
    in_dst = dst_start <= dt.date() < dst_end
    return -4 * 3600 if in_dst else -5 * 3600


def market_offset_seconds(code, dt, sub_daily):
    """Futu reports exchange-local clock time; map to UTC offset."""
    if not sub_daily:
        # Daily-and-coarser bars keep their calendar label date as UTC midnight.
        return 0
    c = code.upper()
    if c.startswith(("HK.", "SH.", "SZ.")):
        return 8 * 3600          # Beijing time
    if c.startswith("US."):
        return _us_utc_offset(dt)
    if c.startswith(("SG.", "JP.", "AU.")):
        return 0                 # reported in the exchange zone == UTC? no —
    return 0                     # unknown markets: treat label as UTC (documented)


def ts_ns(code, time_str, sub_daily):
    """Parse 'YYYY-MM-DD HH:MM:SS[.fff]' (Futu wire format) -> epoch ns UTC."""
    if not time_str:
        return None
    s = time_str.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    else:
        # Fall back to raw date only.
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None
    offset = market_offset_seconds(code, dt, sub_daily)
    utc = dt - timedelta(seconds=offset)
    return int(utc.replace(tzinfo=None).timestamp() * 1_000_000_000)


def sub_daily(period):
    return period.lower() in ("1m", "3m", "5m", "15m", "30m", "60m")

# ---------------------------------------------------------------------------
# Period -> seconds (for auto lookback windows)
# ---------------------------------------------------------------------------

PERIOD_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "60m": 3600,
    "1d": SEC_DAY, "1w": 7 * SEC_DAY, "1M": 30 * SEC_DAY,
    "1Q": 91 * SEC_DAY, "1Y": 365 * SEC_DAY,
}

def lookback_days(period, n_bars):
    """Calendar days that roughly contain n_bars of `period`. Sub-daily bars
    only occur inside trading hours, so stretch the window ~3x."""
    bar_sec = PERIOD_SECONDS.get(period.lower(), SEC_DAY)
    stretch = 3 if sub_daily(period) else 1
    days = max(2, int(bar_sec * n_bars * stretch / SEC_DAY))
    # Futu depth caps: 8 years for minute bars, 20 years for daily+.
    cap = 8 * 366 if sub_daily(period) else 20 * 366
    return min(days, cap)

# ---------------------------------------------------------------------------
# JSON helpers (no NaN allowed in strict JSON)
# ---------------------------------------------------------------------------

def jnum(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def jint(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------------
# Futu actions
# ---------------------------------------------------------------------------

def load_futu():
    from futu import OpenQuoteContext, RET_OK, KLType, AuType, SubType
    try:
        # Console INFO logs (connect/disconnect) go to stdout and would corrupt
        # our JSON payload — silence them; real errors still reach WARNING+.
        from futu.common import set_debug_model
        set_debug_model(False)
    except Exception:
        pass
    KTYPE = {
        "1m": KLType.K_1M, "3m": KLType.K_3M, "5m": KLType.K_5M,
        "15m": KLType.K_15M, "30m": KLType.K_30M, "60m": KLType.K_60M,
        "1d": KLType.K_DAY, "1w": KLType.K_WEEK, "1M": KLType.K_MON,
        "1Q": KLType.K_QUARTER, "1Y": KLType.K_YEAR,
    }
    SUBTYPE = {
        "1m": SubType.K_1M, "5m": SubType.K_5M, "15m": SubType.K_15M,
        "60m": SubType.K_60M, "1d": SubType.K_DAY, "1w": SubType.K_WEEK,
        "1M": SubType.K_MON,
    }
    AUTYPE = {"none": AuType.NONE, "qfq": AuType.QFQ, "hfq": AuType.HFQ}
    return OpenQuoteContext, RET_OK, KTYPE, SUBTYPE, AUTYPE


def action_klines(ctx, args, OpenQuoteContext, RET_OK, KTYPE, AUTYPE):
    ktype = KTYPE.get(args.period.lower())
    if ktype is None:
        return {"ok": False, "error": "unsupported period '%s'" % args.period}
    autype = AUTYPE.get(args.adjust, AUTYPE["qfq"])
    bounded = bool(args.start and args.end)
    # No explicit max: bounded ranges fetch everything; auto windows target 1000.
    n_bars = args.max if args.max and args.max > 0 else (0 if bounded else 1000)

    end_d = date.today()
    if args.end:
        end_d = datetime.strptime(args.end[:10], "%Y-%m-%d").date()
    if args.start:
        start_d = datetime.strptime(args.start[:10], "%Y-%m-%d").date()
    else:
        start_d = end_d - timedelta(days=lookback_days(args.period, n_bars))

    ret, data, page_key = ctx.request_history_kline(
        args.code, start=str(start_d), end=str(end_d),
        ktype=ktype, autype=autype, max_count=1000,
    )
    if ret != RET_OK:
        return {"ok": False, "error": str(data)}
    frames = [data]
    while page_key is not None:
        ret, data, page_key = ctx.request_history_kline(
            args.code, start=str(start_d), end=str(end_d),
            ktype=ktype, autype=autype, max_count=1000, page_req_key=page_key,
        )
        if ret != RET_OK:
            return {"ok": False, "error": str(data)}
        if data is not None and len(data):
            frames.append(data)
        if sum(len(f) for f in frames) >= n_bars:
            break
    import pandas as pd
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    if df is None or len(df) == 0:
        return {"ok": True, "rows": []}
    if n_bars > 0 and len(df) > n_bars:
        df = df.tail(n_bars)
    sd = sub_daily(args.period)
    rows = []
    for _, r in df.iterrows():
        t = ts_ns(args.code, str(r.get("time_key", "")), sd)
        if t is None:
            continue
        rows.append({
            "ts_ns": t,
            "open": jnum(r.get("open")), "high": jnum(r.get("high")),
            "low": jnum(r.get("low")), "close": jnum(r.get("close")),
            "volume": jnum(r.get("volume")), "turnover": jnum(r.get("turnover")),
            "adjclose": None,
        })
    return {"ok": True, "rows": rows}


def action_ticks(ctx, args, OpenQuoteContext, RET_OK, SUBTYPE):
    from futu import SubType
    ret, msg = ctx.subscribe([args.code], [SubType.TICKER])
    if ret != RET_OK:
        return {"ok": False, "error": "subscribe TICKER failed: %s" % msg}
    num = args.max if args.max and args.max > 0 else 500
    num = min(num, 1000)
    ret, data = ctx.get_rt_ticker(args.code, num=num)
    if ret != RET_OK:
        return {"ok": False, "error": str(data)}
    if data is None or len(data) == 0:
        return {"ok": True, "rows": []}
    rows = []
    for _, r in data.iterrows():
        t = ts_ns(args.code, str(r.get("time", "")), True)
        if t is None:
            continue
        direction = jint(r.get("ticker_direction"))
        rows.append({
            "ts_ns": t,
            "price": jnum(r.get("price")),
            "volume": jnum(r.get("volume")),
            "turnover": jnum(r.get("turnover")),
            "direction": {1: "B", 2: "S", 3: "N"}.get(direction, "N"),
            "sequence": jint(r.get("sequence")) or 0,
        })
    return {"ok": True, "rows": rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--action", choices=["klines", "ticks"], required=True)
    ap.add_argument("--code", required=True)
    ap.add_argument("--period", default="1d")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--adjust", default="qfq")
    ap.add_argument("--host", default=os.getenv("FUTU_OPEND_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("FUTU_OPEND_PORT", "11111")))
    args = ap.parse_args()

    try:
        OpenQuoteContext, RET_OK, KTYPE, SUBTYPE, AUTYPE = load_futu()
        ctx = OpenQuoteContext(host=args.host, port=args.port)
        try:
            if args.action == "klines":
                out = action_klines(ctx, args, OpenQuoteContext, RET_OK, KTYPE, AUTYPE)
            else:
                out = action_ticks(ctx, args, OpenQuoteContext, RET_OK, SUBTYPE)
        finally:
            try:
                ctx.close()
            except Exception:
                pass
    except Exception as e:  # noqa: BLE001 — report everything to the caller
        out = {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
    sys.stdout.write(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
