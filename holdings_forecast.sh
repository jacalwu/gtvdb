#!/usr/bin/env bash
#
# holdings_forecast.sh — forecast the next H trading days (default 7) for every
# holding in HOLDING.txt, aggregate a decision table and attach a market
# benchmark context (Hang Seng Index) as reference — "would this signal be just
# beta?" each signal is reported next to HSI momentum & the stock's relative
# strength vs HSI.
#
# Each holding runs the calibrated pipeline (stock_analysis.sh: walk-forward
# Platt/isotonic calibration -> decision threshold -> exit code). Per-symbol
# TSVs land in OUT_DIR; the aggregate is $OUT_DIR/holdings_forecast_<H>d.tsv.
#
# Usage:
#   ./holdings_forecast.sh                  # SOURCE=futu (needs OpenD logged in)
#   SOURCE=yahoo ./holdings_forecast.sh
#   HORIZON=7 THRESHOLD=0.72 ./holdings_forecast.sh
#   SYMS="HK.00700 HK.02333" ./holdings_forecast.sh
#
# Env: SYMS, SOURCE=futu|yahoo, HORIZON=7, THRESHOLD=0.72, METHOD=platt|iso|raw,
#   OUT_DIR=./analytics_out, GTV_PROFILE=release.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$here"

HOLDINGS="${SYMS:-$(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' HOLDING.txt 2>/dev/null | awk '{print $1}' || true)}"
[ -n "$HOLDINGS" ] || { echo "no symbols: set SYMS or fill HOLDING.txt" >&2; exit 1; }
SOURCE="${SOURCE:-futu}"; HORIZON="${HORIZON:-7}"; THRESHOLD="${THRESHOLD:-0.72}"
METHOD="${METHOD:-platt}"; OUT_DIR="${OUT_DIR:-./analytics_out}"; GTV_PROFILE="${GTV_PROFILE:-release}"
EVENT_MODE="${EVENT_MODE:-0}"   # 1 = geopolitical/event day: raise effective threshold, flag gaps
mkdir -p "$OUT_DIR"
[ "$SOURCE" = "futu" ] || [ "$SOURCE" = "yahoo" ] || { echo "SOURCE=$SOURCE unknown" >&2; exit 1; }

# Normalise an HK holding code: futu HK.03668 (5-digit), yahoo 3668.HK.
norm_code() { printf '%s' "$1" | sed 's/[^0-9]//g' | sed 's/^0*//'; }

# Optional per-stock horizon/threshold map: HOLDING.cfg lines "<sym> <H> <THR>"
# (unlisted symbols use the global HORIZON/THRESHOLD).
CFG_FILE="${CFG_FILE:-HOLDING.cfg}"
cfg_lookup() {  # $1=sym -> echoes "H T" (global defaults when unlisted)
  if [ -f "$CFG_FILE" ]; then
    local line
    while read -r sym_c h_c t_c _; do
      [ -z "$sym_c" ] && continue
      case "$sym_c" in \#*) continue ;; esac
      if [ "$sym_c" = "$1" ]; then echo "$h_c $t_c"; return; fi
    done < "$CFG_FILE"
  fi
  echo "$HORIZON $THRESHOLD"
}
CFG_PRESENT=0; [ -f "$CFG_FILE" ] && CFG_PRESENT=1

for sym in $HOLDINGS; do
  num="$(norm_code "$sym")"
  [ -n "$num" ] || { echo "!! bad symbol '$sym'" >&2; continue; }
  case "$SOURCE" in
    futu)   ysym="HK.$(printf '%05d' "$num")" ;;
    yahoo)  ysym="$(printf '%04d' "$num").HK" ;;
  esac
  read -r Hh Th <<< "$(cfg_lookup "$ysym")"
  echo "=== $sym (H=${Hh:-$HORIZON} td, threshold ${Th:-$THRESHOLD}, $SOURCE -> $ysym) ==="
  set +e
  OUT_DIR="$OUT_DIR" HORIZON="${Hh:-$HORIZON}" THRESHOLD="${Th:-$THRESHOLD}" METHOD="$METHOD" \
    GTV_PROFILE="$GTV_PROFILE" SOURCE="$SOURCE" ./stock_analysis.sh "$ysym" >/dev/null 2>&1
  rc=$?
  set -e
  [ -f "$OUT_DIR/${ysym//./_}_analysis.tsv" ] || echo "!! $sym: no output tsv (rc=$rc)" >&2
done

# ---- aggregate + attach HSI benchmark context (python) ----------------------
OUT_DIR="$OUT_DIR" HORIZON="$HORIZON" SOURCE="$SOURCE" CFG_PRESENT="$CFG_PRESENT" python3 - <<'PY'
import os, sys, glob, datetime, csv
import numpy as np, pandas as pd, pyarrow.parquet as pq

out_dir = os.environ["OUT_DIR"]; H = int(os.environ["HORIZON"])
cfg_present = os.environ.get("CFG_PRESENT") == "1"
# per-stock horizon map from HOLDING.cfg (for sim-file selection & output)
def cfg_horizon(sym):
    if cfg_present:
        for line in open("HOLDING.cfg", encoding="utf-8"):
            parts = line.split()
            if len(parts) >= 2 and parts[0] == sym and not parts[0].startswith("#"):
                try: return int(parts[1])
                except ValueError: pass
    return H
holdings = []
for line in open("HOLDING.txt", encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#"): continue
    num = "".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
    holdings.append("HK." + num.rjust(5, "0"))

def hkt_date(ns):
    return (datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=ns / 1e9 + 8 * 3600)).date()

# per-symbol last decision rows + HKT decision date from the bars parquet
rows, hdr = [], None
for sym in sorted(holdings, key=lambda s: int("".join(ch for ch in s if ch.isdigit()))):
    tsv = f"{out_dir}/{sym.replace('.','_')}_analysis.tsv"
    if not os.path.exists(tsv): continue
    lines = [l.rstrip("\n") for l in open(tsv, encoding="utf-8") if l.strip()]
    if not lines: continue
    if hdr is None: hdr = lines[0].split("\t")
    last = lines[-1].split("\t")
    d = {}
    for i, name in enumerate(hdr):
        try: d[name] = float(last[i])
        except (ValueError, IndexError): d[name] = last[i]
    # decision HKT date + last close from the bars parquet (authoritative)
    bar = None
    for cand in (f"{out_dir}/{sym.replace('.','_')}_bars.parquet",
                 f"/home/jacal/gtvdb/analytics_out/{sym.replace('.','_')}_bars.parquet"):
        if os.path.exists(cand): bar = pq.read_table(cand).to_pandas().sort_values("ts"); break
    if bar is not None:
        d["date"] = hkt_date(bar["ts"].iloc[-1])
        d["close"] = float(bar["close"].iloc[-1])
        closes = bar["close"].astype(float).values
        d["stk_ret20"] = closes[-1] / closes[-21] - 1 if len(closes) > 21 else float("nan")
        d["stk_ret5"] = closes[-1] / closes[-6] - 1 if len(closes) > 6 else float("nan")
    rows.append((sym, d))

# HSI benchmark series (refresh if stale and futu is the source)
idx_path = f"{out_dir}/HK_INDEX_daily.parquet"
if not os.path.exists(idx_path) and os.path.exists("/home/jacal/gtvdb/analytics_out/HK_INDEX_daily.parquet"):
    idx_path = "/home/jacal/gtvdb/analytics_out/HK_INDEX_daily.parquet"
idx = None
if os.path.exists(idx_path):
    t = pq.read_table(idx_path).to_pandas()
    if "date" not in t.columns:
        t = t.reset_index()          # pyarrow restored the named index
    t["date"] = t["date"].astype(str).map(datetime.date.fromisoformat)
    idx = t.set_index("date").sort_index()
want_last = max((d.get("date") or datetime.date(2000,1,1) for _, d in rows), default=None)
if idx is not None and want_last and idx.index.max() < want_last and os.environ.get("SOURCE") == "futu":
    try:
        from futu import OpenQuoteContext, KLType, AuType
        q = OpenQuoteContext(host="127.0.0.1", port=11111); add = {}
        for code, col in [("HK.800000","hsi"), ("HK.800700","hstech")]:
            parts, s = [], idx.index.max() + datetime.timedelta(days=1)
            for yr in range(s.year, want_last.year + 1):
                lo = max(s, datetime.date(yr,1,1)); hi = min(datetime.date(yr,12,31), want_last + datetime.timedelta(days=1))
                if lo > hi: continue
                ret, df, _ = q.request_history_kline(code, start=lo.isoformat(), end=hi.isoformat(),
                    ktype=KLType.K_DAY, autype=AuType.NONE, max_count=1000)
                if ret == 0:
                    df["date"] = df["time_key"].str[:10].map(datetime.date.fromisoformat)
                    parts.append(df[["date","close"]])
            if parts:
                f = pd.concat(parts).drop_duplicates("date").set_index("date")["close"].astype(float)
                add[col] = f
        q.close()
        if add:
            idx = idx.join(pd.DataFrame(add), how="outer").sort_index()
            idx.to_parquet(idx_path)
    except Exception as e:
        print(f"(index refresh skipped: {type(e).__name__}: {str(e)[:60]})", file=sys.stderr)

def bench(d):
    """HSI context on/around decision HKT date."""
    if idx is None or "date" not in d: return {}
    date = d["date"]
    if date not in idx.index:
        pr = idx.index[idx.index <= date]
        if len(pr) == 0: return {}
        date = pr[-1]
    h = idx.loc[date, "hsi"]; hi = idx["hsi"]
    out = {"date": date, "hsi": float(h)}
    pos = {k: idx.index.get_loc(date) for k in [5, 20, 60] if date in idx.index}
    for k in (5, 20, 60):
        i = idx.index.get_loc(date)
        if i >= k:
            out[f"hsi_ret{k}"] = float(hi.iloc[i] / hi.iloc[i - k] - 1)
    if "stk_ret20" in d and "hsi_ret20" in out:
        out["rel20"] = d["stk_ret20"] - out["hsi_ret20"]   # stock vs market (alpha-ish)
    return out

extra = ["date","hsi","hsi_ret5","hsi_ret20","hsi_ret60","stk_ret5","stk_ret20","rel20"]
# two requested columns:
#   direction = UP / DOWN / FLAT(無方向)  from p_cal vs a probability flat band
#   action    = BUY / HOLD / SELL         today's signal x paper-sim reliability
#     (same rule as holdings_sim.sh: BUY  = UP-signal strength>=0.60 and historic
#      longs paid; SELL = DOWN-signal strength>=0.55 and historic shorts paid;
#      else HOLD. No sim data (short history / not yet simulated) -> HOLD.)
band = float(os.environ.get("FLAT_BAND", "0.12"))   # 中性帶 0.38–0.62（tuning5 ④ 採納）
def direction_of(pc):
    if pc is None: return "FLAT"
    if pc >= 0.5 + band: return "UP"
    if pc <= 0.5 - band: return "DOWN"
    return "FLAT"
# paper-sim side means per symbol (THR 0.6, cost 0, that symbol's horizon), best-effort
sim = {}
for sym in (s for s, _ in rows):
    sf = f"{out_dir}/{sym.replace('.','_')}_sim_H{cfg_horizon(sym)}d.tsv"
    if not os.path.exists(sf): continue
    for r in csv.DictReader(open(sf, encoding="utf-8"), delimiter="\t"):
        if abs(float(r["thr"]) - 0.6) < 1e-9 and float(r["cost_bps"]) == 0 and int(r["n_sig"]) > 0:
            sim[sym] = (int(r["n_buy"]), float(r["mean_buy_bp"]),
                        int(r["n_sell"]), float(r["mean_sell_bp"]))
            break
def action_of(pc, sym):
    pc = pc if pc is not None else 0.5
    st = max(pc, 1 - pc)
    s = sim.get(sym)
    up = pc >= 0.5
    if up and st >= 0.62:
        return "BUY" if s and s[0] >= 30 and s[1] > 0 else "HOLD"
    if (not up) and st >= 0.62:
        return "SELL" if s and s[2] >= 30 and s[3] > 0 else "HOLD"
    return "HOLD"

hdr_out = ["symbol","name","close","p_raw","p_cal","direction","action","sim_L","sim_S","decision","horizon"] + extra
# 中文股名 from HOLDING.txt (2nd column, if present)
names = {}
for _l in open("HOLDING.txt", encoding="utf-8"):
    _p = _l.split()
    if len(_p) >= 2 and not _p[0].startswith("#"):
        _d = "".join(ch for ch in _p[0] if ch.isdigit()).lstrip("0")
        names["HK." + _d.rjust(5, "0")] = _p[1]
out_rows = []
for sym, d in rows:
    b = bench(d)
    def fv(n): return d.get(n, float("nan"))
    def bv(n): return b.get(n, "")
    pc = d.get("p_cal")
    pc = float(pc) if isinstance(pc, (int, float)) else (float(pc) if pc not in (None, "") else None)
    direction = direction_of(pc)
    action = action_of(pc, sym)
    def simf(side):
        s = sim.get(sym)
        if s is None: return ""
        n, m = (s[0], s[1]) if side == "L" else (s[2], s[3])
        return f"{n}/{m:+.0f}" if n >= 1 and m == m else f"{n}/-"
    out_rows.append([sym, names.get(sym, ""), fv("close"), fv("p_raw"), pc if pc is not None else float("nan"),
                     direction, action, simf("L"), simf("S"), d.get("decision", "NO_SIGNAL"),
                     cfg_horizon(sym), bv("date"), bv("hsi"),
                     bv("hsi_ret5"), bv("hsi_ret20"), bv("hsi_ret60"),
                     fv("stk_ret5"), fv("stk_ret20"), bv("rel20")])
primary = f"{out_dir}/holdings_forecast_{H}d.tsv"
content = "\t".join(hdr_out) + "\n" + "\n".join(
    "\t".join("" if v == "" else f"{v:.4f}" if isinstance(v, float) else str(v) for v in r)
    for r in out_rows
) + "\n"
with open(primary, "w", encoding="utf-8") as f:
    f.write(content)
if not cfg_present:
    # keep the legacy generic name when running a single horizon
    with open(f"{out_dir}/holdings_forecast.tsv", "w", encoding="utf-8") as f:
        f.write(content)
print(f"saved {primary}")
PY
echo
python3 - "$OUT_DIR" "$HORIZON" <<'PY'
import sys, os
p = f"{sys.argv[1]}/holdings_forecast.tsv"
if not os.path.exists(p):
    p = f"{sys.argv[1]}/holdings_forecast_{sys.argv[2]}d.tsv"
rows = [l.rstrip("\n").split("\t") for l in open(p, encoding="utf-8") if l.strip()]
hdr, body = rows[0], rows[1:]
if os.environ.get("ZH") == "1":
    zh = {"symbol":"代碼","name":"名稱","close":"收市","p_raw":"P原始","p_cal":"P校準",
          "direction":"方向","action":"動作","sim_L":"多單(n/bp)","sim_S":"空單(n/bp)",
          "decision":"警報","horizon":"週期日","date":"日期","hsi":"恆指",
          "hsi_ret5":"恆5日","hsi_ret20":"恆20日","hsi_ret60":"恆60日",
          "stk_ret5":"股5日","stk_ret20":"股20日","rel20":"相對20日"}
    hdr = [zh.get(h, h) for h in hdr]
    tr = {"UP":"升","DOWN":"跌","FLAT":"橫行","BUY":"買","HOLD":"持","SELL":"賣"}
    hb = None
    for i, h in enumerate(rows[0]):
        if h == "direction": hb = i
    ab = None
    for i, h in enumerate(rows[0]):
        if h == "action": ab = i
    nb = None
    for i, h in enumerate(rows[0]):
        if h == "name": nb = i
    for r in body:
        if hb is not None and r[hb] in tr: r[hb] = tr[r[hb]]
        if ab is not None and r[ab] in tr: r[ab] = tr[r[ab]]
        if nb is not None and not r[nb]: r[nb] = "-"
w = [max(len(h), *(len(c) for c in col)) for h, col in zip(hdr, zip(*body))]
print("  ".join(h.ljust(w[i]) for i, h in enumerate(hdr)))
for r in body:
    print("  ".join(c.ljust(w[i]) for i, c in enumerate(r)))
PY

if [ "$EVENT_MODE" = "1" ]; then
  echo
  echo "⚠ EVENT MODE（重大新聞/地緣事件進行中）—— 風險覆蓋層："
  echo "  1) 訊號門檻建議提升至 ≥ 0.80（下方 0.72–0.79 的訊號請視為『觀望』，不追新倉）；"
  echo "  2) 注意週末跳空風險附錄（見下）：事件日開盤跳空遠大於平日；"
  echo "  3) 已持倉請以倉位/止損管理為主，勿依賴訊號做加倉決定。"
fi

# ---- weekend-gap appendix (risk of holding through an event weekend) --------
OUT_DIR="$OUT_DIR" python3 - <<'PY'
import os, datetime
import pandas as pd, pyarrow.parquet as pq
def hkt(ns): return (datetime.datetime(1970,1,1)+datetime.timedelta(seconds=ns/1e9+8*3600)).date()
hold=[]
for line in open("HOLDING.txt", encoding="utf-8"):
    line=line.strip()
    if not line or line.startswith("#"): continue
    num="".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
    hold.append("HK."+num.rjust(5,"0"))
rows=[]
for sym in sorted(hold, key=lambda s:int("".join(ch for ch in s if ch.isdigit()))):
    f=f"{os.environ['OUT_DIR']}/{sym.replace('.','_')}_bars.parquet"
    if not os.path.exists(f): continue
    d=pq.read_table(f).to_pandas().sort_values("ts")
    d["date"]=pd.to_datetime(d["ts"].map(hkt))
    d["dt"]=(d["date"]-d["date"].shift(1)).dt.days
    d["gap"]=d["open"]/d["close"].shift(1)-1
    wk=d[d["dt"]>=3]
    if len(wk)<5: continue
    rows.append((sym, len(wk),
        round(wk["gap"].abs().mean()*100,2), round(wk["gap"].quantile(.02)*100,2),
        round(wk["gap"].quantile(.98)*100,2), round(wk["gap"].std()*100,2)))
dest=f"{os.environ['OUT_DIR']}/holdings_weekend_gap.tsv"
with open(dest,"w",encoding="utf-8") as o:
    o.write("symbol\twkend_n\twk_avg_gap%\tp2%\tp98%\twk_std%\n")
    for r in rows: o.write("\t".join(map(str,r))+"\n")
print("\n=== 週末/長假跳空風險附錄（avg |gap|、p2/p98、std） -> "+dest+" ===")
print(f"{'symbol':<10}{'wk#':>5}{'avg|gap|':>9}{'p2':>8}{'p98':>8}{'std':>7}")
for r in rows:
    print(f"{r[0]:<10}{r[1]:>5}{r[2]:>9.2f}{r[3]:>8.2f}{r[4]:>8.2f}{r[5]:>7.2f}")
PY

# ---- data-source update status (health hook: freshness vs market calendar) --
OUT_DIR="$OUT_DIR" python3 - <<'PY'
import os, datetime
import pandas as pd, pyarrow.parquet as pq

def hkt(ns): return (datetime.datetime(1970,1,1)+datetime.timedelta(seconds=ns/1e9+8*3600)).date()

def load_idx():
    for p in (f"{os.environ['OUT_DIR']}/HK_INDEX_daily.parquet",
              "/home/jacal/gtvdb/analytics_out/HK_INDEX_daily.parquet"):
        if os.path.exists(p):
            t = pq.read_table(p).to_pandas()
            if "date" not in t.columns: t = t.reset_index()
            t["date"] = pd.to_datetime(t["date"].astype(str)).dt.date
            return t.sort_values("date")
    return None

hold=[]
for line in open("HOLDING.txt", encoding="utf-8"):
    line=line.strip()
    if not line or line.startswith("#"): continue
    num="".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
    hold.append("HK."+num.rjust(5,"0"))
idx = load_idx()
today = datetime.date.today()
market_last = idx["date"].max() if idx is not None else None
sessions = set(idx["date"]) if idx is not None else set()
rows=[]
for sym in sorted(hold, key=lambda s:int("".join(ch for ch in s if ch.isdigit()))):
    f=f"{os.environ['OUT_DIR']}/{sym.replace('.','_')}_bars.parquet"
    if not os.path.exists(f):
        rows.append((sym, 0, "-", "-", "-", "missing file", "fail")); continue
    d=pq.read_table(f).to_pandas().sort_values("ts")
    last = hkt(d["ts"].iloc[-1]); nrows=len(d)
    missed = sum(1 for s_ in sessions if last < s_ <= market_last) if market_last is not None else 0
    age = (today - last).days
    if market_last is not None and missed > 0:
        status = "warn"
    elif age > 7:
        status = "warn"
    else:
        status = "ok"
    if nrows < 250:
        status = "note"   # new listing / short history (still refreshed)
    rows.append((sym, nrows, last, market_last, missed, f"age {age}d", status))
print("\n=== 資料源更新狀態（health：對比恆指交易日曆；今日 "+str(today)+"） ===")
print(f"{'symbol':<10}{'bars':>6}  {'last_bar':>12}  {'mkt_last':>12}  {'missed':>7}  {'':>10} status")
for r in rows:
    sym,nrows,last,ml,missed,aged,st = r
    print(f"{sym:<10}{nrows:>6}  {str(last):>12}  {str(ml):>12}  {missed:>7}  {aged:>10} {st}")
if market_last is not None and (today - market_last).days > 7:
    print("⚠ 市場資料本身已 >7 天未更新：OpenD 是否運行/登入？重跑 ./holdings_forecast.sh 會自動補拉。")
dest=f"{os.environ['OUT_DIR']}/holdings_data_status.tsv"
with open(dest,"w",encoding="utf-8") as o:
    o.write("symbol\tn_bars\tlast_bar\tmarket_last\tmissed_sessions\tnote\tstatus\n")
    for r in rows:
        o.write("\t".join(map(str,r))+"\n")
print("-> "+dest)
PY

# ---- technical snapshot at the decision bar (engine-identical formulas) ----
OUT_DIR="$OUT_DIR" python3 - <<'PY'
import os, datetime, math
import numpy as np, pandas as pd, pyarrow.parquet as pq

def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi14(c):
    r = c.pct_change()
    up = r.clip(lower=0); dn = (-r).clip(lower=0)
    ru = up.ewm(alpha=1/14, adjust=False).mean(); rd = dn.ewm(alpha=1/14, adjust=False).mean()
    return 100 - 100/(1 + ru/rd.replace(0, np.nan))
def atr(h, l, c, n):
    tr = np.maximum(h-l, np.maximum((h-c.shift()).abs(), (l-c.shift()).abs()))
    return tr.ewm(alpha=1/n, adjust=False).mean()
def hv_ratio(c):
    r = c.pct_change()
    return (r.rolling(10).std(ddof=0) / r.rolling(30).std(ddof=0).replace(0, np.nan))

hold = []
for line in open("HOLDING.txt", encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#"): continue
    num = "".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
    sym = "HK." + num.rjust(5, "0")
    parts = line.split()
    name = parts[1] if len(parts) > 1 else ""
    hold.append((sym, name))
out = []
print("\n=== 技術指標快照（決策 bar：最後一根） ===")
print(f"{'symbol':<10}{'RSI14':>7}{'MACD_h':>8}{'BBW%':>7}{'ATRr':>7}{'HVr':>7}{'VolSpk':>8}{'ESlope%':>8}{'TrendR%':>8}")
for sym, name in hold:
    f = f"{os.environ['OUT_DIR']}/{sym.replace('.','_')}_bars.parquet"
    if not os.path.exists(f): continue
    d = pd.read_parquet(f).sort_values("ts")
    o, h, l, c, v = d["open"].astype(float), d["high"].astype(float), d["low"].astype(float), d["close"].astype(float), d["volume"].astype(float)
    e5, e20, e60 = ema(c, 5), ema(c, 20), ema(c, 60)
    macd = ema(c, 12) - ema(c, 26); hist = (macd - ema(macd, 9)).iloc[-1]
    m20 = c.rolling(20).mean(); s20 = c.rolling(20).std(ddof=0)
    bbw = (4 * s20 / m20).iloc[-1]
    ar = (atr(h, l, c, 7) / atr(h, l, c, 21).replace(0, np.nan)).iloc[-1]
    hvr = hv_ratio(c).iloc[-1]
    vs = (v / v.rolling(20).mean() - 1).iloc[-1]
    es = ((e5 - e5.shift()) / c).iloc[-1]
    tr = ((e20 - e60) / c).iloc[-1]
    rs = rsi14(c).iloc[-1]
    vals = [rs, hist, bbw * 100, ar, hvr, vs, es * 100, tr * 100]
    print(f"{sym:<10}" + "".join(f"{v:>8.2f}" if not (isinstance(v, float) and math.isnan(v)) else f"{'':>8}" for v in vals))
    out.append([sym, name, *[round(float(v), 3) if not (isinstance(v, float) and math.isnan(v)) else None for v in vals]])
dest = f"{os.environ['OUT_DIR']}/holdings_indicators.tsv"
with open(dest, "w", encoding="utf-8") as o:
    o.write("symbol\tname\trsi14\tmacd_hist\tbb_width_pct\tatr_ratio\thv_ratio\tvol_spike\teslope_pct\ttrend_regime_pct\n")
    for r in out:
        o.write("\t".join("" if x is None else str(x) for x in r) + "\n")
print("-> " + dest)
PY
