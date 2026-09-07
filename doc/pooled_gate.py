#!/usr/bin/env python3
# Phase B pooled gate (design.md): can cross-stock pooling beat per-stock models?
# Same strict protocol as doc/m1_gate.py — H=7, tail-60% eval rows per stock,
# cadence C=21 refit with embargo H — but the pooled model trains on ALL
# holdings' rows whose forward label was realised by the refit date.
# Models: knn4 (per-bar, production proxy) | gbdt15ix pooled | gbdt15 pooled |
#         gbdt15ix per-stock   (everything measured on identical eval rows)
import datetime, os
import numpy as np, pandas as pd, pyarrow.parquet as pq
from xgboost import XGBClassifier

H = 7; K = 20; C = 21; EVAL = 0.6
OUT = "/home/jacal/gtvdb/analytics_out"
GB = dict(objective="binary:logistic", max_depth=3, learning_rate=0.05,
          n_estimators=180, subsample=0.8, colsample_bytree=0.8,
          min_child_weight=5, n_jobs=6, verbosity=0)

def holdings():
    out = []
    for line in open("/home/jacal/gtvdb/HOLDING.txt", encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"): continue
        num = "".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
        out.append("HK." + num.rjust(5, "0"))
    return out

def hkt_date(ns):
    return (datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=ns / 1e9 + 8 * 3600)).date()

def load_idx():
    t = pq.read_table(f"{OUT}/HK_INDEX_daily.parquet").to_pandas()
    if "date" not in t.columns: t = t.reset_index()
    t["date"] = t["date"].astype(str).map(datetime.date.fromisoformat)
    return t.set_index("date").sort_index()

IDX = load_idx()

def stock(sym):
    f = pq.read_table(f"{OUT}/{sym.replace('.','_')}_bars.parquet").to_pandas().sort_values("ts")
    f["date"] = f["ts"].map(hkt_date)
    f = f.set_index("date").sort_index()
    return f

def feats(df, mode):
    c, v = df["close"].astype(float), df["volume"].astype(float)
    hi, lo = df["high"].astype(float), df["low"].astype(float)
    ret1 = c.pct_change()
    f = pd.DataFrame(index=df.index)
    f["mom5"] = c / c.shift(5) - 1; f["mom10"] = c / c.shift(10) - 1
    f["vol10"] = ret1.rolling(10).std()
    f["above_sma20"] = c / c.rolling(20).mean() - 1
    if mode == "base": return f.ffill().fillna(0.0)
    f["mom20"] = c / c.shift(20) - 1; f["ret1"] = ret1
    for w in (5, 20, 60): f[f"sma{w}r"] = c / c.rolling(w).mean() - 1
    f["vol20"] = ret1.rolling(20).std()
    up = ret1.clip(lower=0.0); dn = (-ret1).clip(lower=0.0)
    ru = up.ewm(alpha=1/14, adjust=False).mean(); rd = dn.ewm(alpha=1/14, adjust=False).mean()
    f["rsi14"] = 100 - 100/(1 + ru/rd.replace(0, np.nan))
    ema = lambda s, n: s.ewm(span=n, adjust=False).mean()
    macd = ema(c, 12) - ema(c, 26); f["macdh"] = macd - ema(macd, 9)
    m20 = c.rolling(20).mean(); s20 = c.rolling(20).std()
    f["bbpos"] = (c - (m20 - 2*s20)) / (4*s20)
    f["volr"] = v / v.rolling(20).mean() - 1
    lo20, hi20 = lo.rolling(20).min(), hi.rolling(20).max()
    f["hilo20"] = (c - lo20) / (hi20 - lo20).replace(0, np.nan)
    f["range20"] = (hi20 - lo20) / c
    if mode == "full": return f.ffill().fillna(0.0)
    ix = IDX.reindex(df.index).ffill()
    hi_, ht_ = ix["hsi"].astype(float), ix["hstech"].astype(float)
    f["h_m5"] = hi_ / hi_.shift(5) - 1
    f["h_m20"] = hi_ / hi_.shift(20) - 1
    f["t_m5"] = ht_ / ht_.shift(5) - 1
    f["h_vol20"] = hi_.pct_change().rolling(20).std()
    f["rs_m5"] = f["mom5"] - f["h_m5"]
    f["rs_m20"] = f["mom20"] - f["h_m20"]
    f["t_rs5"] = f["mom5"] - f["t_m5"]
    return f.ffill().fillna(0.0)

SYMS = holdings()
# per-symbol arrays: dates (py date list), X full/idx, y label, fwd dates
S = {}
for sym in SYMS:
    df = stock(sym); n = len(df); m = n - H
    dates = list(df.index)
    y = (df["close"].astype(float).shift(-H) > df["close"].astype(float)).astype(int).iloc[:m].values
    fwd_d = [dates[i + H] for i in range(m)]
    S[sym] = dict(dates=dates, Xf=feats(df, "full").iloc[:m].values.astype(float),
                  Xx=feats(df, "idx").iloc[:m].values.astype(float), y=y,
                  fwd=fwd_d, m=m, close=df["close"].astype(float).values)

def knn_eval(X, y, m):
    i0 = int(m * (1 - EVAL)); rows = []
    for i in range(i0, m):
        tr = np.arange(0, i - H + 1)   # expanding past-only, mirrors the engine
        if len(tr) < 30: continue
        mu, sd = X[tr].mean(0), X[tr].std(0).clip(min=1e-9)
        d2 = ((X[tr] - mu) / sd - (X[i] - mu) / sd) ** 2
        d2 = d2.sum(1)
        nb = np.argsort(d2, kind="stable")[:min(K, len(tr))]
        rows.append((y[i], float(y[nb].mean())))
    return rows

def gb_eval_stock(X, y, m):
    """per-stock cadence (m1_gate semantics)"""
    i0 = int(m * (1 - EVAL)); rows = []
    for s0 in range(i0, m, C):
        e = min(s0 + C, m)
        tr = np.arange(0, s0 - H + 1)
        if len(tr) < 30:
            for i in range(s0, e): rows.append((y[i], 0.5))
            continue
        md = XGBClassifier(**GB).fit(X[tr], y[tr])
        for i, p in zip(range(s0, e), md.predict_proba(X[s0:e])[:, 1]):
            rows.append((y[i], float(p)))
    return rows

# pooled training: rows of every stock whose fwd label date <= cutoff D
def pool_rows(cut):
    xs, ys = [], []
    for s in S.values():
        q = int(np.searchsorted(s["dates"], cut, side="right"))  # bars with date <= cut
        jmax = min(s["m"], max(0, q - H))
        if jmax <= 0: continue
        xs.append(s["Xx"][:jmax]); ys.append(s["y"][:jmax])
    if not xs: return None, None
    return np.vstack(xs), np.concatenate(ys)

model_cache = {}
def pooled_model(cut):
    if cut not in model_cache:
        X, y = pool_rows(cut)
        model_cache[cut] = XGBClassifier(**GB).fit(X, y) if X is not None and len(y) >= 30 else None
    return model_cache[cut]

def gb_eval_pooled(X, y, dates, m):
    i0 = int(m * (1 - EVAL)); rows = []
    for s0 in range(i0, m, C):
        e = min(s0 + C, m)
        cut = dates[min(s0, len(dates) - 1)]
        md = pooled_model(cut)
        if md is None:
            for i in range(s0, e): rows.append((y[i], 0.5))
            continue
        for i, p in zip(range(s0, e), md.predict_proba(X[s0:e])[:, 1]):
            rows.append((y[i], float(p)))
    return rows

def met(rows):
    if not rows: return None
    y = np.array([r[0] for r in rows]); p = np.array([r[1] for r in rows])
    hit = np.mean((p > 0.5) == (y == 1))
    return dict(hit=float(hit), brier=float(np.mean((p - y) ** 2)), n=len(y))

models = {"knn4": [], "p15ix": [], "s15ix": []}
print(f"{'symbol':<10} {'m':>4} | {'knn4':>6} {'p15ix':>6} {'s15ix':>6}")
for sym in SYMS:
    s = S[sym]; m, y = s["m"], s["y"]
    i0 = int(m * (1 - EVAL))
    res = {}
    res["knn4"] = knn_eval(s["Xf"][:, :4], y, m)          # base4 = first 4 cols
    res["s15ix"] = gb_eval_stock(s["Xx"], y, m)
    res["p15ix"] = gb_eval_pooled(s["Xx"], y, s["dates"], m)
    # pooled gbdt on full feats (no idx) via temp store of Xf per sym already have
    # (skip p15 for brevity — reuse pool with full features instead)
    r = {k: met(v) for k, v in res.items()}
    for k in r: models[k].append(r[k])
    print(f"{sym:<10} {m:>4} | " + " ".join(f"{r[k]['hit']:.3f}" if r[k] else "  -  " for k in ["knn4","p15ix","s15ix"]))
print("\nweighted aggregate:")
for k in ["knn4","p15ix","s15ix"]:
    rs = [r for r in models[k] if r]
    n = sum(r["n"] for r in rs)
    print(f"  {k:<6} hit={sum(r['hit']*r['n'] for r in rs)/n:.3f} brier={sum(r['brier']*r['n'] for r in rs)/n:.3f} n={n}")
