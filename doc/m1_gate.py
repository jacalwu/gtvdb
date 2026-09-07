#!/usr/bin/env python3
# M1 feasibility gate (design.md): strict time-CV comparison on the 8 holdings,
# H=7. Same eval rows & protocol for every model:
#   eval window  = tail 60% of labelled rows  i in [ceil(m*0.4), m)
#   cadence C=21 -> retrain at window start s using labelled rows j <= s-H
#   (knn4 runs per-bar like production -> an *upper-hand* baseline)
# Models:
#   knn4      kNN k=20, auto-features (mom5,mom10,vol10,above_sma20), per-bar
#   gbdt4     GBDT, same 4 features              (pure model effect)
#   gbdt15    GBDT, richer 15 causal features    (feature effect)
#   gbdt15ix  gbdt15 + HSI/HSTECH relative & market features (index effect)
# Metrics on raw p: directional hit, Brier, ECE10. Reference rows show the
# trivial base (always-most-frequent class) and the HSI buy-hold H-ret mean.
import datetime, glob, os
import numpy as np, pandas as pd
import pyarrow.parquet as pq
from xgboost import XGBClassifier

H = 7; K = 20; C = 21; EVAL = 0.6
HOLD = ["HK.00020","HK.00100","HK.00823","HK.01929","HK.02333","HK.02513","HK.03668","HK.06613"]
GB_PARAMS = dict(objective="binary:logistic", max_depth=3, learning_rate=0.05,
                 n_estimators=180, subsample=0.8, colsample_bytree=0.8,
                 min_child_weight=5, n_jobs=4, verbosity=0)

# ---------- data ----------
def hkt_date(ns):  # engine ts = UTC ms of HKT midnight -> HKT date
    return (datetime.datetime(1970,1,1) + datetime.timedelta(seconds=ns/1e9 + 8*3600)).date()

def stock_df(sym):
    f = f"/home/jacal/gtvdb/analytics_out/{sym.replace('.','_')}_bars.parquet"
    d = pq.read_table(f).to_pandas().sort_values("ts")
    d["date"] = d["ts"].map(hkt_date)
    return d[["date","open","high","low","close","volume"]].set_index("date").sort_index()

def idx_df():
    cache = "/home/jacal/gtvdb/analytics_out/HK_INDEX_daily.parquet"
    if os.path.exists(cache):
        d = pq.read_table(cache).to_pandas()
        d["date"] = d["date"].astype(str).map(datetime.date.fromisoformat)
        if len(d) > 800:  # full-history sanity (pagination previously truncated)
            return d.set_index("date").sort_index()
    from futu import OpenQuoteContext, KLType, AuType
    q = OpenQuoteContext(host="127.0.0.1", port=11111)
    out = {}
    today = datetime.date.today()
    for code, col in [("HK.800000","hsi"), ("HK.800700","hstech")]:
        parts = []
        for yr in range(2021, today.year + 1):
            s = datetime.date(yr, 1, 1); e = min(datetime.date(yr, 12, 31), today)
            ret, df, _ = q.request_history_kline(code, start=s.isoformat(), end=e.isoformat(),
                ktype=KLType.K_DAY, autype=AuType.NONE, max_count=1000)
            if ret != 0: raise RuntimeError(f"idx {code}: {df}")
            df["date"] = df["time_key"].str[:10].map(datetime.date.fromisoformat)
            parts.append(df[["date","close"]])
        allf = pd.concat(parts).drop_duplicates("date").set_index("date")["close"]
        out[col] = allf.astype(float)
    q.close()
    d = pd.DataFrame(out)
    d.to_parquet(cache)
    return d

IDX = idx_df()

def feats(df, mode):
    c, v = df["close"].astype(float), df["volume"].astype(float)
    hi, lo = df["high"].astype(float), df["low"].astype(float)
    ret1 = c.pct_change()
    base = pd.DataFrame(index=df.index)
    base["mom5"] = c / c.shift(5) - 1; base["mom10"] = c / c.shift(10) - 1
    base["vol10"] = ret1.rolling(10).std()
    lo20 = c.rolling(20).mean(); base["above_sma20"] = c / lo20 - 1
    if mode in ("base",):
        return base.ffill().fillna(0.0)
    f = base.copy()
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
    if mode == "full":
        return f.ffill().fillna(0.0)
    # mode "idx": align HSI/HSTECH closes onto stock trading dates (ffill)
    ix = IDX.reindex(df.index).ffill().reindex(df.index)
    hi_, ht_ = ix["hsi"].astype(float), ix["hstech"].astype(float)
    f["h_m5"] = hi_ / hi_.shift(5) - 1
    f["h_m20"] = hi_ / hi_.shift(20) - 1
    f["t_m5"] = ht_ / ht_.shift(5) - 1
    f["h_vol20"] = hi_.pct_change().rolling(20).std()
    f["rs_m5"] = f["mom5"] - f["h_m5"]      # idiosyncratic momentum vs HSI
    f["rs_m20"] = f["mom20"] - f["h_m20"]
    f["t_rs5"] = f["mom5"] - f["t_m5"]      # vs HSTECH (AI/tech names)
    return f.ffill().fillna(0.0)

# ---------- models (windowed, causal) ----------
def window_eval(feat, up, model_name, gb=None):
    m = len(up); i0 = int(m * (1 - EVAL))
    idx = np.arange(i0, m)
    rows = []
    X = feat.values.astype(np.float64)
    if model_name == "knn4":  # per-bar retrain, mirrors production engine
        for i in idx:
            tr = np.arange(0, i - H + 1)          # j + H <= i
            if len(tr) < 30: continue
            ztr = (X[tr] - X[tr].mean(0)) / X[tr].std(0).clip(min=1e-9)
            zq = (X[i] - X[tr].mean(0)) / X[tr].std(0).clip(min=1e-9)
            d2 = ((ztr - zq) ** 2).sum(1)
            k = min(K, len(tr)); nb = np.argsort(d2, kind="stable")[:k]
            rows.append((i, up[i], float(up[nb].mean())))
    else:  # gbdt cadence C: retrain at s using j <= s-H, apply s..s+C
        for s in range(i0, m, C):
            e = min(s + C, m)
            tr = np.arange(0, s - H + 1)
            if len(tr) < 30:
                for i in range(s, e):
                    if i < m: rows.append((i, up[i], 0.5))
                continue
            md = XGBClassifier(**GB_PARAMS)
            md.fit(X[tr], up[tr])
            p = md.predict_proba(X[s:e])[:, 1]
            for i, pi in zip(range(s, e), p):
                rows.append((i, up[i], float(pi)))
    return rows

def metrics(rows):
    y = np.array([r[1] for r in rows]); p = np.array([r[2] for r in rows])
    hit = float(np.mean((p > 0.5) == (y == 1)))
    brier = float(np.mean((p - y) ** 2))
    ece = 0.0
    for b in range(10):
        msk = (p >= b/10) & (p < (b+1)/10)
        if msk.sum():
            ece += abs(p[msk].mean() - y[msk].mean()) * msk.sum()
    return dict(hit=hit, brier=brier, ece=ece/len(y), n=len(y),
                base=max(y.mean(), 1 - y.mean()))

def run_symbol(sym):
    df = stock_df(sym)
    c = df["close"].astype(float)
    m = len(df) - H
    up = (c.shift(-H) > c).astype(int).iloc[:m].values
    f4 = feats(df, "base").iloc[:m]
    f15 = feats(df, "full").iloc[:m]
    f15ix = feats(df, "idx").iloc[:m]
    assert len(up) == m
    mods = {
        "knn4":  window_eval(f4, up, "knn4"),
        "gbdt4": window_eval(f4, up, "gbdt"),
        "gbdt15": window_eval(f15, up, "gbdt"),
        "gbdt15ix": window_eval(f15ix, up, "gbdt"),
    }
    return {k: metrics(v) for k, v in mods.items()}, m

print(f"{'symbol':<10} {'m':>4} | " + " | ".join(
    f"{n:>6}/{n[:4]}" for n in ["knn4","gbdt4","gbdt15","gbdt15ix"]))
names = ["knn4","gbdt4","gbdt15","gbdt15ix"]
agg = {n: [] for n in names}
for sym in HOLD:
    try:
        res, m = run_symbol(sym)
        cells = []
        for n in names:
            r = res[n]; agg[n].append(r)
            cells.append(f"{r['hit']:.3f}/{r['ece']:.3f}")
        print(f"{sym:<10} {m:>4} | " + " | ".join(f"{x:>12}" for x in cells))
    except Exception as e:
        print(f"{sym:<10} ERR {type(e).__name__}: {str(e)[:70]}")
print("\nweighted aggregate (by eval n):")
tot = {n: sum(r["n"] for r in agg[n]) for n in names}
for n in names:
    rs = agg[n]
    wh = sum(r["hit"] * r["n"] for r in rs) / tot[n]
    wb = sum(r["brier"] * r["n"] for r in rs) / tot[n]
    we = sum(r["ece"] * r["n"] for r in rs) / tot[n]
    beats = sum(1 for r in rs if r["hit"] >= max(x["hit"] for x in rs if x is not r) - 0.005 and r is not None)
    print(f"  {n:<9} hit={wh:.3f} brier={wb:.3f} ece={we:.3f} n={tot[n]}")
