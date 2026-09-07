#!/usr/bin/env python3
# feature gate 2 — does adding the function3.md Wave A feature battery (trend
# slopes / vol ratios / volume / candles / gaps / regimes, on top of the old
# "full15") move H=7 direction at all? Same causal protocol as doc/m1_gate.py:
# tail-60% eval, knn per-bar, gbdt cadence C=21 with embargo H.
import datetime
import numpy as np, pandas as pd, pyarrow.parquet as pq
from xgboost import XGBClassifier

H = 7; K = 20; C = 21
OUT = "/home/jacal/gtvdb/analytics_out"
SYMS = ["HK.00020","HK.00100","HK.00823","HK.01929","HK.02333","HK.02513",
        "HK.03668","HK.00857","HK.00386","HK.06613"]

def hkt(ns):
    return (datetime.datetime(1970,1,1)+datetime.timedelta(seconds=ns/1e9+8*3600)).date()

def stock_df(sym):
    f = f"{OUT}/{sym.replace('.','_')}_bars.parquet"
    d = pq.read_table(f).to_pandas().sort_values("ts")
    d["date"] = d["ts"].map(hkt)
    return d[["date","open","high","low","close","volume"]].set_index("date").sort_index()

def feats_wave(df):
    o = df["open"].astype(float); h = df["high"].astype(float)
    l = df["low"].astype(float); c = df["close"].astype(float); v = df["volume"].astype(float)
    ret = c.pct_change()
    ema = lambda s, n: s.ewm(span=n, adjust=False).mean()
    f = pd.DataFrame(index=df.index)
    # --- previous "full15" battery (keep as control) ---
    f["mom5"] = c/c.shift(5)-1; f["mom10"] = c/c.shift(10)-1; f["mom20"] = c/c.shift(20)-1
    f["ret1"] = ret
    for w in (5,20,60): f[f"sma{w}r"] = c/c.rolling(w).mean()-1
    f["vol10"] = ret.rolling(10).std(); f["vol20"] = ret.rolling(20).std()
    up = ret.clip(lower=0); dn = (-ret).clip(lower=0)
    ru = up.ewm(alpha=1/14, adjust=False).mean(); rd = dn.ewm(alpha=1/14, adjust=False).mean()
    f["rsi14"] = 100-100/(1+ru/rd.replace(0,np.nan))
    macd = ema(c,12)-ema(c,26); f["macdh"] = macd-ema(macd,9)
    m20 = c.rolling(20).mean(); s20 = c.rolling(20).std()
    f["bbpos"] = (c-(m20-2*s20))/(4*s20)
    f["volr"] = v/v.rolling(20).mean()-1
    f["hilo20"] = (c-l.rolling(20).min())/(h.rolling(20).max()-l.rolling(20).min()).replace(0,np.nan)
    # --- Wave A additions ---
    e5 = ema(c,5); e20 = ema(c,20); e60 = ema(c,60)
    f["ema_slope"] = (e5-e5.shift(1))/c
    f["ema_accel"] = (e5-2*e5.shift(1)+e5.shift(2))/c
    bbw = 4*s20/m20
    f["bb_width"] = bbw
    f["bb_mid_slope"] = (m20-m20.shift(1))/c
    f["macd_dist"] = (macd-ema(macd,9))/c
    f["atr_ratio"] = _atr(h,l,c,7)/_atr(h,l,c,21).replace(0,np.nan)
    f["hv_ratio"] = ret.rolling(10).std()/ret.rolling(30).std().replace(0,np.nan)
    f["vol_spike"] = v/v.rolling(20).mean()-1          # same info as volr; kept for parity
    obv = (np.sign(c.diff().fillna(0))*v).cumsum()
    f["obv_z"] = (obv-obv.rolling(20).mean())/obv.rolling(20).std().replace(0,np.nan)
    f["vwap_dev"] = c/(( (h+l+c)/3*v).rolling(10).sum()/v.rolling(10).sum())-1
    f["hh20"] = (h > h.rolling(20).max().shift()).astype(float)
    f["ll20"] = (l < l.rolling(20).min().shift()).astype(float)
    rng = (h-l).replace(0,np.nan)
    f["big_green"] = ((c>o)&((c-o)/rng>0.5)).astype(float)
    f["big_red"] = ((o>c)&((o-c)/rng>0.5)).astype(float)
    f["shadow_low"] = (o.combine(c, min) - l) / rng
    f["shadow_up"] = (h-o.combine(c,max))/rng
    f["gap1"] = o/c.shift(1)-1
    f["regime_trend"] = (e20-e60)/c
    f["regime_vol"] = ret.rolling(10).std()/ret.rolling(60).std().replace(0,np.nan)
    return f.ffill().fillna(0.0)

def _atr(h,l,c,n):
    tr = np.maximum(h-l, np.maximum((h-c.shift()).abs(), (l-c.shift()).abs()))
    return tr.ewm(alpha=1/n, adjust=False).mean()

def walk_knn(X, y):
    m = len(y); i0 = int(m*0.4); rows = []
    Xv = X.values.astype(float)
    for i in range(i0, m):
        tr = np.arange(0, i-H+1)
        if len(tr) < 30: continue
        mu, sd = Xv[tr].mean(0), Xv[tr].std(0).clip(min=1e-9)
        q = (Xv[i]-mu)/sd; z = (Xv[tr]-mu)/sd
        d2 = ((z-q)**2).sum(1)
        nb = np.argsort(d2, kind="stable")[:min(K, len(tr))]
        rows.append((y[i], float(y[nb].mean())))
    return rows

def walk_gbdt(X, y):
    m = len(y); i0 = int(m*0.4); rows = []
    Xv = X.values.astype(float)
    for s0 in range(i0, m, C):
        e = min(s0+C, m); tr = np.arange(0, s0-H+1)
        if len(tr) < 30:
            for i in range(s0,e): rows.append((y[i], 0.5))
            continue
        md = XGBClassifier(objective="binary:logistic", max_depth=3, learning_rate=0.05,
                           n_estimators=160, subsample=0.8, colsample_bytree=0.8,
                           min_child_weight=5, n_jobs=4, verbosity=0)
        md.fit(Xv[tr], y[tr])
        for i, p in zip(range(s0,e), md.predict_proba(Xv[s0:e])[:,1]):
            rows.append((y[i], float(p)))
    return rows

def met(rows):
    if not rows: return None
    y = np.array([r[0] for r in rows]); p = np.array([r[1] for r in rows])
    return dict(hit=float(np.mean((p>0.5)==(y==1))), n=len(y))

print(f"{'symbol':<9}{'m':>5} | {'knn_base':>8}{'knn_wave':>9}{'gbdt_wave':>10} | Δ(knn)")
agg = {k: [] for k in ["knn_base","knn_wave","gbdt_wave"]}
for sym in SYMS:
    df = stock_df(sym); c = df["close"].astype(float); m = len(df)-H
    y = (c.shift(-H) > c).astype(int).iloc[:m].values
    F = feats_wave(df).iloc[:m]
    base4 = F[["mom5", "mom10", "vol10", "sma20r"]].copy()
    res = {"knn_base": walk_knn(base4, y), "knn_wave": walk_knn(F, y), "gbdt_wave": walk_gbdt(F, y)}
    mm = {k: met(v) for k, v in res.items()}
    for k in mm: agg[k].append(mm[k])
    d = (mm["knn_wave"]["hit"]-mm["knn_base"]["hit"]) if mm["knn_base"] and mm["knn_wave"] else float("nan")
    print(f"{sym:<9}{m:>5} | {mm['knn_base']['hit'] if mm['knn_base'] else float('nan'):>8.3f}"
          f"{mm['knn_wave']['hit'] if mm['knn_wave'] else float('nan'):>9.3f}"
          f"{mm['gbdt_wave']['hit'] if mm['gbdt_wave'] else float('nan'):>10.3f} | {d:+.3f}")
print("\nweighted:")
for k in agg:
    rs = [r for r in agg[k] if r]; n = sum(r["n"] for r in rs)
    if rs: print(f"  {k:<10} hit={sum(r['hit']*r['n'] for r in rs)/n:.3f} n={n}")
