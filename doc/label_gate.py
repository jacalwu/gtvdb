#!/usr/bin/env python3
# label-gate: does adding a deadband to the binary label (|H-ret| < eps => not
# up, folding into down) change H=7 directional hit? knn base4, causal protocol.
import datetime
import numpy as np, pandas as pd, pyarrow.parquet as pq

H = 7; K = 20
OUT = "/home/jacal/gtvdb/analytics_out"
SYMS = ["HK.00020","HK.00100","HK.00823","HK.01929","HK.02333","HK.02513",
        "HK.03668","HK.00857","HK.00386","HK.06613"]

def hkt(ns):
    return (datetime.datetime(1970,1,1)+datetime.timedelta(seconds=ns/1e9+8*3600)).date()

def stock_df(sym):
    d = pq.read_table(f"{OUT}/{sym.replace('.','_')}_bars.parquet").to_pandas().sort_values("ts")
    d["date"] = d["ts"].map(hkt)
    return d[["date","close"]].set_index("date").sort_index()

def feats_base(c):
    ret = c.pct_change()
    f = pd.DataFrame(index=c.index)
    f["mom5"] = c/c.shift(5)-1
    f["mom10"] = c/c.shift(10)-1
    f["vol10"] = ret.rolling(10).std()
    f["above_sma20"] = c/c.rolling(20).mean()-1
    return f.ffill().fillna(0.0)

def walk_knn(X, y, eps):
    m = len(y); i0 = int(m*0.4); rows = []
    Xv = X.values.astype(float)
    for i in range(i0, m):
        tr = np.arange(0, i-H+1)
        if len(tr) < 30: continue
        mu, sd = Xv[tr].mean(0), Xv[tr].std(0).clip(min=1e-9)
        q = (Xv[i]-mu)/sd; z = (Xv[tr]-mu)/sd
        nb = np.argsort(((z-q)**2).sum(1), kind="stable")[:min(K, len(tr))]
        rows.append((y[i], float(y[nb].mean())))
    return rows

epsilons = [0.0, 0.002, 0.005]
agg = {e: [] for e in epsilons}
print(f"{'symbol':<9}{'m':>5} | " + " | ".join(f"eps={e}".ljust(9) for e in epsilons))
for sym in SYMS:
    df = stock_df(sym); c = df["close"].astype(float); m = len(c)-H
    fwd = c.shift(-H); rets = (fwd/c-1).iloc[:m]
    F = feats_base(c).iloc[:m]
    line = [sym, f"{m:>4}"]
    for eps in epsilons:
        y = (rets > eps).astype(int).values
        r = walk_knn(F, y, eps)
        if r:
            h = float(np.mean([(p > 0.5) == (u == 1) for u, p in r]))
            agg[eps].extend(r)
            line.append(f"{h:.3f}".ljust(9))
    print(" | ".join(str(x) for x in line))
print("\nweighted:")
for eps in epsilons:
    r = agg[eps]; n = len(r)
    if n:
        print(f"  eps={eps}: hit={np.mean([(p>0.5)==(u==1) for u,p in r]):.3f} n={n}")
