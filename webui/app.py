"""webui/app.py — gtvdb 持倉預測 / 量化研究 Web UI（Streamlit）。

執行：
    streamlit run webui/app.py

功能：
  1) 持倉管理（HOLDING.txt / HOLDING.cfg 維護）
  2) 預測未來 7 交易日走勢儀表板（direction 3態 + action 3態 + 恆指情境）
  3) 指定日期回歸測試（fwd_regress as-of）
  4) 其他已實現功能界面：紙上模擬、診斷圖、資料品質/健康、SQL 控制台、恆指情境圖

引擎呼叫走 webui/gtv_bridge.py（與 stock_*.sh 相同的 gtv binary 驅動模式）。
"""
from __future__ import annotations

import datetime
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

from gtv_bridge import (
    ROOT, OUT,
    bars_parquet, bars_symbols,
    read_text, write_text,
    read_index_daily,
    regress_on, fwd_walk_table, run_sql_table,
)

st.set_page_config(page_title="gtvdb 持倉預測 UI", page_icon="📈", layout="wide")

ZH_HDR = {
    "symbol": "代碼", "name": "名稱", "close": "收市", "p_raw": "P原始", "p_cal": "P校準",
    "direction": "方向", "action": "動作", "sim_L": "多單(n/bp)", "sim_S": "空單(n/bp)",
    "decision": "警報", "horizon": "週期(日)", "date": "日期", "hsi": "恆指",
    "hsi_ret5": "恆指5日", "hsi_ret20": "恆指20日", "hsi_ret60": "恆指60日",
    "stk_ret5": "個股5日", "stk_ret20": "個股20日", "rel20": "相對強度20日",
}
TR = {"UP": "升", "DOWN": "跌", "FLAT": "橫行", "BUY": "買", "HOLD": "持", "SELL": "賣"}


def to_zh(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    cols = list(d.columns)
    d = d.rename(columns=ZH_HDR)
    if "方向" in d.columns:
        d["方向"] = d["方向"].map(lambda v: TR.get(str(v), v))
    if "動作" in d.columns:
        d["動作"] = d["動作"].map(lambda v: TR.get(str(v), v))
    keep = ["代碼", "名稱", "收市", "P校準", "方向", "動作", "多單(n/bp)", "空單(n/bp)", "警報", "週期(日)"]
    shown = [c for c in keep if c in d.columns] + [c for c in d.columns if c not in keep]
    return d[[c for c in shown if c in d.columns]]


def gtv_profile() -> str:
    release = ROOT / "target" / "release" / "gtv"
    return "release" if release.exists() else "debug"


def hkt_date(ts_ns):
    """engine ts (UTC ns, HKT-midnight) -> HKT date"""
    return (pd.to_datetime(ts_ns, unit="ns") + pd.Timedelta(hours=8)).dt.date


def acc_metrics(df: pd.DataFrame) -> dict | None:
    d = df[["p_up", "up"]].dropna()
    if len(d) < 10:
        return None
    p = d["p_up"].astype(float).values
    y = (d["up"].astype(float) > 0.5).astype(int).values
    n = len(y)
    raw = float(np.mean((p > 0.5) == (y == 1)))
    st_ = np.maximum(p, 1 - p)

    def bucket_hit(mask):
        if mask.sum() < 5:
            return None, int(mask.sum())
        return float(np.mean((p > 0.5) == (y == 1))), int(mask.sum())

    h60, n60 = bucket_hit(st_ >= 0.60)
    h70, n70 = bucket_hit(st_ >= 0.70)
    h75, n75 = bucket_hit(st_ >= 0.75)
    ece = 0.0
    for b in range(10):
        m = (p >= b / 10) & (p < (b + 1) / 10)
        if m.sum():
            ece += abs(p[m].mean() - y[m].mean()) * m.sum()
    return {"n": n, "raw": raw, "h60": h60, "n60": n60, "h70": h70, "n70": n70,
            "h75": h75, "n75": n75, "ece": ece / n}



@st.cache_data(ttl=10)
def read_forecast(horizon: int = 7) -> pd.DataFrame:
    # cfg 模式（預設 7 日、可含每股自訂 H）寫 holdings_forecast_7d.tsv；
    # 全體 14 日寫 holdings_forecast_14d.tsv
    name = f"holdings_forecast_{horizon}d.tsv"
    for p in (OUT / name, OUT / "holdings_forecast.tsv"):
        if p.exists():
            df = pd.read_csv(p, sep="\t")
            if "name" not in df.columns:
                df.insert(1, "name", "")
            return df
    return pd.DataFrame()


@st.cache_data(ttl=10)
def read_tsv(name: str) -> pd.DataFrame:
    p = OUT / name
    if p.exists():
        return pd.read_csv(p, sep="\t")
    return pd.DataFrame()


def run_holdings_script(horizon: int = 7, timeout: int = 2400) -> str:
    env = dict(os.environ)
    env.update({"GTV_PROFILE": gtv_profile(), "SOURCE": "futu", "HORIZON": str(horizon)})
    if horizon != 7:
        env["CFG_FILE"] = "/dev/null"  # 非預設週期：全體同 H，不套用每股 cfg
    p = subprocess.run(
        ["bash", "holdings_forecast.sh"],
        cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=timeout,
    )
    return p.stdout + p.stderr


# --------------------------------------------------------------------------- UI
st.sidebar.title("📈 gtvdb 持倉預測")
page = st.sidebar.radio(
    "功能",
    ["📊 儀表板：7日預測", "📝 持倉管理", "🔬 指定日期回歸測試",
     "📈 模擬 / 診斷 / 情境圖", "📉 技術指標（引擎 UDF）", "🩺 資料品質與健康",
     "🎯 全持倉×H 正確率/校準", "🕯 K線＋高信心訊號", "🧪 SQL 控制台"],
)
st.sidebar.caption(
    f"GTV bin: `{gtv_profile()}` · bars: {len(bars_symbols())} 隻\n"
    "研究用途，非投資建議"
)

if page == "📊 儀表板：7日預測":
    st.title("未來走勢預測（已校準）")
    horizon = st.selectbox("預測週期（交易日）", [1, 7, 14], index=1,
                           format_func=lambda h: ("下一個交易日（H=1）" if h == 1
                                                  else f"未來 {h} 交易日" + ("（預設/cfg 每股）" if h == 7 else "（全體同 H）")))
    if st.button("🔄 重新執行每日預測（需要 Futu OpenD 登入）"):
        with st.spinner(f"執行 holdings_forecast.sh（H={horizon}，可能需數分鐘）…"):
            try:
                log = run_holdings_script(horizon)
                st.code(log[-3000:])
                read_forecast.clear()
            except Exception as e:  # noqa: BLE001
                st.error(f"執行失敗：{e}")
    df = read_forecast(horizon)
    if df.empty:
        st.info("尚無預測結果。請先執行上方「重新執行每日預測」。")
    else:
        st.subheader("訊號統計")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("持有股票", len(df))
        act = df["action"].value_counts()
        c2.metric("買(BUY)", int(act.get("BUY", 0)))
        c3.metric("持(HOLD)", int(act.get("HOLD", 0)))
        c4.metric("賣(SELL)", int(act.get("SELL", 0)))
        st.dataframe(to_zh(df), use_container_width=True, hide_index=True)
        col_l, col_r = st.columns([3, 2])
        with col_l:
            st.subheader("P(未來上升｜校準)")
            chart = df[["symbol", "p_cal"]].set_index("symbol")
            st.bar_chart(chart, color="#4a9")  # type: ignore[call-arg]
        with col_r:
            st.subheader("方向 / 動作分佈")
            dirc = df["direction"].value_counts().reindex(["UP", "FLAT", "DOWN"], fill_value=0)
            st.bar_chart(dirc)
        fs = {h: read_forecast(h) for h in (1, 7, 14)}
        if sum(1 for d in fs.values() if not d.empty) >= 2:
            with st.expander("H=1 / H=7 / H=14 三週期並排對比"):
                cols = {"symbol": "symbol"}
                for h in (1, 7, 14):
                    d = fs[h]
                    if d.empty:
                        continue
                    part = d[["symbol", "p_cal", "direction", "action"]].rename(columns={
                        "p_cal": f"p_cal_{h}", "direction": f"方向_{h}", "action": f"動作_{h}"})
                    cols[f"p_{h}"] = part
                cmp = fs[7][["symbol"]]
                for h in (1, 7, 14):
                    if f"p_{h}" in cols:
                        cmp = cmp.merge(cols[f"p_{h}"], on="symbol", how="outer")
                st.dataframe(cmp, use_container_width=True, hide_index=True)
                st.caption("p_cal = 已校準上升機率；方向: 升/橫行/跌；動作: 買/持/賣（短歷史或模擬為負會顯示持）")
        with st.expander("恆指情境欄位說明"):
            st.markdown(
                "`hsi_ret5/20/60` = 恆指近期報酬；`rel20` = 個股20日報酬 − 恆指20日報酬 "
                "（>0 表示跑贏大市）。訊號要對照市場看，避免把 beta 當 alpha。"
            )
        ind = read_tsv("holdings_indicators.tsv")
        if not ind.empty:
            with st.expander("技術指標快照（決策 bar，引擎同公式）"):
                st.dataframe(ind, use_container_width=True, hide_index=True)
                st.caption(
                    "RSI14 / MACD 直方圖 / 布林帶寬% / ATR_ratio(7/21) / HV_ratio(10/30) / "
                    "放量(vs MA20) / EMA5 斜率% / 趨勢regime(EMA20−60)/close%"
                )

elif page == "📝 持倉管理":
    st.title("持倉清單維護")
    st.markdown(
        "編輯 **HOLDING.txt**（代碼+中文名+網址）與 **HOLDING.cfg**（每股 HORIZON/門檻）。"
        "代碼範例 `HK.00857`（5 位補零）。"
    )
    t1 = st.text_area("HOLDING.txt 內容", read_text(ROOT / "HOLDING.txt"), height=240)
    if st.button("💾 儲存 HOLDING.txt", key="s1"):
        write_text(ROOT / "HOLDING.txt", t1)
        st.success("已儲存。重新執行預測即套用。")
    st.divider()
    t2 = st.text_area("HOLDING.cfg（每股 HORIZON THRESHOLD，可留空）", read_text(ROOT / "HOLDING.cfg"), height=160)
    if st.button("💾 儲存 HOLDING.cfg", key="s2"):
        write_text(ROOT / "HOLDING.cfg", t2)
        st.success("已儲存。格式：`HK.00857 20 0.85`")
    st.divider()
    with st.expander("快速新增持股"):
        code = st.text_input("代碼", placeholder="HK.00857")
        zh = st.text_input("中文名", placeholder="中國石油股份")
        url = st.text_input("網址（可略）", placeholder="https://www.futunn.com/hk/stock/00857-HK")
        if st.button("➕ 附加到 HOLDING.txt"):
            line = f"{code.strip()}  {zh.strip()}  {url.strip()}".rstrip()
            write_text(ROOT / "HOLDING.txt", read_text(ROOT / "HOLDING.txt").rstrip() + "\n" + line + "\n")
            st.success(f"已加：{line}")

elif page == "🔬 指定日期回歸測試":
    st.title("As-of 回歸測試（指定過去日期為決策日）")
    syms = bars_symbols()
    symbol = st.selectbox("股票", syms) if syms else st.text_input("股票代碼")
    today = datetime.date.today()
    asof = st.date_input("決策日（需早於最後一個 bar ≥ H 日，才有未來可對比）", today - datetime.timedelta(days=30))
    H = st.slider("H（未來交易日）", 1, 20, 7)
    K = st.number_input("K（類比數）", 1, 100, 20)
    if st.button("執行 fwd_regress", type="primary"):
        try:
            df = regress_on(symbol, asof, int(H), int(K))
        except Exception as e:  # noqa: BLE001
            st.error(f"執行失敗：{e}")
            df = pd.DataFrame()
        if not df.empty:
            r = df.iloc[0]
            st.subheader(f"{symbol} · 決策日 {asof} · H={H}")
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("P(上升)", f"{r.get('p_up', float('nan')):.3f}")
            m2.metric("P(下跌)", f"{r.get('p_down', float('nan')):.3f}")
            m3.metric("預測區間", f"[{r.get('pred_lo',0):.2%}, {r.get('pred_hi',0):.2%}]")
            m4.metric("實際區間", f"[{r.get('act_lo',0):.2%}, {r.get('act_hi',0):.2%}]")
            m5.metric("實際H日報酬", f"{r.get('act_ret', float('nan')):.2%}")
            up = r.get("up")
            if up == 1:
                st.success("實際結果：上升（方向命中 ✓）")
            elif up == 0:
                st.warning("實際結果：下跌 / 平盤（方向未命中 ✗）")
            else:
                st.info("n_future < H：該日期之後的資料不足，暫無完整結果可比對。")
            st.caption(
                "「預測區間」由類比鄰居的 H 日路徑 10–90% 分位給出；與實際開高低收路徑 "
                "（act_lo/act_hi）重疊程度即區間預測品質。"
            )
            st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("多決策日回測統計（選定週期 → 對比實際 → 正確率）")
    st.markdown(
        "在過去日期範圍內每隔幾日取決策日，跑 fwd_regress（H 可選 1/7/14），"
        "統計**方向正確率、區間覆蓋率、收益誤差**。日期須早於最後一根 bar 至少 H 日才會有完整實際。"
    )
    msym = st.selectbox("股票", bars_symbols(), key="regsym2")
    mh = st.radio("H（預測週期）", [1, 7, 14], index=1, horizontal=True, key="regh2")
    today = datetime.date.today()
    col_a, col_b, col_c = st.columns([2, 2, 1])
    start_d = col_a.date_input("起始日", today - datetime.timedelta(days=260), key="regs")
    end_d = col_b.date_input("結束日", today - datetime.timedelta(days=30), key="rege")
    step_d = col_c.number_input("取樣間隔(日)", 1, 90, 10, key="regstep")
    if st.button("執行多日回測統計", type="primary"):
        if start_d >= end_d:
            st.error("起始日須早於結束日")
        else:
            import gtv_bridge as gb
            dates: list[datetime.date] = []
            dd = start_d
            while dd <= end_d:
                dates.append(dd)
                dd += datetime.timedelta(days=int(step_d))
            rows = []
            prog = st.progress(0.0)
            for i, ad in enumerate(dates):
                try:
                    df1 = gb.regress_on(msym, ad, int(mh), 20)
                except Exception:  # noqa: BLE001
                    df1 = pd.DataFrame()
                if not df1.empty:
                    r = df1.iloc[0]
                    rows.append({
                        "date": ad,
                        "p_up": r.get("p_up", float("nan")),
                        "pred_lo": r.get("pred_lo", float("nan")),
                        "pred_hi": r.get("pred_hi", float("nan")),
                        "pred_ret": r.get("pred_ret", float("nan")),
                        "act_lo": r.get("act_lo", float("nan")),
                        "act_hi": r.get("act_hi", float("nan")),
                        "act_ret": r.get("act_ret", float("nan")),
                        "up": r.get("up", -1),
                        "n_future": r.get("n_future", 0),
                    })
                prog.progress((i + 1) / len(dates))
            prog.empty()
            if not rows:
                st.warning("沒有可用的決策日（歷史不足？）")
            else:
                rd = pd.DataFrame(rows)
                full = rd[rd["up"] != -1]
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("回測決策日", len(rd))
                m2.metric("完整結果(H 日實際)", len(full))
                if not full.empty:
                    pred_up = (full["p_up"] > 0.5).astype(int)
                    m3.metric("方向正確率", f"{(pred_up == full['up']).mean():.1%}")
                    inside = (full["act_lo"] >= full["pred_lo"]) & (full["act_hi"] <= full["pred_hi"])
                    m4.metric("區間覆蓋率", f"{inside.mean():.1%}")
                    err = (full["act_ret"] - full["pred_ret"]).abs()
                    m5.metric("收益 MAE", f"{err.mean():.2%}")
                    hits = int((pred_up == full['up']).sum())
                    st.caption(f"方向正確 = 預測升(p_up>0.5)且實際升 或 預測跌且實際跌：{hits}/{len(full)}。")
                st.dataframe(rd, use_container_width=True, hide_index=True)

        if st.button("執行三週期正確率對比（H=1/7/14，同日期集）", key="reg3h"):
            if start_d >= end_d:
                st.error("起始日須早於結束日")
            else:
                import gtv_bridge as gb
                dates3: list[datetime.date] = []
                dd = start_d
                while dd <= end_d:
                    dates3.append(dd)
                    dd += datetime.timedelta(days=int(step_d))
                res = []
                prog = st.progress(0.0)
                total = len(dates3) * 3
                k = 0
                for hh in (1, 7, 14):
                    rows3 = []
                    for ad in dates3:
                        try:
                            r = gb.regress_on(msym, ad, hh, 20).iloc[0]
                        except Exception:  # noqa: BLE001
                            r = None
                        if r is not None and r.get("up", -1) != -1:
                            rows3.append(r)
                        k += 1
                        prog.progress(k / total)
                    if rows3:
                        rr = pd.DataFrame(rows3)
                        pu = (rr["p_up"] > 0.5).astype(int)
                        res.append({
                            "H": hh,
                            "決策日": len(rows3),
                            "方向正確率": f"{(pu == rr['up']).mean():.1%}",
                            "區間覆蓋": f"{((rr['act_lo'] >= rr['pred_lo']) & (rr['act_hi'] <= rr['pred_hi'])).mean():.1%}",
                            "收益MAE": f"{(rr['act_ret'] - rr['pred_ret']).abs().mean():.2%}",
                        })
                prog.empty()
                if res:
                    tbl = pd.DataFrame(res)
                    st.subheader("H=1 / H=7 / H=14 正確率對比")
                    st.dataframe(tbl, use_container_width=True, hide_index=True)
                    acc = tbl.copy()
                    acc["acc"] = acc["方向正確率"].str.rstrip("%").astype(float)
                    st.bar_chart(acc.set_index("H")[["acc"]])  # type: ignore[call-arg]
                    st.caption("同日期集各自以 H 日實際比對；決策日數越靠近現在越少（H 大者需更久未來）。")

elif page == "📈 模擬 / 診斷 / 情境圖":
    st.title("紙上模擬、診斷與市場情境")
    tab_sim, tab_gap, tab_idx, tab_diag = st.tabs(["📄 訊號模擬", "🌊 週末跳空", "🏦 恆指情境", "🔍 診斷"])
    syms = bars_symbols()
    with tab_sim:
        symbol = st.selectbox("股票（模擬）", syms, key="simsym") if syms else None
        Hsel = st.radio("週期 H", [1, 7, 14, 20], horizontal=True, key="simh")
        if symbol:
            df = read_tsv(f"{symbol.replace('.','_')}_sim_H{Hsel}d.tsv")
            if df.empty:
                st.info("此股票尚未跑模擬。可用 `./stock_sim.sh <SYMBOL>` 產生（見 README），或由其他界面觸發。")
            else:
                c0 = df[df["cost_bps"] == 0.0]
                st.subheader(f"{symbol} · H={Hsel} · 成本 0 bps：THR 掃描")
                st.line_chart(c0.set_index("thr")[["mean_net_bp", "maxdd_sized_pct"]])
                st.dataframe(df, use_container_width=True, hide_index=True)
        act = read_tsv("holdings_action_7d.tsv")
        if not act.empty:
            st.subheader("組合動作彙總（holdings_action_7d.tsv）")
            st.dataframe(act, use_container_width=True, hide_index=True)
    with tab_gap:
        gap = read_tsv("holdings_weekend_gap.tsv")
        if gap.empty:
            st.info("先跑 ./holdings_forecast.sh 產生週末跳空附錄。")
        else:
            st.subheader("週末/長假開盤跳空風險（avg |gap| 與 p98）")
            st.bar_chart(gap.set_index("symbol")[["wk_avg_gap%", "p98%"]])
            st.dataframe(gap, use_container_width=True, hide_index=True)
    with tab_idx:
        idx = read_index_daily()
        if idx.empty:
            st.info("無恆指日線（HK_INDEX_daily.parquet）。")
        else:
            idx = idx.set_index("date")
            norm = idx[["hsi", "hstech"]] / idx[["hsi", "hstech"]].iloc[0] * 100
            st.subheader("恆指 / 恆生科技（基準=100）")
            st.line_chart(norm)
    with tab_diag:
        st.markdown(
            "引擎診斷四件套（calibration / quantile coverage / tail error / regime）由 "
            "`./stock_diag.sh <SYMBOL>` 產出到 analytics_out/*.csv。選一隻執行後回此頁即可見圖："
        )
        symbol = st.selectbox("股票（診斷）", syms, key="diagsym") if syms else None
        if symbol and st.button("執行 stock_diag.sh（parquet 離線）"):
            barf = bars_parquet(symbol)
            env = dict(os.environ)
            env.update({"SOURCE": "parquet", "FILE": barf or "", "GTV_PROFILE": gtv_profile()})
            p = subprocess.run(["bash", "stock_diag.sh", symbol], cwd=str(ROOT),
                               env=env, capture_output=True, text=True, timeout=1200)
            st.code((p.stdout or "")[-2000:])
        cal = OUT / "calibration_points.csv"
        if cal.exists():
            d = pd.read_csv(cal)
            st.subheader("Calibration（p_up vs 實測漲率）")
            st.scatter_chart(d.set_index("mean_p_up")["empirical_up_rate"])
        qc = OUT / "quantile_coverage.csv"
        if qc.exists():
            st.dataframe(pd.read_csv(qc), use_container_width=True, hide_index=True)

elif page == "📉 技術指標（引擎 UDF）":
    st.title("技術指標（Wave 0 / A / B 引擎 WindowUDF）")
    syms = bars_symbols()
    symbol = st.selectbox("股票", syms) if syms else None
    st.caption(
        "指標在引擎內因果計算（sqlmode=full）。Wave A：ema_slope/accel、bb_width、macd_distance/slope、"
        "atr_ratio、hv/hv_ratio、vol_spike、obv(+slope)、vwap_dev、hh/ll、big_green/red、長影、"
        "regime_trend/vol/event、gap_up/down；Wave B：xrank/rank_cs、rs_index/rs、beta_index/beta。"
    )
    if symbol:
        sql = ("SELECT ts, close, ema(close,10) OVER (ORDER BY ts) e10, "
               "ema(close,20) OVER (ORDER BY ts) e20, "
               "rsi(close,14) OVER (ORDER BY ts) rsi14, "
               "macd_hist(close,12,26,9) OVER (ORDER BY ts) macdh, "
               "bb_width(close,20) OVER (ORDER BY ts) bbw, "
               "atr_ratio(high,low,close,7,21) OVER (ORDER BY ts) atrr, "
               "vol_spike(volume,20) OVER (ORDER BY ts) vspk "
               "FROM {t}")
        if st.button("讀取指標序列", type="primary"):
            try:
                df = run_sql_table(sql, preload=symbol)
            except Exception as e:  # noqa: BLE001
                st.error(f"執行失敗：{e}")
                df = pd.DataFrame()
            if not df.empty:
                df["dt"] = pd.to_datetime(df["ts"].astype("int64"), unit="ns")
                df = df.set_index("dt").drop(columns=["ts"])
                st.subheader("價格與 EMA10/20")
                st.line_chart(df[["close", "e10", "e20"]])
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("RSI(14)")
                    st.line_chart(df["rsi14"])
                    st.subheader("布林帶寬")
                    st.line_chart(df["bbw"])
                with c2:
                    st.subheader("MACD 直方圖")
                    st.line_chart(df["macdh"])
                    st.subheader("放量(vol_spike)")
                    st.line_chart(df["vspk"])
                st.subheader("ATR_ratio(7/21)")
                st.line_chart(df["atrr"])
                with st.expander("原始資料（尾 60 列）"):
                    st.dataframe(df.tail(60), use_container_width=True)

elif page == "🩺 資料品質與健康":
    st.title("資料品質 / 健康度 / 策略漂移")
    syms = bars_symbols()
    symbol = st.selectbox("表（bars parquet）", syms) if syms else st.text_input("股票代碼")
    mode = st.radio("檢查", ["health_check（含 freshness）", "dq_report / dq_check", "strategy_stats（決策表 up+p_up）"],
                    horizontal=True)
    if st.button("執行", type="primary"):
        try:
            if mode.startswith("health"):
                df = run_sql_table("SELECT * FROM health_check('{t}', 7, 50)", preload=symbol)
            elif mode.startswith("dq_report"):
                df1 = run_sql_table("SELECT * FROM dq_report('{t}')", preload=symbol)
                st.subheader("dq_report")
                st.dataframe(df1, use_container_width=True, hide_index=True)
                df = run_sql_table("SELECT * FROM dq_check('{t}')", preload=symbol)
                st.subheader("dq_check（僅列出有問題項目，空表=乾淨）")
            else:
                df = run_sql_table("SELECT * FROM strategy_stats('{t}')", preload=symbol)
        except Exception as e:  # noqa: BLE001
            st.error(f"執行失敗：{e}")
            df = pd.DataFrame()
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(
        "strategy_stats 需要表含 `up`（實際 0/1）與 `p_up`/`cal_p_up_*`（預測機率）欄 —— "
        "例如把 fwd_walk 決策表存成 parquet 再 load。"
    )

elif page == "🎯 全持倉×H 正確率/校準":
    st.title("全持倉 × H 正確率 / 校準對照")
    st.markdown(
        "對每檔持倉跑引擎 walk-forward（H=1/7/14），在**同一個因果回放**上統計：\n"
        "整體方向命中（raw）、高信心桶（強度≥0.60/0.70/0.75）命中、ECE(校準誤差)。\n"
        "**看點**：raw≈50% 正常（短線方向接近硬幣）；真正有用的是『高信心桶命中 > raw』與低 ECE。"
    )
    all_acc = st.session_state.get("acc3", {})
    if st.button("執行（全部持倉 × H=1/7/14，需數分鐘）"):
        import gtv_bridge as gb
        syms = bars_symbols()
        acc = {}
        prog = st.progress(0.0)
        jobs = [(s, h) for h in (1, 7, 14) for s in syms]
        for i, (s, h) in enumerate(jobs):
            try:
                w = gb.fwd_walk_table(s, h, 20, 0.3)
                m = acc_metrics(w)
                if m:
                    acc[(s, h)] = m
            except Exception:  # noqa: BLE001
                pass
            prog.progress((i + 1) / len(jobs))
        prog.empty()
        st.session_state["acc3"] = acc
        all_acc = acc
    if not all_acc:
        st.info("尚未計算。按上方按鈕（引擎 walk-forward，含新上市短歷史股會略過）。")
    else:
        hs = st.radio("顯示 H", [1, 7, 14], index=1, horizontal=True, key="acch")
        rows = []
        for (s, h), m in all_acc.items():
            if h != hs:
                continue
            rows.append({"symbol": s, "n": m["n"], "raw命中": f"{m['raw']:.1%}",
                         "hit≥0.60": f"{m['h60']:.1%}({m['n60']})" if m["h60"] is not None else "-",
                         "hit≥0.70": f"{m['h70']:.1%}({m['n70']})" if m["h70"] is not None else "-",
                         "hit≥0.75": f"{m['h75']:.1%}({m['n75']})" if m["h75"] is not None else "-",
                         "ECE": f"{m['ece']:.3f}"})
        tbl = pd.DataFrame(rows)
        st.subheader(f"H={hs}")
        st.dataframe(tbl, use_container_width=True, hide_index=True)
        num = pd.DataFrame([{k: v for k, v in m.items()} for (s, h), m in all_acc.items() if h == hs])
        if not num.empty:
            num = num.replace({None: np.nan})
            num = num.apply(pd.to_numeric, errors="coerce")
            pooled = num[["n", "raw", "h60", "h70", "h75", "ece"]].sum()
            pooled_hit = lambda col: (num[col] * num["n"]).sum() / pooled["n"]  # noqa: E731
            st.caption(
                f"加權合計：n={int(pooled['n'])} · raw命中={pooled_hit('raw'):.1%} · "
                f"≥0.60命中={pooled_hit('h60'):.1%} · ≥0.70={pooled_hit('h70'):.1%} · "
                f"≥0.75={pooled_hit('h75'):.1%} · 加權ECE={pooled['ece'] / (len(num) or 1):.3f}"
            )
            chart = num[["raw", "h60", "h70", "h75"]] * 100
            chart.index = [s for (s, h), _ in all_acc.items() if h == hs]
            st.bar_chart(chart)
            st.caption(
                "條越高越準；若『高信心桶(h60/h70/h75) 明顯高於 raw』代表訊號有選擇性（只在高信心時出手），"
                "反之則該股/該週期屬噪音。"
            )

elif page == "🕯 K線＋高信心訊號":
    st.title("K線圖 ＋ 高信心買入/賣出訊號位置")
    st.markdown("選股票後載入日 K，歷史回放中『校準強度 ≥ 門檻』的日子在圖上標 ▲買入 / ▼賣出(減持/避開)。")
    syms = bars_symbols()
    ksym = st.selectbox("股票", syms, key="kline_sym")
    kh = st.radio("訊號週期 H", [1, 7, 14], index=1, horizontal=True, key="kline_h")
    thr = st.slider("高信心門檻（強度）", 0.55, 0.90, 0.65, 0.05)
    nbars = st.radio("顯示最近交易日數", [60, 90, 180, 360], index=2, horizontal=True, key="kline_n")
    if ksym and st.button("繪製 K線 + 訊號", type="primary"):
        import plotly.graph_objects as go
        import gtv_bridge as gb
        parquet = gb.bars_parquet(ksym)
        if parquet is None:
            st.error("沒有 bars（先抓資料）")
        else:
            bars = pd.read_parquet(parquet).sort_values("ts").tail(nbars)
            # 統一以 HKT 日期當 x 軸（避免 ts 時區偏移造成標記漂移）
            bdates = hkt_date(bars["ts"])
            xbar = pd.to_datetime(pd.Series(list(bdates), index=bars.index))
            close_by = dict(zip(bdates, bars["close"].astype(float)))
            try:
                w = gb.fwd_walk_table(ksym, int(kh), 20, 0.3)
            except Exception as e:  # noqa: BLE001
                st.error(f"訊號計算失敗：{e}")
                w = pd.DataFrame()
            fig = go.Figure()
            fig.add_trace(go.Candlestick(
                x=xbar, open=bars["open"], high=bars["high"], low=bars["low"],
                close=bars["close"], name=ksym,
                increasing_line_color="#e02020", decreasing_line_color="#1f8a4c",
            ))
            buy = sell = pd.DataFrame()
            if not w.empty and "p_up" in w.columns:
                sig = w.copy()
                sig["dt"] = hkt_date(sig["t"])
                sig["p"] = pd.to_numeric(sig["p_up"], errors="coerce")
                sig = sig.dropna(subset=["p"])
                sig["strength"] = np.maximum(sig["p"], 1 - sig["p"])
                sig = sig[(sig["strength"] >= thr) & sig["dt"].isin(set(bdates))]
                if not sig.empty:
                    sig["y"] = sig["dt"].map(close_by)
                    sig["xm"] = pd.to_datetime(sig["dt"].astype(str))
                    sig = sig.dropna(subset=["y"])
                    buy = sig[sig["p"] >= 0.5]
                    sell = sig[sig["p"] < 0.5]
                    for lab, sub, sym_g, color in [
                        ("買入↑(信號)", buy, "arrow-up", "#1f6fd8"),
                        ("賣出↓(信號)", sell, "arrow-down", "#f7b500"),
                    ]:
                        if sub.empty:
                            continue
                        texts = [f"{lab}: P={p * 100:.0f}%" for p in sub["p"]]
                        fig.add_trace(go.Scatter(
                            x=sub["xm"], y=sub["y"], mode="markers", name=lab, text=texts,
                            hovertemplate="%{text}<extra></extra>",
                            marker=dict(symbol=sym_g, size=15, color=color),
                        ))
            fig.update_layout(
                height=680, xaxis_rangeslider_visible=False,
                legend=dict(title="訊號", orientation="h", y=1.02, x=0),
                title=f"{ksym} · K線（最近 {nbars} 根）＋ 高信心訊號(H={kh}, 強度≥{thr:.2f})",
            )
            st.plotly_chart(fig, use_container_width=True)
            nb, ns_ = len(buy), len(sell)
            if nb == 0 and ns_ == 0:
                st.warning(
                    f"⚠ 此範圍（最近 {nbars} 根、強度≥{thr:.2f}）內沒有高信心訊號。"
                    "可降低門檻或選 360 根再看；有訊號時圖右上角會出現 買入↑ / 賣出↓ 圖例。"
                )
            else:
                st.success(f"本範圍內標記：買入↑ {nb} 個、賣出↓ {ns_} 個（hover 看 P）。")
            st.caption(
                "↑買入 / ↓賣出：該歷史決策日類比模型信心 max(p_up,1−p_up)≥門檻，標在當日收盤價。"
                "為『模型當時的信心』非事後諸葛；實際出手前會再經 Platt/isotonic 校準（更保守）。"
            )

else:  # SQL 控制台
    st.title("SQL 控制台（full mode）")
    st.caption("可先選一隻 bars parquet 自動 load 成 `{t}`，SQL 裡以 {t} 引用。")
    preload = st.selectbox("preload 表（可選）", [""] + bars_symbols())
    sql = st.text_area(
        "SQL",
        height=180,
        value="SELECT * FROM dq_report('{t}')",
    )
    if st.button("執行 SQL", type="primary"):
        try:
            df = run_sql_table(sql, preload=preload or None)
        except Exception as e:  # noqa: BLE001
            st.error(f"SQL 失敗：{e}")
            df = pd.DataFrame()
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.success("執行成功（無表格輸出）。")
    st.markdown(
        "可用函數示例：技術指標 `rsi(close,14) OVER (ORDER BY t)`、`boll_up(close,20,2)`…；"
        "`align('表',freq_ns,'ffill')`、`backtest/bt_report/pf_report`、`cov/pca/var_historical`、"
        "`ohlc`、`fwd_*`。視窗函數需在 full mode（本頁已自動切換）。"
    )
