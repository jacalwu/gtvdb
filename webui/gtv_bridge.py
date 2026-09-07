"""webui/gtv_bridge.py — 呼叫 gtv REPL/引擎 + 檔案讀寫 helper。

網頁端不重造引擎：與 `holdings_forecast.sh` 等腳本相同模式 —— 把指令餵給
`gtv` binary（SQL / 裸 table fn），解析其印出的 '|' 表格為 DataFrame。
"""
from __future__ import annotations

import datetime
import glob
import os
import re
import subprocess
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "analytics_out"
GTV_BIN = None


def gtv_bin() -> Path:
    global GTV_BIN
    if GTV_BIN is None:
        cands = [ROOT / "target" / "release" / "gtv", ROOT / "target" / "debug" / "gtv"]
        cands = [c for c in cands if c.exists()]
        if not cands:
            raise FileNotFoundError("未找到 gtv binary，請先 cargo build")
        # 以最新的為準：release 若沒重build 會缺新 UDF（Wave A/B 等）
        GTV_BIN = max(cands, key=lambda p: p.stat().st_mtime)
    return GTV_BIN


def run_gtv(lines: list[str], timeout: int = 600) -> str:
    """在 repo 根目錄執行 gtv，餵入 lines（自動補 quit），回傳 stdout。"""
    script = "\n".join(lines) + "\nquit\n"
    p = subprocess.run(
        [str(gtv_bin())],
        input=script,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        timeout=timeout,
    )
    if p.returncode != 0:
        tail = "\n".join(p.stdout.splitlines()[-8:]) + p.stderr[-800:]
        raise RuntimeError(f"gtv 執行失敗 (rc={p.returncode}):\n{tail}")
    return p.stdout


def parse_tables(out: str) -> list[tuple[list[str], list[list[str]]]]:
    """按 '+' 邊框切成 chunks；每張表 = header-chunk（首欄非數值）＋ 隨後的 data-chunk。"""
    chunks: list[list[list[str]]] = []
    cur: list[list[str]] = []

    def flush() -> None:
        chunks.append(list(cur))
        cur.clear()

    for line in out.splitlines():
        if line.startswith("+"):
            flush()
        if line.startswith("|"):
            cells = [c.strip() for c in line.split("|")]
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]
            cur.append(cells)
    flush()

    tables: list[tuple[list[str], list[list[str]]]] = []
    i = 0
    num_re = re.compile(r"[-+eE0-9.]*")
    while i < len(chunks):
        c = chunks[i]
        first = c[0][0].strip() if c else ""
        if c and not num_re.fullmatch(first):  # 首欄非數值 → header chunk
            header = c[0]
            data = chunks[i + 1] if i + 1 < len(chunks) else []
            if data:
                tables.append((header, data))
            i += 2
        else:
            i += 1
    return tables


def tables_to_df(tables: list[tuple[list[str], list[list[str]]]], index: int = 0) -> pd.DataFrame:
    """把第 index 張表轉成 DataFrame（數值自動轉 float）。"""
    if not tables:
        return pd.DataFrame()
    header, body = tables[index]
    df = pd.DataFrame(body, columns=header)
    for col in header:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def regress_on(symbol: str, asof: datetime.date, horizon: int, k: int = 20) -> pd.DataFrame:
    """fwd_regress：以指定過去日期為決策日，回測方向/區間。"""
    parquet = bars_parquet(symbol)
    if parquet is None:
        raise FileNotFoundError(f"{symbol} 沒有 bars parquet（先抓資料）")
    tbl = "r_" + re.sub(r"[^0-9A-Za-z]", "", symbol)
    asof_ns = int(datetime.datetime(asof.year, asof.month, asof.day).timestamp()) * 1_000_000_000
    out = run_gtv(
        [
            f"load {tbl} {parquet}",
            f"fwd_regress('{tbl}',{asof_ns},{horizon},{k})",
        ]
    )
    return tables_to_df(parse_tables(out))


def fwd_walk_table(symbol: str, horizon: int, k: int = 20, cal: float = 0.3) -> pd.DataFrame:
    parquet = bars_parquet(symbol)
    if parquet is None:
        raise FileNotFoundError(f"{symbol} 沒有 bars parquet（先抓資料）")
    tbl = "w_" + re.sub(r"[^0-9A-Za-z]", "", symbol)
    out = run_gtv(
        [
            f"load {tbl} {parquet}",
            f"fwd_walk('{tbl}',{horizon},{k},{cal:.3f})",
        ]
    )
    return tables_to_df(parse_tables(out))


def run_sql_table(sql: str, preload: str | None = None, timeout: int = 900) -> pd.DataFrame:
    """在 full mode 執行一條 SQL（可先 load 一個 parquet 表），回傳首張結果表。"""
    lines: list[str] = ["ALTER SESSION SET sqlmode = full"]
    if preload:
        parquet = bars_parquet(preload)
        if parquet is None:
            raise FileNotFoundError(f"{preload} 沒有 bars parquet（先抓資料）")
        tbl = "q_" + re.sub(r"[^0-9A-Za-z]", "", preload)
        lines.append(f"load {tbl} {parquet}")
        sql = sql.replace("{t}", tbl)
    lines.append(sql)
    out = run_gtv(lines, timeout=timeout)
    tables = parse_tables(out)
    if not tables:
        return pd.DataFrame()
    return tables_to_df(tables)


# ---------------------------------------------------------------------------
# 檔案 / 持倉管理
# ---------------------------------------------------------------------------

def bars_symbols() -> list[str]:
    """analytics_out 下所有 *_bars.parquet 的符號（HK.xxxxx 正規化）。"""
    syms = set()
    for f in glob.glob(str(OUT / "*_bars.parquet")):
        base = Path(f).name.replace("_bars.parquet", "").replace("HK_", "HK.")
        digits = "".join(ch for ch in base if ch.isdigit()).lstrip("0")
        if digits:
            syms.add("HK." + digits.rjust(5, "0"))
    return sorted(syms, key=lambda s: int("".join(ch for ch in s if ch.isdigit())))


def bars_parquet(symbol: str) -> str | None:
    name = symbol.replace(".", "_")
    for cand in (OUT / f"{name}_bars.parquet",):
        if cand.exists():
            return str(cand)
    return None


def read_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def read_index_daily() -> pd.DataFrame:
    for cand in (OUT / "HK_INDEX_daily.parquet", ROOT / "analytics_out" / "HK_INDEX_daily.parquet"):
        if cand.exists():
            df = pd.read_parquet(cand).reset_index()
            if "date" not in df.columns:
                df = df.reset_index()
            df["date"] = pd.to_datetime(df["date"])
            return df
    return pd.DataFrame()
