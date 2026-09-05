//! Futu provider — talks to a locally running Futu **OpenD** gateway.
//!
//! The provider shells out to the bundled Python bridge
//! (`crates/gtv-engine/python/futu_bridge.py`, embedded via [`include_str!`])
//! which drives the official `futu-api` Python SDK over OpenD. That keeps the
//! heavy protocol (protobuf, connection pooling, keyset pagination) in the
//! SDK while gtv stays language-neutral; the bridge returns strict JSON that
//! this module converts into the unified Arrow batches.
//!
//! Prerequisites (checked lazily at first call, not at construction):
//! * OpenD running and logged in (default `127.0.0.1:11111`).
//! * `python3` with the `futu-api` package.
//!
//! Environment overrides:
//! * `FUTU_OPEND_HOST` / `FUTU_OPEND_PORT` — OpenD address (defaults above).
//! * `GTV_FUTU_PY` — python interpreter (default `python3`).
//! * `GTV_FUTU_TIMEOUT_SEC` — per-request timeout (default 300 s).
//!
//! K-lines: full history via `request_history_kline`; minute-bar depth limited
//! by the account entitlement (HK LV2 covers intraday bars). Ticks: only the
//! *current session's* prints (Futu has no historical trade dump) — requires
//! the market to be open and a `TICKER` subscription to succeed.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::RecordBatch;
use serde_json::Value;

use super::{
    kline_to_batch, tick_to_batch, KlineReq, KlineRow, MarketProvider, TickReq, TickRow,
};

/// Embedded Python bridge script (kept alongside the crate so it stays in
/// sync; shipped inside the binary, so no runtime file dependency).
const BRIDGE: &str = include_str!("../../python/futu_bridge.py");

fn python() -> String {
    std::env::var("GTV_FUTU_PY").unwrap_or_else(|_| "python3".to_string())
}

fn opend_host() -> String {
    std::env::var("FUTU_OPEND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn opend_port() -> u16 {
    std::env::var("FUTU_OPEND_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11111)
}

fn timeout_secs() -> u64 {
    std::env::var("GTV_FUTU_TIMEOUT_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

/// One bridge invocation: pipe the embedded script to `python3 -`, pass the
/// request as CLI args, read strict JSON from stdout.
fn run_bridge(args: &[String]) -> Result<Value> {
    let mut cmd = Command::new(python());
    cmd.arg("-")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FUTU_OPEND_HOST", opend_host())
        .env("FUTU_OPEND_PORT", opend_port().to_string());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {} (futu bridge)", python()))?;

    // Feed the script then close stdin so the interpreter starts executing.
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("cannot open bridge stdin"))?;
        stdin
            .write_all(BRIDGE.as_bytes())
            .context("write bridge script")?;
    }

    // Drain stdout on a side thread while we poll the exit status, so a large
    // reply can never wedge the child on a full pipe.
    let stdout = child.stdout.take().expect("bridge stdout");
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>>>();
    thread::spawn(move || {
        use std::io::Read;
        let mut buf = Vec::new();
        let mut stdout = stdout;
        let r = stdout
            .read_to_end(&mut buf)
            .map(|_| buf)
            .map_err(|e| anyhow!("read bridge stdout: {e}"));
        let _ = tx.send(r);
    });

    let deadline = Duration::from_secs(timeout_secs());
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) => {
                if start.elapsed() > deadline {
                    let _ = child.kill();
                    bail!("futu bridge timed out after {} s", timeout_secs());
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => bail!("futu bridge wait: {e}"),
        }
    };

    let mut stderr = String::new();
    if let Some(mut err) = child.stderr.take() {
        use std::io::Read;
        let _ = err.read_to_string(&mut stderr);
    }
    let out_bytes = rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| anyhow!("futu bridge stdout reader lost"))??;

    if !status.success() {
        bail!(
            "futu bridge exited {}: {}",
            status.code().map(|c| c.to_string()).unwrap_or_else(|| "killed".into()),
            stderr.trim()
        );
    }
    // The futu SDK occasionally emits INFO logs to stdout *around* our JSON
    // (its `set_debug_model` toggle is not always honoured). Rather than fight
    // the SDK, extract the strict-JSON reply line: it is the only stdout line
    // that starts with `{"ok"`.
    let text = String::from_utf8_lossy(&out_bytes);
    let json_line = text
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with("{\"ok\""))
        .ok_or_else(|| {
            anyhow!(
                "futu bridge returned no JSON reply; stdout: {:?} stderr: {}",
                text.chars().take(200).collect::<String>(),
                stderr.trim()
            )
        })?;
    let out: Value = serde_json::from_str(json_line).map_err(|e| {
        anyhow!(
            "futu bridge returned non-JSON ({e}); line: {json_line:?} stderr: {}",
            stderr.trim()
        )
    })?;
    if out.get("ok").and_then(Value::as_bool) != Some(true) {
        let msg = out
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown bridge error");
        bail!("{msg}");
    }
    Ok(out)
}

fn f64_at(row: &Value, key: &str) -> f64 {
    row.get(key)
        .and_then(Value::as_f64)
        .unwrap_or(f64::NAN)
}

fn i64_at(row: &Value, key: &str) -> i64 {
    row.get(key).and_then(Value::as_i64).unwrap_or(0)
}

fn str_at(row: &Value, key: &str, fallback: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or(fallback)
        .to_string()
}

fn rows_of(out: &Value) -> Vec<Value> {
    out.get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// A `futu` provider instance. Stateless: every request spawns a fresh bridge
/// process, which opens its own short-lived OpenD connection (OpenD caps
/// concurrent API connections at 128).
pub struct FutuProvider {
    name: String,
}

impl FutuProvider {
    pub fn new() -> Self {
        Self {
            name: "futu".to_string(),
        }
    }
}

impl Default for FutuProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketProvider for FutuProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn describe(&self) -> String {
        format!(
            "futu — Futu OpenD {}:{} (needs python3 + futu-api + logged-in OpenD)",
            opend_host(),
            opend_port()
        )
    }

    fn fetch_klines(&self, req: &KlineReq) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for code in &req.codes {
            let mut args = vec![
                "--action".to_string(),
                "klines".to_string(),
                "--code".to_string(),
                code.clone(),
                "--period".to_string(),
                req.period.clone(),
                "--adjust".to_string(),
                req.adjust.clone(),
            ];
            if let Some(s) = &req.start {
                args.push("--start".to_string());
                args.push(s.clone());
            }
            if let Some(e) = &req.end {
                args.push("--end".to_string());
                args.push(e.clone());
            }
            if req.max > 0 {
                args.push("--max".to_string());
                args.push(req.max.to_string());
            }
            let out = run_bridge(&args)
                .with_context(|| format!("futu klines `{code}` period={}", req.period))?;
            let rows: Vec<KlineRow> = rows_of(&out)
                .into_iter()
                .map(|r| KlineRow {
                    provider: self.name.clone(),
                    symbol: code.clone(),
                    ts_ns: i64_at(&r, "ts_ns"),
                    open: f64_at(&r, "open"),
                    high: f64_at(&r, "high"),
                    low: f64_at(&r, "low"),
                    close: f64_at(&r, "close"),
                    volume: f64_at(&r, "volume"),
                    turnover: f64_at(&r, "turnover"),
                    adjclose: f64_at(&r, "adjclose"),
                })
                .collect();
            if !rows.is_empty() {
                batches.push(kline_to_batch(&rows));
            }
        }
        Ok(batches)
    }

    fn fetch_ticks(&self, req: &TickReq) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for code in &req.codes {
            let mut args = vec![
                "--action".to_string(),
                "ticks".to_string(),
                "--code".to_string(),
                code.clone(),
            ];
            if req.max > 0 {
                args.push("--max".to_string());
                args.push(req.max.to_string());
            }
            let out = run_bridge(&args)
                .with_context(|| format!("futu ticks `{code}`"))?;
            let rows: Vec<TickRow> = rows_of(&out)
                .into_iter()
                .map(|r| TickRow {
                    provider: self.name.clone(),
                    symbol: code.clone(),
                    ts_ns: i64_at(&r, "ts_ns"),
                    price: f64_at(&r, "price"),
                    volume: f64_at(&r, "volume"),
                    turnover: f64_at(&r, "turnover"),
                    direction: str_at(&r, "direction", "N"),
                    sequence: i64_at(&r, "sequence"),
                })
                .collect();
            if !rows.is_empty() {
                batches.push(tick_to_batch(&rows));
            }
        }
        Ok(batches)
    }
}
