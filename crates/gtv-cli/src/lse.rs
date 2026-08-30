//! London Strategic Edge live WebSocket feed.
//!
//! Protocol (`wss://ws.londonstrategicedge.com`, JSON):
//!   client -> {"action":"auth","api_key":...}
//!             {"action":"subscribe","symbol":...}
//!             {"action":"ping"}                        (heartbeat)
//!   server -> {"type":"welcome","max_symbols":N}
//!             {"type":"auth","status":"ok","l3_access":bool}
//!             {"type":"subscribed","symbol":...,"count":N}
//!             {"type":"tick","symbol","price","bid","ask","ts"}
//!             {"type":"error","code","message"}

use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

/// One quote tick: symbol, last price, best bid, best ask, event time.
#[derive(Debug, Clone)]
pub struct Tick {
    pub symbol: String,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub ts: i64,
}

pub const WSS_URL: &str = "wss://ws.londonstrategicedge.com";

/// Normalize a symbol to LSE's wire format (mirrors the site's `mn` fn):
/// metals `XAUUSD` -> `XAU/USD`, crypto `BTCUSD` -> `BTC/USD`, forex
/// `EURUSD` -> `EUR/USD`, everything else (stocks/indices) uppercased as-is.
pub fn normalize_symbol(s: &str) -> String {
    let t = s.trim().replace('/', "").to_uppercase();
    const METALS: &[&str] = &["XAU", "XAG", "XPT", "XPD"];
    const FOREX: &[&str] = &["EUR", "USD", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"];
    const CRYPTO: &[&str] = &[
        "BTC", "ETH", "LTC", "XRP", "SOL", "DOGE", "ADA", "DOT", "AVAX", "MATIC",
        "LINK", "UNI", "ATOM", "XLM", "ALGO", "FIL", "NEAR", "AAVE", "MKR", "COMP",
        "SNX", "YFI", "SUSHI", "CRV", "BAL", "INJ", "SUI", "SEI", "APT", "ARB", "OP",
    ];
    for m in METALS {
        if t.starts_with(m) && t.len() > m.len() {
            return format!("{m}/{}", &t[m.len()..]);
        }
    }
    for c in CRYPTO {
        if t.starts_with(c) && t.len() > c.len() {
            let rest = &t[c.len()..];
            if rest == "USD" || rest == "USDT" || rest == "BUSD" {
                return format!("{c}/{rest}");
            }
        }
    }
    if t.len() == 6 {
        let (a, b) = (&t[..3], &t[3..]);
        if FOREX.contains(&a) && FOREX.contains(&b) {
            return format!("{a}/{b}");
        }
    }
    t
}

/// Live-tick table schema.
pub fn live_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// An empty single-batch table with the live schema (registered up front so the
/// table exists before the first tick arrives).
pub fn empty_batch() -> RecordBatch {
    RecordBatch::try_new(
        live_schema(),
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        ],
    )
    .expect("empty live batch")
}

/// Convert a slice of ticks into a single [`RecordBatch`].
pub fn ticks_to_batch(ticks: &[Tick]) -> RecordBatch {
    let sym: Vec<String> = ticks.iter().map(|t| t.symbol.clone()).collect();
    let price: Vec<f64> = ticks.iter().map(|t| t.price).collect();
    let bid: Vec<f64> = ticks.iter().map(|t| t.bid).collect();
    let ask: Vec<f64> = ticks.iter().map(|t| t.ask).collect();
    let ts: Vec<i64> = ticks.iter().map(|t| t.ts).collect();
    RecordBatch::try_new(
        live_schema(),
        vec![
            Arc::new(StringArray::from(sym)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
            Arc::new(Float64Array::from(bid)) as ArrayRef,
            Arc::new(Float64Array::from(ask)) as ArrayRef,
            Arc::new(Int64Array::from(ts)) as ArrayRef,
        ],
    )
    .expect("build tick batch")
}

/// Connect, authenticate, subscribe and stream ticks into the shared buffer.
pub async fn run_feed(
    api_key: &str,
    symbols: &[String],
    ticks: Arc<Mutex<Vec<Tick>>>,
) -> Result<()> {
    let (mut ws, _) = connect_async(WSS_URL).await?;
    ws.send(Message::Text(
        format!(r#"{{"action":"auth","api_key":"{api_key}"}}"#).into(),
    ))
    .await?;
    for s in symbols {
        let sym = normalize_symbol(s);
        ws.send(Message::Text(
            format!(r#"{{"action":"subscribe","symbol":"{sym}"}}"#).into(),
        ))
        .await?;
    }

    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let v: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            match v.get("type").and_then(|x| x.as_str()).unwrap_or("") {
                "welcome" => eprintln!(
                    "lse: connected (max_symbols={})",
                    v.get("max_symbols").and_then(|x| x.as_i64()).unwrap_or(0)
                ),
                "auth" => {
                    if v.get("status").and_then(|x| x.as_str()) == Some("ok") {
                        eprintln!(
                            "lse: auth ok (l3_access={})",
                            v.get("l3_access").and_then(|x| x.as_bool()).unwrap_or(false)
                        );
                    } else {
                        anyhow::bail!(
                            "lse auth rejected: {}",
                            v.get("message").and_then(|x| x.as_str()).unwrap_or("unknown")
                        );
                    }
                }
                "subscribed" => eprintln!(
                    "lse: subscribed {} (count={})",
                    v.get("symbol").and_then(|x| x.as_str()).unwrap_or(""),
                    v.get("count").and_then(|x| x.as_i64()).unwrap_or(0)
                ),
                "tick" => {
                    let tick = Tick {
                        symbol: v.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                        price: v.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0),
                        bid: v.get("bid").and_then(|x| x.as_f64()).unwrap_or(0.0),
                        ask: v.get("ask").and_then(|x| x.as_f64()).unwrap_or(0.0),
                        ts: v.get("ts").and_then(|x| x.as_i64()).unwrap_or(0),
                    };
                    if let Ok(mut g) = ticks.lock() {
                        g.push(tick);
                    }
                }
                "error" => eprintln!(
                    "lse error: {} — {}",
                    v.get("code").and_then(|x| x.as_str()).unwrap_or(""),
                    v.get("message").and_then(|x| x.as_str()).unwrap_or("")
                ),
                _ => {}
            }
        }
    }
    Ok(())
}
