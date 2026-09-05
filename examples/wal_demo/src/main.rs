use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const WAL_PATH: &str = "data/partition=demo/wal.log";
const META_PATH: &str = "data/partition=demo/meta.json";
const BLOCK_DIR: &str = "data/partition=demo/blocks";

#[derive(Serialize, Deserialize, Debug)]
enum Op {
    Insert { id: u64, ts: u64, price: f64 },
    Delete { id: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
struct WalRecord {
    txn_id: u64,
    op: Op,
}

#[derive(Debug, Default)]
struct DeltaBuffer {
    // simple column-store: vectors for each column
    ids: Vec<u64>,
    tss: Vec<u64>,
    prices: Vec<f64>,
}

impl DeltaBuffer {
    fn apply(&mut self, rec: &WalRecord) {
        match &rec.op {
            Op::Insert { id, ts, price } => {
                self.ids.push(*id);
                self.tss.push(*ts);
                self.prices.push(*price);
            }
            Op::Delete { id: _ } => {
                // naive: no physical delete in delta; production would mark tombstone
            }
        }
    }

    fn row_count(&self) -> usize {
        self.ids.len()
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.tss.clear();
        self.prices.clear();
    }
}

fn ensure_dirs() -> std::io::Result<()> {
    std::fs::create_dir_all(BLOCK_DIR)?;
    Ok(())
}

fn append_wal(record: &WalRecord) -> std::io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WAL_PATH)?;
    let bytes = bincode::serialize(record).unwrap();
    // write length prefix then payload (simple framing)
    let len = bytes.len() as u32;
    f.write_all(&len.to_le_bytes())?;
    f.write_all(&bytes)?;
    f.sync_all()?; // configurable in real system
    Ok(())
}

fn replay_wal(delta: &mut DeltaBuffer) -> std::io::Result<()> {
    if !Path::new(WAL_PATH).exists() {
        return Ok(());
    }
    let mut f = File::open(WAL_PATH)?;
    loop {
        let mut len_buf = [0u8; 4];
        match f.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(_) => break, // EOF
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        f.read_exact(&mut payload)?;
        let rec: WalRecord = bincode::deserialize(&payload).unwrap();
        delta.apply(&rec);
    }
    Ok(())
}

fn flush_delta(delta: &mut DeltaBuffer) -> std::io::Result<()> {
    // create a simple block file: write columns as binary arrays with length prefix
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let block_name = format!("{}/block_{}.col", BLOCK_DIR, ts);
    let mut f = File::create(&block_name)?;

    // write ids
    f.write_all(&(delta.ids.len() as u64).to_le_bytes())?;
    for v in &delta.ids {
        f.write_all(&v.to_le_bytes())?;
    }
    // write tss
    f.write_all(&(delta.tss.len() as u64).to_le_bytes())?;
    for v in &delta.tss {
        f.write_all(&v.to_le_bytes())?;
    }
    // write prices
    f.write_all(&(delta.prices.len() as u64).to_le_bytes())?;
    for v in &delta.prices {
        f.write_all(&v.to_le_bytes())?;
    }
    f.sync_all()?;

    // update meta.json (very simplified)
    let meta = serde_json::json!({
        "blocks": [ { "file": block_name, "row_count": delta.row_count() } ],
        "wal_files": ["wal.log"],
        "version": 1u64,
    });
    let tmp_meta = format!("{}.tmp", META_PATH);
    let mut mf = File::create(&tmp_meta)?;
    mf.write_all(serde_json::to_string_pretty(&meta).unwrap().as_bytes())?;
    mf.sync_all()?;
    std::fs::rename(tmp_meta, META_PATH)?;

    // truncate wal after flush (simple strategy: remove file)
    std::fs::remove_file(WAL_PATH).ok();

    delta.clear();
    Ok(())
}

fn demo_workflow() -> std::io::Result<()> {
    ensure_dirs()?;
    let mut delta = DeltaBuffer::default();

    // replay existing WAL on startup
    replay_wal(&mut delta)?;
    println!("After replay, delta rows = {}", delta.row_count());

    // simulate incoming writes
    for i in 0..10u64 {
        let rec = WalRecord {
            txn_id: i,
            op: Op::Insert { id: i + 1000, ts: 1_700_000_000 + i, price: 10.0 + (i as f64) },
        };
        append_wal(&rec)?; // durable append
        delta.apply(&rec); // apply to in‑memory delta
    }
    println!("Before flush, delta rows = {}", delta.row_count());

    // flush when delta grows beyond threshold (here we just flush)
    flush_delta(&mut delta)?;
    println!("Flushed delta to block and updated meta.json");

    Ok(())
}

fn main() {
    if let Err(e) = demo_workflow() {
        eprintln!("Error: {}", e);
    }
}
