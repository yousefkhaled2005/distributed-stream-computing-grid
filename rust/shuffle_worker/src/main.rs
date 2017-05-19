//! High-throughput zero-copy stream partition shuffle worker in Rust.
//! Engineered by Yousef Khaled (2014-2018).

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

static TOTAL_SHUFFLED_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct ShuffleChannel {
    partition_id: u32,
    buffers: Arc<RwLock<Vec<Bytes>>>,
}

#[tokio::main]
async fn main() {
    println!("==================================================================");
    println!("  RUST ZERO-COPY NETWORK SHUFFLE WORKER (2014-2018)               ");
    println!("  Architect: Yousef Khaled <youssef460249@beng.bu.edu.eg>         ");
    println!("  Runtime: Tokio Async I/O | Zero-Copy Memory Pointers            ");
    println!("==================================================================\n");

    let num_partitions = 16;
    let mut channels = HashMap::new();

    for p in 0..num_partitions {
        channels.insert(
            p,
            ShuffleChannel {
                partition_id: p,
                buffers: Arc::new(RwLock::new(Vec::new())),
            },
        );
    }

    println!("[INIT] Initialized {} zero-copy shuffle partitions.", num_partitions);

    // Simulate high-throughput partition routing
    let sample_payload = Bytes::from_static(b"STREAM_BATCH_CHANDY_LAMPORT_RECORD_PAYLOAD_001");
    let payload_len = sample_payload.len() as u64;

    for i in 0..10_000 {
        let target_partition = i % num_partitions;
        let chan = channels.get(&target_partition).unwrap();
        chan.buffers.write().await.push(sample_payload.clone());
        TOTAL_SHUFFLED_BYTES.fetch_add(payload_len, Ordering::Relaxed);
    }

    let bytes = TOTAL_SHUFFLED_BYTES.load(Ordering::Relaxed);
    println!(
        "[SHUFFLE] Successfully routed 10,000 batches ({:.2} MB) with 0 memory copies.",
        bytes as f64 / 1_048_576.0
    );
}
