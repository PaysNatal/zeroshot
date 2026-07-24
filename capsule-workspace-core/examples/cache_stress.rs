//! Cross-process stress + safety harness for `CachedBlobStore` sharing ONE cache dir (the node-cache /
//! DaemonSet-hostPath shape: many pods, one node-local cache). The atomics that coordinate sweeping
//! (`sweeping`, `since_sweep` in `cache.rs`) are PER-PROCESS, so N processes on one cache dir have
//! independent sweep state and evict under each other's reads. This binary SELF-ORCHESTRATES so the
//! non-vacuity checks are ASSERTED, not printed: a tighter-than-intended ceiling that makes the cache
//! never-hit fails the run instead of passing silently (the failure mode this whole campaign exists to
//! kill — and one an earlier version of this very harness had).
//!
//! Roles (`--role`):
//!   coordinator (default) — spawns N `worker` child PROCESSES against one shared cache dir, samples peak
//!       cache size, then ASSERTS: (1) every worker byte-identical (correctness), (2) aggregate cache
//!       reads ≥ a floor (the read-under-eviction race was actually exercised), (3) eviction happened
//!       (peak > ceiling), (4) the negative control fires. Exits non-zero if any fails.
//!   worker      — one publisher/materializer loop; writes its result to `--result`.
//!   negctl      — corrupts a cached block and asserts materialize REJECTS it (proves the safety claim
//!       "a wrong cache entry is caught, never served as truth" — the detector works, not just "nothing
//!       bad happened").
//!
//!   cargo run --release --example cache_stress -- --workers 16 --iters 40 --max-bytes 8000000 --tmp /tmp/cs

use capsule_workspace_core::cache::CachedBlobStore;
use capsule_workspace_core::cas::{BlobStore, BlockId, ChunkIndex, LocalBlobStore};
use capsule_workspace_core::daemon::{materialize, publish_pipelined};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1).cloned())
}
fn argu(name: &str, default: u64) -> u64 {
    arg(name).map(|s| s.parse().unwrap()).unwrap_or(default)
}

/// Wraps the durable backing and counts `get_block` calls that reach it — cache MISSES. Used to prove
/// (not assert-away) that reads HIT the shared cache under eviction.
struct CountingStore {
    inner: LocalBlobStore,
    backing_block_gets: Arc<AtomicU64>,
}
impl BlobStore for CountingStore {
    fn put_block(&self, id: &BlockId, b: &[u8]) -> anyhow::Result<()> {
        self.inner.put_block(id, b)
    }
    fn get_block(&self, id: &BlockId) -> anyhow::Result<Vec<u8>> {
        self.backing_block_gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_block(id)
    }
    fn put_manifest(&self, d: &str, b: &[u8]) -> anyhow::Result<()> {
        self.inner.put_manifest(d, b)
    }
    fn get_manifest(&self, d: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get_manifest(d)
    }
    fn has_block(&self, id: &BlockId) -> bool {
        self.inner.has_block(id)
    }
    fn delete_block(&self, id: &BlockId) -> anyhow::Result<bool> {
        self.inner.delete_block(id)
    }
    fn delete_manifest(&self, d: &str) -> anyhow::Result<bool> {
        self.inner.delete_manifest(d)
    }
}

/// Deterministic bytes — no RNG, so a mismatch is a real corruption, not nondeterminism. INCOMPRESSIBLE
/// (hash-derived) so block sizes are predictable and the cache genuinely fills.
fn det(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Shared base (identical across workers → same block hashes → cache-key contention) + a per-worker
/// unique file (distinct blocks → eviction pressure and distinct content to verify).
fn build_tree(root: &Path, id: u64) {
    for i in 0..24u64 {
        let p = root.join(format!("base/f{i}.bin"));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, det(i, 300_000)).unwrap();
    }
    let uniq = root.join(format!("worker{id}/uniq.bin"));
    std::fs::create_dir_all(uniq.parent().unwrap()).unwrap();
    std::fs::write(&uniq, det(0xDEAD_0000 + id, 700_000)).unwrap();
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut m = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.is_file() {
                m.insert(
                    p.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(&p).unwrap(),
                );
            }
        }
    }
    m
}

fn cache_bytes(cache: &Path) -> u64 {
    let mut total = 0u64;
    for sub in ["blocks", "manifests"] {
        if let Ok(rd) = std::fs::read_dir(cache.join(sub)) {
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    if m.is_file() && !e.file_name().to_string_lossy().ends_with(".tmp") {
                        total += m.len();
                    }
                }
            }
        }
    }
    total
}

/// One worker: publish → materialize → assert byte-identical, N times, against the SHARED cache dir.
/// Writes `cache_reads<TAB>backing_misses` to `--result`. Panics (⇒ non-zero exit) on any corruption.
fn run_worker() {
    let durable = PathBuf::from(arg("--durable").unwrap());
    let cache = PathBuf::from(arg("--cache").unwrap());
    let out = PathBuf::from(arg("--out").unwrap());
    let result = PathBuf::from(arg("--result").unwrap());
    let id = argu("--id", 0);
    let iters = argu("--iters", 40);
    let max_bytes = argu("--max-bytes", 8_000_000);

    let tree = out.join(format!("tree{id}"));
    build_tree(&tree, id);
    let want = snapshot(&tree);

    let backing_gets = Arc::new(AtomicU64::new(0));
    let mut blocks_materialized = 0u64;
    for it in 0..iters {
        let backing = CountingStore {
            inner: LocalBlobStore::new(&durable).unwrap(),
            backing_block_gets: backing_gets.clone(),
        };
        let store =
            CachedBlobStore::with_limit(&cache, Box::new(backing), Some(max_bytes)).unwrap();
        // `ChunkIndex::new()` = a COLD dedup index, modelling a fresh pod that just materialized on start.
        // Two consequences to read the peak with: (a) it's realistic for the node-cache shape; (b) a cold
        // index lets `publish_pipelined`'s parallel compressors pack identical chunks into a couple of
        // different block groupings (the manifest digest is still identical — content identity is stable),
        // so the measured cache PEAK conflates per-process-sweep overshoot with this cold-index variant
        // churn. The overshoot number below is therefore an observed peak, NOT pure sweep-coordination
        // overshoot. A pod that CARRIES its index (via `--state`) packs deterministically — one block, no
        // variants — so this churn is a cold-republish / cross-capsule property, not a general one.
        let stats = publish_pipelined(&tree, &store, &ChunkIndex::new(), None, 4, 8, None)
            .unwrap_or_else(|e| panic!("worker {id} iter {it}: publish failed: {e:#}"));
        blocks_materialized += stats.blocks as u64;
        let dst = out.join(format!("mat{id}_{it}"));
        let _ = std::fs::remove_dir_all(&dst);
        materialize(&store, &stats.manifest, &dst)
            .unwrap_or_else(|e| panic!("worker {id} iter {it}: materialize failed: {e:#}"));
        if snapshot(&dst) != want {
            panic!("worker {id} iter {it}: CORRUPTION — materialized tree != published tree");
        }
        let _ = std::fs::remove_dir_all(&dst);
    }
    let misses = backing_gets.load(Ordering::Relaxed);
    let hits = blocks_materialized.saturating_sub(misses);
    std::fs::write(&result, format!("{hits}\t{misses}")).unwrap();
}

/// Corrupt a cached block; materialize MUST reject it (decompress/hash verify), never serve it as truth.
/// This is the negative control: it proves the corruption detector actually fires.
fn run_negctl() {
    let base = PathBuf::from(arg("--tmp").unwrap()).join("negctl");
    let _ = std::fs::remove_dir_all(&base);
    let (durable, cache, tree, out) = (
        base.join("d"),
        base.join("c"),
        base.join("t"),
        base.join("o"),
    );
    build_tree(&tree, 999);
    let store =
        CachedBlobStore::new(&cache, Box::new(LocalBlobStore::new(&durable).unwrap())).unwrap();
    let stats = publish_pipelined(&tree, &store, &ChunkIndex::new(), None, 4, 8, None).unwrap();

    // Corrupt one block in the CACHE tier only (backing stays correct). A read prefers the cache, so the
    // corrupt bytes are what materialize sees — and it must reject them.
    let blocks = cache.join("blocks");
    let victim = std::fs::read_dir(&blocks)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.is_file() && !p.to_string_lossy().ends_with(".tmp"))
        .expect("cache must hold a block to corrupt");
    let mut bytes = std::fs::read(&victim).unwrap();
    for b in bytes.iter_mut().take(4096) {
        *b ^= 0xFF;
    }
    std::fs::write(&victim, &bytes).unwrap();

    let r = materialize(&store, &stats.manifest, &out);
    assert!(
        r.is_err(),
        "NEGATIVE CONTROL FAILED: materialize accepted a corrupted cache block — the detector does not \
         fire, so every 'byte-identical' result above is meaningless"
    );
    println!(
        "negctl: OK (corrupted cache block was rejected: {})",
        r.unwrap_err()
    );
}

/// Spawn N worker processes against a shared cache dir, sample peak, then ASSERT all four properties.
fn run_coordinator() {
    let workers = argu("--workers", 16);
    let iters = argu("--iters", 40);
    let max_bytes = argu("--max-bytes", 8_000_000);
    let tmp = PathBuf::from(arg("--tmp").unwrap_or_else(|| "/tmp/cache_stress".into()));
    let _ = std::fs::remove_dir_all(&tmp);
    let (durable, cache, out) = (tmp.join("durable"), tmp.join("cache"), tmp.join("out"));
    for d in [&durable, &cache, &out] {
        std::fs::create_dir_all(d).unwrap();
    }
    let exe = std::env::current_exe().unwrap();

    // In-process peak sampler (honest LOWER BOUND on overshoot — external sampling can only undercount).
    let stop = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));
    let mon = {
        let (stop, peak, cache) = (stop.clone(), peak.clone(), cache.clone());
        std::thread::spawn(move || {
            while stop.load(Ordering::Relaxed) == 0 {
                peak.fetch_max(cache_bytes(&cache), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };

    let mut children = Vec::new();
    for id in 0..workers {
        let res = tmp.join(format!("res{id}.txt"));
        let child = std::process::Command::new(&exe)
            .args(["--role", "worker", "--id", &id.to_string()])
            .args(["--durable", durable.to_str().unwrap()])
            .args(["--cache", cache.to_str().unwrap()])
            .args(["--out", out.to_str().unwrap()])
            .args(["--result", res.to_str().unwrap()])
            .args(["--iters", &iters.to_string()])
            .args(["--max-bytes", &max_bytes.to_string()])
            .spawn()
            .unwrap();
        children.push((id, child, res));
    }

    let mut ok = 0u64;
    let (mut agg_hits, mut agg_miss) = (0u64, 0u64);
    let mut failed = Vec::new();
    for (id, mut child, res) in children {
        let status = child.wait().unwrap();
        if status.success() {
            ok += 1;
            if let Ok(s) = std::fs::read_to_string(&res) {
                let mut it = s.split('\t');
                agg_hits += it.next().unwrap_or("0").parse().unwrap_or(0);
                agg_miss += it.next().unwrap_or("0").parse().unwrap_or(0);
            }
        } else {
            failed.push(id);
        }
    }
    stop.store(1, Ordering::Relaxed);
    mon.join().ok();
    let peak = peak.load(Ordering::Relaxed);

    println!(
        "coordinator: workers OK={ok}/{workers}  aggregate cache_reads={agg_hits} backing_misses={agg_miss}  \
         peak_cache={}KB vs ceiling={}KB ({:.1}x)",
        peak / 1024,
        max_bytes / 1024,
        peak as f64 / max_bytes as f64
    );

    // (1) correctness
    assert!(
        failed.is_empty(),
        "CORRUPTION / crash in workers {failed:?} — shared cache is NOT safe"
    );
    // (2) NON-VACUITY, asserted not printed: reads must have hit the shared cache, or the whole run
    // proved nothing about read-under-eviction. Floor = workers (each averaged ≥1 hit). A ceiling so
    // tight that nothing is ever cache-hit fails HERE instead of passing green.
    assert!(
        agg_hits >= workers,
        "VACUOUS RUN: aggregate cache_reads={agg_hits} < floor {workers} — the read-under-eviction race \
         was not exercised (ceiling too tight / cache never hit). This run proves nothing; raise --max-bytes."
    );
    // (3) eviction actually happened (otherwise reads never raced a delete)
    assert!(
        peak > max_bytes,
        "NO EVICTION: peak cache {peak} never exceeded the ceiling {max_bytes}, so no read raced a sweep"
    );
    // (4) the corruption detector fires (negative control), so "0 corruption" means something
    let negctl = std::process::Command::new(&exe)
        .args(["--role", "negctl", "--tmp", tmp.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(
        negctl.success(),
        "negative control failed — see negctl output above"
    );

    println!(
        "coordinator: PASS — {} cross-process cycles byte-identical; reads hit the shared cache under \
         eviction (agg {agg_hits}); observed peak overshoot ≥ {:.1}x (per-process sweep + cold-index \
         variant churn — see the worker comment); detector verified.",
        ok * iters,
        peak as f64 / max_bytes as f64
    );
}

fn main() {
    match arg("--role").as_deref() {
        Some("worker") => run_worker(),
        Some("negctl") => run_negctl(),
        _ => run_coordinator(),
    }
}
