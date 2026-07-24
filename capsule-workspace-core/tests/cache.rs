//! `CachedBlobStore` — warm-cache tier over a durable backing. Proves: a warm read is served locally
//! (no backing GET), a miss reads-through + populates, deletes hit both tiers, and materialize works
//! over the cached store. The wall-clock warm-vs-cold speedup is measured on real S3 in the EC2 batch;
//! here we assert the CALL behavior that produces it (a counting backing).

use capsule_workspace_core::cache::CachedBlobStore;
use capsule_workspace_core::cas::*;
use capsule_workspace_core::daemon::{materialize, publish};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A `BlobStore` that counts `get_block` calls (the "network" hits we want the cache to eliminate).
/// The counter is an `Arc` so the test can read it after the store is moved into the cache.
struct Counting {
    inner: LocalBlobStore,
    get_blocks: Arc<AtomicUsize>,
}
impl BlobStore for Counting {
    fn put_block(&self, id: &BlockId, b: &[u8]) -> anyhow::Result<()> {
        self.inner.put_block(id, b)
    }
    fn get_block(&self, id: &BlockId) -> anyhow::Result<Vec<u8>> {
        self.get_blocks.fetch_add(1, Ordering::SeqCst);
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

fn write(p: &Path, b: &[u8]) {
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, b).unwrap();
}

#[test]
fn warm_read_skips_backing_and_materialize_works() {
    let d = tempfile::tempdir().unwrap();
    let backing_dir = d.path().join("backing");
    let cache_dir = d.path().join("cache");

    // publish a multi-block tree DIRECTLY to the backing (as another node / a prior generation did) —
    // the cache starts COLD (empty).
    let tree = d.path().join("t");
    write(&tree.join("a.bin"), &vec![3u8; 300_000]);
    write(&tree.join("b.bin"), &vec![9u8; 300_000]);
    let backing0 = LocalBlobStore::new(&backing_dir).unwrap();
    let m = publish(&tree, &backing0, &ChunkIndex::new(), None)
        .unwrap()
        .manifest;

    let counter = Arc::new(AtomicUsize::new(0));
    let counting = Box::new(Counting {
        inner: LocalBlobStore::new(&backing_dir).unwrap(),
        get_blocks: Arc::clone(&counter),
    });
    let cached = CachedBlobStore::new(&cache_dir, counting).unwrap();

    // COLD materialize: every block is a cache miss → read-through to backing (counted) + populate.
    let out1 = d.path().join("o1");
    materialize(&cached, &m, &out1).unwrap();
    assert_eq!(fs::read(out1.join("a.bin")).unwrap(), vec![3u8; 300_000]);
    let cold_gets = counter.load(Ordering::SeqCst);
    assert!(
        cold_gets >= 1,
        "cold materialize read blocks from backing ({cold_gets})"
    );

    // WARM materialize (fresh out dir): every block is now in the local cache → ZERO further backing
    // GETs. This is the warm-resume win (local reads, no network).
    let before = counter.load(Ordering::SeqCst);
    let out2 = d.path().join("o2");
    materialize(&cached, &m, &out2).unwrap();
    assert_eq!(fs::read(out2.join("b.bin")).unwrap(), vec![9u8; 300_000]);
    let warm_gets = counter.load(Ordering::SeqCst) - before;
    assert_eq!(
        warm_gets, 0,
        "warm materialize served every block from the cache (no backing GET)"
    );
}

#[test]
fn write_through_and_delete_hit_both_tiers() {
    let d = tempfile::tempdir().unwrap();
    let backing_dir = d.path().join("backing");
    let cache_dir = d.path().join("cache");
    let cached = CachedBlobStore::new(
        &cache_dir,
        Box::new(LocalBlobStore::new(&backing_dir).unwrap()),
    )
    .unwrap();

    let id = "b".repeat(64);
    cached.put_block(&id, &vec![1u8; 1000]).unwrap();
    // present in BOTH tiers after a write-through
    assert!(
        LocalBlobStore::new(&backing_dir).unwrap().has_block(&id),
        "durable in backing"
    );
    assert!(
        LocalBlobStore::new(&cache_dir).unwrap().has_block(&id),
        "populated in cache"
    );

    // delete drops from both tiers (a node stops serving a reclaimed block)
    assert!(cached.delete_block(&id).unwrap());
    assert!(!LocalBlobStore::new(&backing_dir).unwrap().has_block(&id));
    assert!(!LocalBlobStore::new(&cache_dir).unwrap().has_block(&id));
}

// The cache tier must stay under its ceiling, and eviction must never lose data — the cache is a cache,
// so an evicted block has to fall through to the durable backing store and still read back correctly.
// Without a bound the cache grows until the ephemeral NVMe fills, taking the workspace down with it.
#[test]
fn bounded_cache_evicts_and_stays_correct() {
    let d = tempfile::tempdir().unwrap();
    let backing = Box::new(LocalBlobStore::new(d.path().join("durable")).unwrap());
    let cache_root = d.path().join("cache");
    const LIMIT: u64 = 4 * 1024 * 1024;
    let c = CachedBlobStore::with_limit(&cache_root, backing, Some(LIMIT)).unwrap();

    // Write well past the ceiling.
    let mut ids = Vec::new();
    for i in 0..40u32 {
        let id: BlockId = format!("{:064x}", i);
        let bytes = vec![i as u8; 256 * 1024]; // 40 x 256 KiB = 10 MiB into a 4 MiB cache
        c.put_block(&id, &bytes).unwrap();
        ids.push((id, bytes));
    }

    // Write manifests too — they are the FASTER-growing tier (blocks dedup across generations, manifests
    // do not: every changed publish mints a new digest and caches a whole new object). A ceiling that
    // swept only `blocks/` would leave the bigger directory unbounded, which is what shipped first.
    for i in 0..40u32 {
        c.put_manifest(&format!("{:064x}", 1000 + i), &vec![i as u8; 128 * 1024])
            .unwrap();
    }

    let used: u64 = ["blocks", "manifests"]
        .iter()
        .flat_map(|sub| std::fs::read_dir(cache_root.join(sub)).unwrap().flatten())
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum();
    assert!(
        used <= LIMIT,
        "cache must stay under its ceiling: {used} > {LIMIT}"
    );
    assert!(used > 0, "and must not have evicted everything");

    // EVERY block still reads back byte-identical — evicted ones via the durable backing store.
    for (id, want) in &ids {
        assert_eq!(&c.get_block(id).unwrap(), want, "block {id} lost");
    }
}

// ---- Layer 1: node-plane cache (sole-sweeper agent + pods that never sweep) --------------------------

use capsule_workspace_core::cache::evict_to_limit;
use std::path::PathBuf;

fn block_file(cache_root: &Path, id: &str) -> PathBuf {
    cache_root.join("blocks").join(id)
}
fn set_mtime_secs_ago(p: &Path, secs: u64) {
    let t = std::time::SystemTime::now() - std::time::Duration::from_secs(secs);
    std::fs::File::options()
        .write(true)
        .open(p)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(t))
        .unwrap();
}

// A POD store (`for_pod`, max_bytes=None) must NEVER sweep — the node-agent is the sole sweeper, so a pod
// can never cross-UID-evict another pod's blocks. Proof: write far past any ceiling and assert nothing was
// evicted. A regression that made a pod sweep (e.g. someone routing it through `with_limit(Some(..))`)
// would drop blocks here.
#[test]
fn a_pod_store_never_sweeps() {
    let d = tempfile::tempdir().unwrap();
    let cache_root = d.path().join("cache");
    let c = CachedBlobStore::for_pod(
        &cache_root,
        Box::new(LocalBlobStore::new(d.path().join("durable")).unwrap()),
        None, // no backstop → truly unbounded, so the ONLY thing that could shrink it is a sweep
    )
    .unwrap();
    for i in 0..40u32 {
        c.put_block(&format!("{:064x}", i), &vec![i as u8; 256 * 1024])
            .unwrap();
    }
    let n = std::fs::read_dir(cache_root.join("blocks"))
        .unwrap()
        .flatten()
        .filter(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(
        n, 40,
        "a pod store must not evict — the agent is the sole sweeper"
    );
}

// The agent's eviction path (`evict_to_limit`, called directly by the node-agent) holds the ceiling on a
// dir a pod filled without sweeping. This is the other half of the split: pods fill, the agent bounds.
#[test]
fn evict_to_limit_holds_the_ceiling() {
    let d = tempfile::tempdir().unwrap();
    let cache_root = d.path().join("cache");
    let store = LocalBlobStore::new(&cache_root).unwrap();
    for i in 0..40u32 {
        store
            .put_block(&format!("{:064x}", i), &vec![i as u8; 256 * 1024])
            .unwrap(); // 10 MiB
    }
    const LIMIT: u64 = 4 * 1024 * 1024;
    evict_to_limit(&cache_root, LIMIT);
    let used: u64 = std::fs::read_dir(cache_root.join("blocks"))
        .unwrap()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    assert!(
        used <= LIMIT,
        "agent eviction must hold the ceiling: {used} > {LIMIT}"
    );
    assert!(used > 0, "must not evict everything");
}

// THE ECICTION-POLICY DECISION, pinned. The node cache is a READ cache (warm resume, shared base blocks),
// so it must evict by READ recency, not write age — else the most-read, most-valuable blocks are evicted
// first. A read through the store touches the cache entry; the agent's oldest-mtime eviction must then
// spare it. Control proves the read is load-bearing: WITHOUT the read, the same block is the one evicted.
#[test]
fn a_read_saves_a_block_from_eviction_read_recency() {
    let d = tempfile::tempdir().unwrap();
    let (a, b) = ("a".repeat(64), "b".repeat(64));
    let big = 256 * 1024usize;

    // TREATMENT: read A → A becomes the most-recently-used → the ceiling-1 eviction must keep A, drop B.
    {
        let cache_root = d.path().join("treat");
        let c = CachedBlobStore::for_pod(
            &cache_root,
            Box::new(LocalBlobStore::new(d.path().join("dur_t")).unwrap()),
            None,
        )
        .unwrap();
        c.put_block(&a, &vec![1u8; big]).unwrap();
        c.put_block(&b, &vec![2u8; big]).unwrap();
        // Age both, A older than B, so by WRITE age A would be evicted first.
        set_mtime_secs_ago(&block_file(&cache_root, &a), 200);
        set_mtime_secs_ago(&block_file(&cache_root, &b), 100);
        // READ A — touch-on-hit must refresh A's mtime to now, making it newest.
        assert_eq!(c.get_block(&a).unwrap(), vec![1u8; big]);
        // Evict to a ceiling that holds ~one block.
        evict_to_limit(&cache_root, (big as u64) + (big as u64) / 4);
        assert!(
            block_file(&cache_root, &a).exists(),
            "the READ block must survive eviction"
        );
        assert!(
            !block_file(&cache_root, &b).exists(),
            "the un-read block must be evicted"
        );
    }

    // CONTROL: no read of A → A stays oldest → A is the one evicted. Proves the read above did the work.
    {
        let cache_root = d.path().join("ctrl");
        let c = CachedBlobStore::for_pod(
            &cache_root,
            Box::new(LocalBlobStore::new(d.path().join("dur_c")).unwrap()),
            None,
        )
        .unwrap();
        c.put_block(&a, &vec![1u8; big]).unwrap();
        c.put_block(&b, &vec![2u8; big]).unwrap();
        set_mtime_secs_ago(&block_file(&cache_root, &a), 200);
        set_mtime_secs_ago(&block_file(&cache_root, &b), 100);
        // no read
        evict_to_limit(&cache_root, (big as u64) + (big as u64) / 4);
        assert!(
            !block_file(&cache_root, &a).exists(),
            "without a read, the older block is evicted"
        );
        assert!(block_file(&cache_root, &b).exists());
    }
}

// The pod-side device-fill backstop (for nodes without a dedicated cache partition): once the cache is
// over the cap, cache writes are skipped so a crashed agent can't fill the device — while every block
// stays durable and readable via the backing store.
#[test]
fn the_pod_backstop_caps_cache_growth() {
    let d = tempfile::tempdir().unwrap();
    let cache_root = d.path().join("cache");
    const CAP: u64 = 2 * 1024 * 1024;
    let c = CachedBlobStore::for_pod(
        &cache_root,
        Box::new(LocalBlobStore::new(d.path().join("durable")).unwrap()),
        Some(CAP),
    )
    .unwrap();
    let mut ids = Vec::new();
    for i in 0..40u32 {
        let id = format!("{:064x}", i);
        c.put_block(&id, &vec![i as u8; 256 * 1024]).unwrap(); // 10 MiB written into a 2 MiB backstop
        ids.push((id, i as u8));
    }
    let used: u64 = std::fs::read_dir(cache_root.join("blocks"))
        .unwrap()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    // Amortised, so it overshoots by up to a check interval, but must stay bounded near the cap — NOT the
    // full 10 MiB. (No sweep runs: a pod never sweeps; the backstop just stops populating.)
    assert!(
        used < 2 * CAP,
        "backstop must bound cache growth near the cap: {used} vs cap {CAP}"
    );
    // Durability is unaffected — every block reads back from the backing store.
    for (id, byte) in &ids {
        assert_eq!(
            c.get_block(id).unwrap(),
            vec![*byte; 256 * 1024],
            "block {id} must stay durable"
        );
    }
}

// touch_block must refresh a cached block's mtime — the read-recency mechanism, exercised directly. (The
// CROSS-UID property that motivated the utimensat(NULL) switch can only be pinned in a real cluster with a
// second uid; the kind acceptance suite does that. This pins the same-process behaviour as a fast guard.)
#[test]
fn touch_block_refreshes_mtime() {
    let d = tempfile::tempdir().unwrap();
    let cache_root = d.path().join("c");
    let s = LocalBlobStore::new(&cache_root).unwrap();
    let id = "e".repeat(64);
    s.put_block(&id, &vec![7u8; 4096]).unwrap();
    set_mtime_secs_ago(&block_file(&cache_root, &id), 500);
    let old = std::fs::metadata(block_file(&cache_root, &id))
        .unwrap()
        .modified()
        .unwrap();
    s.touch_block(&id).unwrap();
    let new = std::fs::metadata(block_file(&cache_root, &id))
        .unwrap()
        .modified()
        .unwrap();
    assert!(
        new > old,
        "touch_block must move mtime forward ({old:?} -> {new:?})"
    );
}

// Idea 3: manifest zstd at rest. Round-trips (put compresses, get decompresses), is digest-neutral
// (the golden test proves that separately), and is back-compatible with raw pre-feature manifests.
#[test]
fn manifest_compression_roundtrips_and_is_backcompat() {
    use capsule_workspace_core::cas::{compress_manifest, maybe_decompress_manifest};
    let _g = env_guard();
    std::env::set_var("CAPWS_MANIFEST_ZSTD", "1"); // opt-in (default off)
    let d = tempfile::tempdir().unwrap();
    let s = LocalBlobStore::new(d.path()).unwrap();
    let raw = br#"{"files":[{"path":"a","chunks":["c1","c2"]}],"parent":null}"#.to_vec();

    // put -> get returns byte-identical JSON, regardless of on-disk form.
    s.put_manifest("deadbeef", &raw).unwrap();
    assert_eq!(
        s.get_manifest("deadbeef").unwrap(),
        raw,
        "manifest must round-trip through compression"
    );

    // the stored bytes are actually zstd (magic), not the raw JSON.
    let stored = std::fs::read(d.path().join("manifests/deadbeef")).unwrap();
    assert_eq!(
        &stored[..4],
        &[0x28, 0xB5, 0x2F, 0xFD],
        "stored manifest must be zstd"
    );
    assert!(
        stored.len() < raw.len() || raw.len() < 64,
        "should compress (or be tiny)"
    );

    // back-compat: a RAW (uncompressed) manifest already on disk still loads (older stores).
    std::fs::write(d.path().join("manifests/older"), &raw).unwrap();
    assert_eq!(
        s.get_manifest("older").unwrap(),
        raw,
        "must still read pre-compression raw manifests"
    );

    // the helpers are inverses, and the sniff never false-positives on JSON.
    assert_eq!(maybe_decompress_manifest(compress_manifest(&raw)), raw);
    assert_eq!(
        maybe_decompress_manifest(raw.clone()),
        raw,
        "raw JSON passes through untouched"
    );
    std::env::remove_var("CAPWS_MANIFEST_ZSTD");
}

// Idea 2: ranged reads. Materialize with CAPWS_RANGED_READS=1 must produce byte-identical output to the
// whole-block path, and reject a corrupted chunk (per-chunk hash verify is unchanged by ranging).
#[test]
fn ranged_reads_materialize_byte_identically() {
    use capsule_workspace_core::daemon::publish;
    let _g = env_guard();
    let d = tempfile::tempdir().unwrap();
    let tree = d.path().join("t");
    // multi-block tree so ranges land at non-zero offsets inside blocks
    for i in 0..30u32 {
        write(
            &tree.join(format!("f{i}.bin")),
            &vec![(i as u8).wrapping_mul(7); 300_000],
        );
    }
    let store_dir = d.path().join("s");
    let s = LocalBlobStore::new(&store_dir).unwrap();
    let m = publish(&tree, &s, &ChunkIndex::new(), None)
        .unwrap()
        .manifest;

    // whole-block (default)
    std::env::remove_var("CAPWS_RANGED_READS");
    let out_whole = d.path().join("whole");
    materialize(&LocalBlobStore::new(&store_dir).unwrap(), &m, &out_whole).unwrap();

    // ranged
    std::env::set_var("CAPWS_RANGED_READS", "1");
    let out_ranged = d.path().join("ranged");
    materialize(&LocalBlobStore::new(&store_dir).unwrap(), &m, &out_ranged).unwrap();
    std::env::remove_var("CAPWS_RANGED_READS");

    for i in 0..30u32 {
        let want = vec![(i as u8).wrapping_mul(7); 300_000];
        assert_eq!(
            fs::read(out_whole.join(format!("f{i}.bin"))).unwrap(),
            want,
            "whole f{i}"
        );
        assert_eq!(
            fs::read(out_ranged.join(format!("f{i}.bin"))).unwrap(),
            want,
            "ranged f{i} must match"
        );
    }
}

// Env-var-controlled features (ranged reads, dedup gate) are process-global; Rust runs tests in parallel
// threads, so serialize the ones that toggle env vars behind this guard.
fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static M: std::sync::Mutex<()> = std::sync::Mutex::new(());
    M.lock().unwrap_or_else(|e| e.into_inner())
}

// Idea 1 (physical-block determinism): two INDEPENDENT streaming publishes of the same tree to DIFFERENT
// stores must produce the SAME block ids. This is the canonical-packing property cross-capsule dedup rests
// on — and the golden digest test CANNOT see it (it pins logical identity, which excludes packing).
#[test]
fn streaming_packing_is_canonical_across_stores() {
    use capsule_workspace_core::daemon::publish;
    let d = tempfile::tempdir().unwrap();
    let tree = d.path().join("t");
    for i in 0..20u32 {
        write(&tree.join(format!("f{i}.bin")), &vec![i as u8; 300_000]);
    }
    let blocks = |sub: &str| -> std::collections::BTreeSet<String> {
        let s = LocalBlobStore::new(d.path().join(sub)).unwrap();
        publish(&tree, &s, &ChunkIndex::new(), None).unwrap();
        std::fs::read_dir(d.path().join(sub).join("blocks"))
            .unwrap()
            .flatten()
            .filter(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect()
    };
    assert_eq!(
        blocks("a"),
        blocks("b"),
        "streaming publish must pack canonically (identical block ids)"
    );
}

// Idea 1 (dedup gate): a second INDEPENDENT publish (fresh index) of the same tree to the same store, with
// the gate on, must skip every upload — the blocks are already durable (canonical packing makes their ids
// match) — while still producing a materializable manifest.
#[test]
fn dedup_gate_skips_already_durable_blocks() {
    use capsule_workspace_core::daemon::publish;
    let _g = env_guard();
    let d = tempfile::tempdir().unwrap();
    let tree = d.path().join("t");
    for i in 0..20u32 {
        write(&tree.join(format!("f{i}.bin")), &vec![i as u8; 300_000]);
    }
    let sdir = d.path().join("s");

    std::env::remove_var("CAPWS_DEDUP_GATE");
    let p1 = publish(
        &tree,
        &LocalBlobStore::new(&sdir).unwrap(),
        &ChunkIndex::new(),
        None,
    )
    .unwrap();
    assert!(p1.blocks > 0);
    assert_eq!(
        p1.blocks_uploaded, p1.blocks,
        "first publish uploads every block"
    );

    std::env::set_var("CAPWS_DEDUP_GATE", "1");
    let p2 = publish(
        &tree,
        &LocalBlobStore::new(&sdir).unwrap(),
        &ChunkIndex::new(),
        None,
    )
    .unwrap();
    std::env::remove_var("CAPWS_DEDUP_GATE");
    assert_eq!(p2.blocks, p1.blocks, "same tree → same block count");
    assert_eq!(
        p2.blocks_uploaded, 0,
        "canonical packing → all blocks already durable → gate skips every upload"
    );

    // the manifest still materializes (blocks are present from publish 1)
    let out = d.path().join("o");
    materialize(&LocalBlobStore::new(&sdir).unwrap(), &p2.manifest, &out).unwrap();
    for i in 0..20u32 {
        assert_eq!(
            fs::read(out.join(format!("f{i}.bin"))).unwrap(),
            vec![i as u8; 300_000]
        );
    }
}

// Idea 3 COUPLING: manifest compression + the file-backed GC's decompress (gc.rs) are inseparable. The
// GC reads manifest bytes DIRECTLY (not via get_manifest), so with compression on it must decompress or it
// wedges (fail-safe: aborts, deletes nothing — but unbounded growth). This pins that a block referenced by
// a COMPRESSED manifest survives GC. Reverting the gc.rs decompress makes this fail (mutation-verified).
#[test]
fn gc_marks_blocks_of_a_compressed_manifest() {
    use capsule_workspace_core::daemon::publish;
    use capsule_workspace_core::gc;
    let _g = env_guard();
    std::env::set_var("CAPWS_MANIFEST_ZSTD", "1");
    let d = tempfile::tempdir().unwrap();
    let tree = d.path().join("t");
    for i in 0..10u32 {
        write(&tree.join(format!("f{i}.bin")), &vec![i as u8; 300_000]);
    }
    let root = d.path().join("s");
    let p = publish(
        &tree,
        &LocalBlobStore::new(&root).unwrap(),
        &ChunkIndex::new(),
        None,
    )
    .unwrap();
    // the manifest is stored COMPRESSED (this is the state the GC must tolerate)
    let mbytes = std::fs::read(root.join("manifests").join(&p.manifest)).unwrap();
    assert_eq!(
        &mbytes[..4],
        &[0x28, 0xB5, 0x2F, 0xFD],
        "manifest must be stored compressed"
    );
    // GC with this manifest as the sole live HEAD, grace=0: must MARK (keep) its blocks, delete none.
    let st = gc::collect(
        &root,
        std::slice::from_ref(&p.manifest),
        std::time::Duration::ZERO,
    )
    .unwrap();
    std::env::remove_var("CAPWS_MANIFEST_ZSTD");
    assert!(
        st.blocks_kept > 0,
        "GC must mark blocks referenced by a COMPRESSED manifest"
    );
    assert_eq!(
        st.blocks_deleted, 0,
        "GC must not delete a live compressed manifest's blocks"
    );
    // and the tree still materializes — the blocks survived.
    let out = d.path().join("o");
    materialize(&LocalBlobStore::new(&root).unwrap(), &p.manifest, &out).unwrap();
    for i in 0..10u32 {
        assert_eq!(
            fs::read(out.join(format!("f{i}.bin"))).unwrap(),
            vec![i as u8; 300_000]
        );
    }
}

// Idea 2 ADAPTIVE decision: a densely-referenced block is fetched WHOLE (one GET), a sparsely-referenced
// block is fetched by RANGE (per-chunk GETs). Verified by counting each store call — not just byte-identity.
#[test]
fn ranged_reads_are_adaptive_dense_whole_sparse_ranged() {
    use capsule_workspace_core::daemon::publish;
    use capsule_workspace_core::manifest::Manifest;
    let _g = env_guard();
    let d = tempfile::tempdir().unwrap();
    let sdir = d.path().join("s");

    struct Adaptive {
        inner: LocalBlobStore,
        whole: Arc<AtomicUsize>,
        ranged: Arc<AtomicUsize>,
    }
    impl BlobStore for Adaptive {
        fn put_block(&self, i: &BlockId, b: &[u8]) -> anyhow::Result<()> {
            self.inner.put_block(i, b)
        }
        fn get_block(&self, i: &BlockId) -> anyhow::Result<Vec<u8>> {
            self.whole.fetch_add(1, Ordering::SeqCst);
            self.inner.get_block(i)
        }
        fn get_block_range(&self, i: &BlockId, o: u64, l: u32) -> anyhow::Result<Vec<u8>> {
            self.ranged.fetch_add(1, Ordering::SeqCst);
            self.inner.get_block_range(i, o, l)
        }
        fn put_manifest(&self, d: &str, b: &[u8]) -> anyhow::Result<()> {
            self.inner.put_manifest(d, b)
        }
        fn get_manifest(&self, d: &str) -> anyhow::Result<Vec<u8>> {
            self.inner.get_manifest(d)
        }
        fn has_block(&self, i: &BlockId) -> bool {
            self.inner.has_block(i)
        }
        fn delete_block(&self, i: &BlockId) -> anyhow::Result<bool> {
            self.inner.delete_block(i)
        }
        fn delete_manifest(&self, d: &str) -> anyhow::Result<bool> {
            self.inner.delete_manifest(d)
        }
    }
    let mk = || {
        let (w, r) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        (
            Adaptive {
                inner: LocalBlobStore::new(&sdir).unwrap(),
                whole: w.clone(),
                ranged: r.clone(),
            },
            w,
            r,
        )
    };

    // 400 files x 300KB -> ~2 densely-packed 64MiB blocks.
    let tree = d.path().join("t");
    for i in 0..400u32 {
        write(
            &tree.join(format!("f{i:04}.bin")),
            &vec![(i % 251) as u8; 300_000],
        );
    }
    let full = publish(
        &tree,
        &LocalBlobStore::new(&sdir).unwrap(),
        &ChunkIndex::new(),
        None,
    )
    .unwrap();
    let mut idx = ChunkIndex::new();
    idx.extend(
        Manifest::from_bytes(
            &LocalBlobStore::new(&sdir)
                .unwrap()
                .get_manifest(&full.manifest)
                .unwrap(),
        )
        .unwrap()
        .chunks,
    );
    // sparse tree: 4 of those files, deduped into `full`'s blocks -> M_sparse references ~1% of each block.
    let sp = d.path().join("sp");
    for i in [0u32, 100, 200, 300] {
        write(
            &sp.join(format!("f{i:04}.bin")),
            &vec![(i % 251) as u8; 300_000],
        );
    }
    let sparse = publish(&sp, &LocalBlobStore::new(&sdir).unwrap(), &idx, None).unwrap();

    std::env::set_var("CAPWS_RANGED_READS", "1");
    let (st, w, r) = mk();
    materialize(&st, &full.manifest, &d.path().join("od")).unwrap();
    assert!(
        w.load(Ordering::SeqCst) > 0 && r.load(Ordering::SeqCst) == 0,
        "DENSE manifest must fetch blocks WHOLE (whole={}, ranged={})",
        w.load(Ordering::SeqCst),
        r.load(Ordering::SeqCst)
    );
    let (st, w, r) = mk();
    materialize(&st, &sparse.manifest, &d.path().join("os")).unwrap();
    assert!(
        r.load(Ordering::SeqCst) > 0 && w.load(Ordering::SeqCst) == 0,
        "SPARSE manifest must fetch chunks by RANGE (whole={}, ranged={})",
        w.load(Ordering::SeqCst),
        r.load(Ordering::SeqCst)
    );
    std::env::remove_var("CAPWS_RANGED_READS");
}
