//! `CachedBlobStore` — a two-tier `BlobStore`: a node-local `cache` (NVMe `LocalBlobStore`) in front
//! of a durable `backing` store (S3). This is the "node-local NVMe cache" of the design: reads prefer
//! the cache (local, fast — no network), and a miss falls back to the backing store and POPULATES the
//! cache so the next read is warm. It is the mechanism behind fast WARM resume (a node that recently
//! published/materialized this lineage already holds the blocks locally, so materialize does local
//! reads instead of S3 GETs).
//!
//! LOAD-BEARING INVARIANT: the cache is NEVER authoritative. Every write goes to the durable backing
//! FIRST, then best-effort to the cache; a cache write failure never fails the operation. Reads that
//! miss the cache are served from backing. Because blocks/manifests are CONTENT-ADDRESSED, a stale
//! cache entry (e.g. a block a central GC deleted from the backing) is byte-identical to what it
//! keys, so serving it is harmless; and a block genuinely gone from backing (and absent from cache)
//! correctly surfaces `NotFound`. Deletes hit BOTH tiers so a node doesn't keep serving a block the
//! authority reclaimed.

use crate::cas::{BlobStore, BlockId, LocalBlobStore, StoreError};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct CachedBlobStore {
    cache: LocalBlobStore,
    cache_root: PathBuf,
    backing: Box<dyn BlobStore>,
    /// Byte ceiling for the cache tier. `None` = this store NEVER sweeps. In the shared-node-cache
    /// deployment pods use [`CachedBlobStore::for_pod`] (`None`) and the node-agent is the sole sweeper;
    /// see that constructor and [`evict_to_limit`].
    max_bytes: Option<u64>,
    /// Device-fill fallback for a POD on a node with no dedicated cache partition: once the cache exceeds
    /// this, cache WRITES are skipped (durable writes are not), so a crashed agent can't fill the device.
    /// `None` when a dedicated partition provides that guarantee for free (best-effort writes swallow
    /// ENOSPC). Checked amortised, never on every write.
    backstop: Option<u64>,
    /// True once the amortised backstop check has found the cache over `backstop`; flips back when a later
    /// check finds it under. Lets the hot path skip the cache write with a single atomic load, no readdir.
    over_backstop: AtomicBool,
    /// Bytes written since the last sweep/backstop-check — doing either on every write would be O(cache).
    since_sweep: AtomicU64,
    /// One sweeper at a time; a concurrent caller just skips (the next write re-triggers).
    sweeping: AtomicBool,
}

/// Evict oldest-by-mtime files from `cache_root` until it is back under ~90% of `max_bytes`. Enumerates
/// BOTH tiers (blocks + manifests) and skips other writers' in-flight `.tmp` files. This is a free
/// function so the **node-agent** — the SOLE sweeper in the shared-node-cache deployment — calls it
/// directly in a loop (it owns the dir, so its `remove_file` over any pod's file is permitted), while
/// [`CachedBlobStore::maybe_sweep`] calls it for the single-process case. Oldest-by-mtime is a
/// read-recency LRU **only because** [`CachedBlobStore::get_block`]/`get_manifest` touch a cache entry's
/// mtime on a hit (see there); without that touch this would evict by write age, which for a read cache
/// evicts the most-read blocks first.
pub fn evict_to_limit(cache_root: &Path, max_bytes: u64) {
    let mut entries: Vec<(std::time::SystemTime, u64, PathBuf)> = Vec::new();
    let mut total: u64 = 0;
    for sub in ["blocks", "manifests"] {
        if let Ok(rd) = std::fs::read_dir(cache_root.join(sub)) {
            for e in rd.flatten() {
                // Skip other writers' in-flight `.tmp` staging files: deleting one breaks its rename,
                // and counting it inflates the total.
                if e.file_name().to_string_lossy().ends_with(".tmp") {
                    continue;
                }
                if let Ok(m) = e.metadata() {
                    if m.is_file() {
                        total += m.len();
                        entries.push((
                            m.modified().unwrap_or(std::time::UNIX_EPOCH),
                            m.len(),
                            e.path(),
                        ));
                    }
                }
            }
        }
    }
    if total > max_bytes {
        entries.sort_by_key(|(t, _, _)| *t); // oldest first
        let target = max_bytes - max_bytes / 10;
        for (_, len, path) in entries {
            if total <= target {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(len);
            }
        }
    }
}

/// Current on-disk size of the cache (both tiers, skipping `.tmp`). O(cache dir); callers amortise.
fn cache_size(cache_root: &Path) -> u64 {
    let mut total = 0u64;
    for sub in ["blocks", "manifests"] {
        if let Ok(rd) = std::fs::read_dir(cache_root.join(sub)) {
            for e in rd.flatten() {
                if e.file_name().to_string_lossy().ends_with(".tmp") {
                    continue;
                }
                if let Ok(m) = e.metadata() {
                    if m.is_file() {
                        total += m.len();
                    }
                }
            }
        }
    }
    total
}

fn is_not_found(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<StoreError>(),
        Some(StoreError::NotFound(_))
    )
}

impl CachedBlobStore {
    /// `cache_root` is the node-local NVMe dir (fast, disposable); `backing` is the durable authority.
    pub fn new(cache_root: impl AsRef<Path>, backing: Box<dyn BlobStore>) -> Result<Self> {
        Self::with_limit(cache_root, backing, None)
    }

    /// Same, with a byte ceiling on the cache tier.
    ///
    /// WHY THIS EXISTS: without a bound the cache grows until the device fills — and that device is the
    /// same ephemeral NVMe that holds `--ref-dir` and, typically, the agent's workspace. Cache writes are
    /// best-effort so they fail silently; the workspace writes that fail alongside them do NOT. An
    /// unbounded cache therefore converts "this node has been busy for a while" into a workspace-write
    /// outage, which is the same failure class as the unbounded materialize buffer.
    pub fn with_limit(
        cache_root: impl AsRef<Path>,
        backing: Box<dyn BlobStore>,
        max_bytes: Option<u64>,
    ) -> Result<Self> {
        Self::build(cache_root.as_ref(), backing, max_bytes, None)
    }

    /// Shared constructor enforcing the one invariant the two public ones must not break: a store sets
    /// AT MOST ONE of `max_bytes` (it sweeps) or `backstop` (it skips writes over a cap) — never both,
    /// because they share `since_sweep` as their amortisation counter and would fight over it. Impossible
    /// today (`with_limit` ⇒ backstop=None, `for_pod` ⇒ max_bytes=None), but a future third constructor
    /// routed through here can't reintroduce it silently.
    fn build(
        cache_root: &Path,
        backing: Box<dyn BlobStore>,
        max_bytes: Option<u64>,
        backstop: Option<u64>,
    ) -> Result<Self> {
        debug_assert!(
            !(max_bytes.is_some() && backstop.is_some()),
            "max_bytes and backstop share since_sweep; a CachedBlobStore must not set both"
        );
        Ok(Self {
            cache: LocalBlobStore::new(cache_root)?,
            cache_root: cache_root.to_path_buf(),
            backing,
            max_bytes,
            backstop,
            over_backstop: AtomicBool::new(false),
            since_sweep: AtomicU64::new(0),
            sweeping: AtomicBool::new(false),
        })
    }

    /// The ONLY cache entry point for a POD in the shared-node-cache deployment. It NEVER sweeps — the
    /// node-agent is the sole sweeper, so a pod can never cross-UID-evict another pod's blocks (which it
    /// mostly can't anyway: 0644 files + a dir it doesn't own). Making this the sole pod constructor means
    /// "pod never sweeps" is enforced by the type, not by remembering to pass `max_bytes = None` — a pod
    /// that called `with_limit(Some(x))` would silently reintroduce the cross-UID eviction this design
    /// removes.
    ///
    /// `backstop` is a device-fill fallback ONLY for nodes without a dedicated cache partition; prefer the
    /// partition (a full partition makes best-effort cache writes ENOSPC-and-ignore, self-limiting with no
    /// accounting and physically isolating the cache from the workspace). When set, cache writes are
    /// skipped once the cache exceeds it — checked amortised (once per `backstop/8` written), never per
    /// write, so the sole-sweeper's whole point (no readdir on the hot path) is preserved.
    pub fn for_pod(
        cache_root: impl AsRef<Path>,
        backing: Box<dyn BlobStore>,
        backstop: Option<u64>,
    ) -> Result<Self> {
        Self::build(cache_root.as_ref(), backing, None, backstop) // max_bytes=None ⇒ pods NEVER sweep
    }

    /// Evict oldest-by-mtime until the cache is back under ~90% of the ceiling. Safe at any moment: the
    /// cache is never authoritative, so a miss simply falls through to the durable backing store.
    /// Amortised — only runs once an eighth of the ceiling has been written since the last sweep.
    fn maybe_sweep(&self, wrote: u64) {
        let Some(max) = self.max_bytes else { return };
        if self.since_sweep.fetch_add(wrote, Ordering::Relaxed) + wrote < max / 8 {
            return;
        }
        if self.sweeping.swap(true, Ordering::SeqCst) {
            return; // another thread is already sweeping
        }
        // Clear the flag even on an unwind: a latched `sweeping` would silently disable eviction for the
        // rest of the process's life, i.e. quietly restore the unbounded behaviour this exists to prevent.
        struct Unlatch<'a>(&'a AtomicBool);
        impl Drop for Unlatch<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _unlatch = Unlatch(&self.sweeping);
        self.since_sweep.store(0, Ordering::Relaxed);
        evict_to_limit(&self.cache_root, max);
    }

    /// Amortised device-fill backstop for a pod with no dedicated cache partition. Returns whether a cache
    /// write should be SKIPPED (the durable write always proceeds). O(cache) only once per `backstop/8`
    /// written; between checks it answers from a cached atomic, so the sole-sweeper's no-readdir-on-hot-
    /// path property holds. No-op (never skips) when `backstop` is `None`.
    fn backstop_skip(&self, wrote: u64) -> bool {
        let Some(cap) = self.backstop else {
            return false;
        };
        if self.since_sweep.fetch_add(wrote, Ordering::Relaxed) + wrote >= cap / 8 {
            self.since_sweep.store(0, Ordering::Relaxed);
            self.over_backstop
                .store(cache_size(&self.cache_root) > cap, Ordering::Relaxed);
        }
        self.over_backstop.load(Ordering::Relaxed)
    }

    /// Read-through: cache hit → local, and TOUCH it (read-recency LRU — see `touch_cache`); miss →
    /// backing, then populate the cache best-effort unless the device-fill backstop says skip.
    fn get_through(
        &self,
        from_cache: impl Fn(&LocalBlobStore) -> Result<Vec<u8>>,
        touch_cache: impl Fn(&LocalBlobStore),
        from_backing: impl Fn(&dyn BlobStore) -> Result<Vec<u8>>,
        to_cache: impl Fn(&LocalBlobStore, &[u8]) -> Result<()>,
    ) -> Result<Vec<u8>> {
        match from_cache(&self.cache) {
            Ok(b) => {
                // A HIT marks the entry recent so the agent's oldest-mtime eviction doesn't drop the
                // most-read blocks first. Best-effort: a cross-UID touch failure just leaves this entry on
                // its write-age clock, never wrong data.
                touch_cache(&self.cache);
                Ok(b)
            }
            Err(e) if is_not_found(&e) => {
                let b = from_backing(self.backing.as_ref())?;
                if !self.backstop_skip(b.len() as u64) {
                    let _ = to_cache(&self.cache, &b); // best-effort; a cache write must not fail the read
                }
                self.maybe_sweep(b.len() as u64);
                Ok(b)
            }
            Err(e) => Err(e),
        }
    }
}

impl BlobStore for CachedBlobStore {
    fn put_block(&self, id: &BlockId, bytes: &[u8]) -> Result<()> {
        self.backing.put_block(id, bytes)?; // durable authority FIRST
        if !self.backstop_skip(bytes.len() as u64) {
            let _ = self.cache.put_block(id, bytes); // populate cache best-effort
        }
        self.maybe_sweep(bytes.len() as u64);
        Ok(())
    }
    fn get_block(&self, id: &BlockId) -> Result<Vec<u8>> {
        self.get_through(
            |c| c.get_block(id),
            |c| {
                let _ = c.touch_block(id);
            },
            |b| b.get_block(id),
            |c, bytes| c.put_block(id, bytes),
        )
    }
    fn put_manifest(&self, digest: &str, bytes: &[u8]) -> Result<()> {
        self.backing.put_manifest(digest, bytes)?;
        if !self.backstop_skip(bytes.len() as u64) {
            let _ = self.cache.put_manifest(digest, bytes);
        }
        self.maybe_sweep(bytes.len() as u64);
        Ok(())
    }
    fn get_manifest(&self, digest: &str) -> Result<Vec<u8>> {
        self.get_through(
            |c| c.get_manifest(digest),
            |c| {
                let _ = c.touch_manifest(digest);
            },
            |b| b.get_manifest(digest),
            |c, bytes| c.put_manifest(digest, bytes),
        )
    }
    fn has_block(&self, id: &BlockId) -> bool {
        self.cache.has_block(id) || self.backing.has_block(id)
    }
    fn delete_block(&self, id: &BlockId) -> Result<bool> {
        // authority first; also drop from the cache so a node stops serving a reclaimed block.
        let removed = self.backing.delete_block(id)?;
        let _ = self.cache.delete_block(id);
        Ok(removed)
    }
    fn delete_manifest(&self, digest: &str) -> Result<bool> {
        let removed = self.backing.delete_manifest(digest)?;
        let _ = self.cache.delete_manifest(digest);
        Ok(removed)
    }
}
