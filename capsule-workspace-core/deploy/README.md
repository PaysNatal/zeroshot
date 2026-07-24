# Node-plane cache — kind acceptance harness (Layer 1)

Validates the shared node-local cache design in a **real multi-node, multi-uid k8s cluster** — the parts
no unit test can cover: cross-uid access, per-node cache scoping, and the cross-uid read-recency touch.

## What it proves

Run `./accept.sh` against a running cluster (below). Four hard assertions, exits non-zero on any fail:

1. **Cross-uid read HIT** — pod A (uid 1000) writes a block; pod B (uid 2000) reads it on the same node and
   gets a cache HIT with correct bytes. Requires the setgid-2775 dir + shared gid 3000 + `supplementalGroups`
   (pods literally can't write the agent-owned dir otherwise).
2. **Per-node scoping** — the same block on the _other_ worker MISSes and falls through to shared S3. Proves
   the cache is genuinely per-node — passes only on a ≥2-worker cluster (would false-pass on one node).
3. **Cross-uid read-recency at 0664** — a _different_ uid's read refreshes the block's mtime, so the agent's
   oldest-mtime eviction spares the most-read shared blocks. This works only because `touch_mtime` uses
   `utimensat(times=NULL)` (both timestamps → now, needs only _write_), not `set_modified` (explicit mtime,
   needs _ownership_ → EPERM cross-uid). The cluster is what surfaced that distinction.
4. **0644 control** — at 0644 the group has no write, so the cross-uid touch fails and the mtime does NOT
   move. Proves the 0664 requirement is real, and rules out "the bump came from a re-cache write."

## Production note — you probably don't need the cross-uid machinery

This harness exercises the cross-uid **fallback**. The **recommended production default is single-uid cache
I/O via the trusted capsule-agent**: if only the trusted agent (one uid) reads/writes the node cache, every
access is same-uid, so the shared gid / `umask 002` / setgid dir are all unnecessary (the `utimensat` fix is
kept regardless — it's strictly more correct and harmless). Single-uid is also _safer_: the untrusted tenant
never touches the shared cache, avoiding a cross-tenant dedup side-channel (a tenant inferring another's
content from a cache HIT). Choose cross-uid deliberately, understanding that side-channel; scope the cache
per trust-domain if tenants are untrusted.

The `--max-gb 0.05` / `--interval-secs 3` in the DaemonSet are TEST values — a real node cache is GBs.

## Run

```
kind create cluster --config kind-cluster.yaml
docker build --provenance=false -f Dockerfile -t capsule-ws:layer1 ..   # context = the crate root
kind load docker-image capsule-ws:layer1 --name nodeplane
kubectl apply -f minio.yaml && kubectl -n nodeplane rollout status deploy/minio
# create the 'workspace' bucket in MinIO (one-shot mc pod), then:
kubectl apply -f cache-agent-daemonset.yaml && kubectl -n nodeplane rollout status ds/cache-agent
./accept.sh
kind delete cluster --name nodeplane
```
