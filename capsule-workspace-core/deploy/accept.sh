#!/usr/bin/env bash
# Layer 1 kind acceptance tests. Each scenario is a hard assertion; exits non-zero on any fail.
set -uo pipefail
NS="--context kind-nodeplane -n nodeplane"
W1=nodeplane-worker
W2=nodeplane-worker2
PASS=0; FAIL=0
ok(){ echo "  PASS: $1"; PASS=$((PASS+1)); }
bad(){ echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

agent_on(){ kubectl $NS get pod -l app=cache-agent --field-selector spec.nodeName=$1 -o jsonpath='{.items[0].metadata.name}'; }
block_mtime(){ kubectl $NS exec "$(agent_on "$1")" -c agent -- stat -c '%Y' "/cache/blocks/$2" 2>/dev/null; }

# Clear both nodes' caches so the run is idempotent (a read-through populates the OTHER node's cache, so
# stale state from a prior run would turn a per-node MISS into a HIT).
clean(){ for n in $W1 $W2; do kubectl $NS exec "$(agent_on $n)" -c agent -- sh -c 'rm -f /cache/blocks/* /cache/manifests/* 2>/dev/null || true'; done; }

# probe <name> <uid> <node> <op> <id> <umask>; echoes the probe stdout.
probe(){
  local name=$1 uid=$2 node=$3 op=$4 id=$5 um=${6:-002}
  local cmd="umask $um; exec capsule-workspace cache-probe --cache-dir /cache --store s3://workspace --op $op --id $id"
  kubectl $NS delete pod "$name" --ignore-not-found >/dev/null 2>&1
  kubectl $NS run "$name" --image=capsule-ws:layer1 --restart=Never --overrides="{
    \"spec\":{\"nodeName\":\"$node\",\"securityContext\":{\"runAsUser\":$uid,\"supplementalGroups\":[3000]},
      \"containers\":[{\"name\":\"p\",\"image\":\"capsule-ws:layer1\",\"imagePullPolicy\":\"IfNotPresent\",
        \"command\":[\"sh\",\"-c\",\"$cmd\"],
        \"env\":[{\"name\":\"S3_ENDPOINT_URL\",\"value\":\"http://minio:9000\"},{\"name\":\"AWS_ACCESS_KEY_ID\",\"value\":\"minioadmin\"},{\"name\":\"AWS_SECRET_ACCESS_KEY\",\"value\":\"minioadmin\"},{\"name\":\"AWS_REGION\",\"value\":\"us-east-1\"}],
        \"volumeMounts\":[{\"name\":\"cache\",\"mountPath\":\"/cache\"}]}],
      \"volumes\":[{\"name\":\"cache\",\"hostPath\":{\"path\":\"/var/cache/capsule-ws\",\"type\":\"DirectoryOrCreate\"}}]}
  }" >/dev/null 2>&1
  kubectl $NS wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$name" --timeout=45s >/dev/null 2>&1 \
    || kubectl $NS wait --for=jsonpath='{.status.phase}'=Failed pod/"$name" --timeout=3s >/dev/null 2>&1
  kubectl $NS logs "$name" 2>/dev/null
  kubectl $NS delete pod "$name" --ignore-not-found --wait=false >/dev/null 2>&1
}

clean
IDA=$(printf 'a%.0s' {1..64})
IDB=$(printf 'b%.0s' {1..64})

echo "### 1. Cross-UID read HIT: pod A uid1000 writes, pod B uid2000 reads on the SAME node ###"
probe put-a 1000 $W1 put "$IDA" >/dev/null
R=$(probe get-b 2000 $W1 get "$IDA"); echo "   B(uid2000)@$W1: $R"
case "$R" in *HIT*correct=true*) ok "cross-UID read served from the shared node cache";; *) bad "expected cross-UID HIT: $R";; esac

echo "### 2. Per-node scoping: same id on the OTHER node must MISS (falls through to shared S3) ###"
R=$(probe get-c 1000 $W2 get "$IDA"); echo "   C(uid1000)@$W2: $R"
case "$R" in *MISS*correct=true*) ok "the other node's cache does not have it -> per-node cache is real";; *) bad "expected MISS on $W2: $R";; esac

echo "### 3. Cross-UID read-recency (0664 perms payoff): a DIFFERENT uid's read must REFRESH the mtime ###"
clean
probe put-t 1000 $W1 put "$IDB" 002 >/dev/null
M0=$(block_mtime $W1 "$IDB"); sleep 2
probe get-t 2000 $W1 get "$IDB" 002 >/dev/null
M1=$(block_mtime $W1 "$IDB")
echo "   0664 block, cross-UID read: mtime ${M0:-?} -> ${M1:-?}"
if [ -n "${M0:-}" ] && [ -n "${M1:-}" ] && [ "$M1" -gt "$M0" ]; then
  ok "cross-UID read refreshed the block at 0664 (utimensat NULL needs only write) -> read-recency works across pods"
else bad "cross-UID touch did not refresh at 0664 (${M0:-?}->${M1:-?})"; fi

echo "### 4. Control at 0644: group has no write, so the cross-UID touch must FAIL -> mtime UNCHANGED ###"
clean
probe put-u 1000 $W1 put "$IDB" 022 >/dev/null
N0=$(block_mtime $W1 "$IDB"); sleep 2
probe get-u 2000 $W1 get "$IDB" 022 >/dev/null
N1=$(block_mtime $W1 "$IDB")
echo "   0644 block, cross-UID read: mtime ${N0:-?} -> ${N1:-?}"
if [ -n "${N0:-}" ] && [ "${N0:-}" = "${N1:-}" ]; then
  ok "at 0644 the cross-UID touch fails -> falls back to write-age (degraded, safe) -> this is WHY 0664 is needed"
else bad "0644 control unexpected (${N0:-?}->${N1:-?})"; fi

echo
echo "=================  PASS=$PASS  FAIL=$FAIL  ================="
[ "$FAIL" -eq 0 ]
