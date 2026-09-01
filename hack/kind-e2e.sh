#!/usr/bin/env bash
# Product e2e on two kind clusters:
#   classify → useless backup fails → useful dump stored → dest restore
#   dest Ready+pg_isready+rows, cluster-objects live-sync, PVC dest bytes
#   via VolSync restic (incremental ObjectStore), cutover freeze.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC=portage-src
DST=portage-dst
STORE=/tmp/portage-e2e-store
LOG=/tmp/portage-controller.log
CTRL_PID=""
PASSES=0
MINIO_CID=""
PG_NODEPORT=30432

cleanup() {
  if [[ -n "${CTRL_PID}" ]]; then kill "${CTRL_PID}" 2>/dev/null || true; fi
  if [[ -n "${MINIO_CID}" ]]; then docker rm -f portage-minio 2>/dev/null || true; fi
  kind delete cluster --name "$SRC" 2>/dev/null || true
  kind delete cluster --name "$DST" 2>/dev/null || true
}
trap cleanup EXIT

need() { command -v "$1" >/dev/null || { echo "need $1"; exit 1; }; }
pass() { echo "PASS: $*"; PASSES=$((PASSES + 1)); }
die() {
  echo "FAIL: $*" >&2
  kubectl --context "${SRC_CTX:-}" -n pg get policy,action -o yaml 2>/dev/null || true
  kubectl --context "${SRC_CTX:-}" get clusterpair -o yaml 2>/dev/null || true
  kubectl --context "${DST_CTX:-}" -n pg get sts,pod 2>/dev/null || true
  kubectl --context "${SRC_CTX:-}" -n files get pvc,pod,job,event,secret,replicationsources.volsync.backube -o yaml 2>/dev/null | sed '/RESTIC_PASSWORD\|AWS_SECRET\|psk:/,+1d' || true
  kubectl --context "${DST_CTX:-}" -n files get pvc,pod,job,event,secret,replicationdestinations.volsync.backube -o yaml 2>/dev/null | sed '/RESTIC_PASSWORD\|AWS_SECRET\|psk:/,+1d' || true
  kubectl --context "${SRC_CTX:-}" -n volsync-system get pod 2>/dev/null || true
  kubectl --context "${SRC_CTX:-}" -n volsync-system logs deploy/volsync --tail=80 2>/dev/null || true
  kubectl --context "${DST_CTX:-}" -n volsync-system logs deploy/volsync --tail=80 2>/dev/null || true
  tail -n 120 "$LOG" 2>/dev/null || true
  exit 1
}

kind_net_ip() {
  docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$1" 2>/dev/null || true
}

kc() { kubectl --context "$SRC_CTX" "$@"; }
kd() { kubectl --context "$DST_CTX" "$@"; }

wait_psql() {
  local ctx=$1
  local i
  for i in $(seq 1 30); do
    if kubectl --context "$ctx" -n pg exec pg-0 -- psql -U postgres -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_action() {
  wait_action_in pg "$@"
}

wait_action_in() {
  local ns=$1 name=$2 want=$3
  local tries=${4:-40}
  local i phase
  for i in $(seq 1 "$tries"); do
    phase=$(kc -n "$ns" get action "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    echo "  $ns/$name phase=${phase:-none}"
    if [[ "$phase" == "$want" ]]; then
      return 0
    fi
    if [[ "$phase" == "Failed" && "$want" != "Failed" ]]; then
      die "$name Failed: $(kc -n "$ns" get action "$name" -o jsonpath='{.status.message}')"
    fi
    if [[ "$phase" == "Succeeded" && "$want" != "Succeeded" ]]; then
      die "$name Succeeded but wanted $want"
    fi
    sleep 3
  done
  die "$name did not reach $want (last=${phase:-none})"
}

apply_action() {
  local name=$1 typ=$2
  cat <<EOF | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: ${name}
  namespace: pg
spec:
  type: ${typ}
  policyRef: pg
EOF
}

need kind
need kubectl
need go
need docker

kind delete cluster --name "$SRC" 2>/dev/null || true
kind delete cluster --name "$DST" 2>/dev/null || true
cat > /tmp/kind-portage-src.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${SRC}
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: ${PG_NODEPORT}
    hostPort: ${PG_NODEPORT}
    protocol: TCP
EOF
kind create cluster --config /tmp/kind-portage-src.yaml
kind create cluster --name "$DST"

SRC_CTX="kind-${SRC}"
DST_CTX="kind-${DST}"

if grep -q 'apiextensions.k8s.io/v1beta1' "${ROOT}/config/crd/bases"/*.yaml; then
  die "CRDs are v1beta1; regenerate with: make manifests"
fi
kc apply -k "${ROOT}/config/crd"
kd apply -k "${ROOT}/config/crd"
kc wait --for=condition=Established crd/policies.portage.io --timeout=60s
kd wait --for=condition=Established crd/actions.portage.io --timeout=60s

# VolSync Owns VolumeSnapshot; without the CRD the operator never becomes
# Ready (kind has no snapshot controller) and restic jobs are never created.
SNAP_CRD=https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.1/client/config/crd
for crd in snapshot.storage.k8s.io_volumesnapshotclasses.yaml \
           snapshot.storage.k8s.io_volumesnapshotcontents.yaml \
           snapshot.storage.k8s.io_volumesnapshots.yaml; do
  kc apply -f "${SNAP_CRD}/${crd}"
  kd apply -f "${SNAP_CRD}/${crd}"
done
kc wait --for=condition=Established crd/volumesnapshots.snapshot.storage.k8s.io --timeout=60s
kd wait --for=condition=Established crd/volumesnapshots.snapshot.storage.k8s.io --timeout=60s

SRC_IP="$(kind_net_ip "${SRC}-control-plane")"
if [[ -z "$SRC_IP" || "$SRC_IP" == "<no value>" ]]; then
  die "kind src node has no kind-network IP"
fi
# Share the src node's netns so MinIO listens on SRC_IP:9000. Src and dest
# mover pods reach that address via the kind network (CNI overlay cannot hit
# a sibling docker container IP; host:9000 hairpins on GHA).
docker rm -f portage-minio 2>/dev/null || true
docker run -d --name portage-minio --network "container:${SRC}-control-plane" \
  -e MINIO_ROOT_USER=portage \
  -e MINIO_ROOT_PASSWORD=portageportage \
  minio/minio server /data
MINIO_CID=portage-minio
mc_ok=0
for _ in $(seq 1 20); do
  if docker run --rm --network "container:${SRC}-control-plane" --entrypoint /bin/sh minio/mc -c \
    "mc alias set m http://127.0.0.1:9000 portage portageportage && mc mb -p m/portage" \
    >/dev/null 2>&1; then
    mc_ok=1
    break
  fi
  sleep 2
done
[[ "$mc_ok" == "1" ]] || die "minio never accepted mc on src node :9000"
export PORTAGE_S3_ENDPOINT="http://${SRC_IP}:9000"
export PORTAGE_S3_ACCESS_KEY=portage
export PORTAGE_S3_SECRET_KEY=portageportage
export PORTAGE_S3_BUCKET=portage
export PORTAGE_VOLSYNC_SCHEDULE="* * * * *"
KIND_GW="$SRC_IP"
echo "  minio endpoint ${PORTAGE_S3_ENDPOINT} src node ${SRC_IP}"

if command -v helm >/dev/null; then
  helm repo add backube https://backube.github.io/helm-charts/ >/dev/null
  helm repo update >/dev/null
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$SRC_CTX" --wait --timeout 180s
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$DST_CTX" --wait --timeout 180s
  VOLSYNC=1
else
  VOLSYNC=0
  echo "WARN: helm not found; skipping VolSync PVC byte assert"
fi

kc create ns pg || true
kd create ns pg || true
cat <<'EOF' | kc -n pg apply -f -
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pg
spec:
  serviceName: pg
  replicas: 1
  selector:
    matchLabels: { app: pg }
  template:
    metadata:
      labels: { app: pg }
    spec:
      containers:
        - name: postgres
          image: postgres:16
          env:
            - { name: POSTGRES_PASSWORD, value: portage }
          ports: [{ containerPort: 5432 }]
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            periodSeconds: 2
EOF

kc -n pg rollout status sts/pg --timeout=180s
kc -n pg wait pod/pg-0 --for=condition=Ready --timeout=180s
wait_psql "$SRC_CTX" || die "source postgres never accepted connections"

go -C "$ROOT" build -o /tmp/portage ./cmd/portage
go -C "$ROOT" build -o /tmp/portage-controller ./cmd/controller

# --- classify ---
/tmp/portage --context "$SRC_CTX" inventory -n pg | tee /tmp/portage-inv.txt
grep -q SQLLogical /tmp/portage-inv.txt || die "inventory did not classify postgres as SQLLogical"
pass "classify: SQLLogical postgres"

kc create ns portage-system || true
kind get kubeconfig --name "$DST" > /tmp/portage-dst.kubeconfig
kc -n portage-system create secret generic dest-kubeconfig \
  --from-file=kubeconfig=/tmp/portage-dst.kubeconfig --dry-run=client -o yaml | kc apply -f -

# No RPO: e2e creates Backup Actions by name so empty vs useful are distinct.
cat <<EOF | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: ClusterPair
metadata:
  name: kind-pair
spec:
  source:
    name: src
    address: ${KIND_GW}:${PG_NODEPORT}
  destination:
    name: dst
    kubeconfigSecret:
      name: dest-kubeconfig
      namespace: portage-system
    objectStore:
      url: s3://portage/e2e
  transport: ObjectStore
---
apiVersion: portage.io/v1alpha1
kind: Policy
metadata:
  name: pg
  namespace: pg
spec:
  clusterPair: kind-pair
  selector:
    namespaces: [pg]
  backup:
    enabled: true
    requireUseful: true
  renderer:
    kind: Sanitize
EOF

mkdir -p "$STORE"
kind get kubeconfig --name "$SRC" > /tmp/portage-src.kubeconfig
PORTAGE_STORE_DIR="$STORE" \
PORTAGE_S3_ENDPOINT="$PORTAGE_S3_ENDPOINT" \
PORTAGE_S3_ACCESS_KEY="$PORTAGE_S3_ACCESS_KEY" \
PORTAGE_S3_SECRET_KEY="$PORTAGE_S3_SECRET_KEY" \
PORTAGE_S3_BUCKET="$PORTAGE_S3_BUCKET" \
PORTAGE_VOLSYNC_SCHEDULE="$PORTAGE_VOLSYNC_SCHEDULE" \
KUBECONFIG=/tmp/portage-src.kubeconfig /tmp/portage-controller \
  --leader-elect=false \
  --metrics-bind-address=:18080 \
  --health-probe-bind-address=:18081 \
  >"$LOG" 2>&1 &
CTRL_PID=$!

pair=""
for _ in $(seq 1 20); do
  pair=$(kc get clusterpair kind-pair -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "$pair" == "Ready" ]] && break
  sleep 2
done
[[ "$pair" == "Ready" ]] || die "ClusterPair not Ready ($pair)"
pass "ClusterPair dest reachable"

# --- useless backup (empty postgres < 64KiB) ---
apply_action backup-empty Backup
wait_action backup-empty Failed
empty_msg=$(kc -n pg get action backup-empty -o jsonpath='{.status.message}')
echo "  backup-empty: $empty_msg"
[[ "$empty_msg" == *useful* ]] || die "empty backup should fail usefulness, got: $empty_msg"
pass "useless backup Failed (usefulness gate)"

# --- useful backup ---
kc -n pg exec pg-0 -- \
  psql -U postgres -c "CREATE TABLE blob AS SELECT repeat('x', 8192) FROM generate_series(1,32);"
src_rows=$(kc -n pg exec pg-0 -- psql -U postgres -tAc "SELECT count(*) FROM blob")
[[ "${src_rows// /}" == "32" ]] || die "seed rows=$src_rows"

apply_action backup-useful Backup
wait_action backup-useful Succeeded
healthy=$(kc -n pg get policy pg -o jsonpath='{.status.backupHealthy}')
useful=$(kc -n pg get policy pg -o jsonpath='{.status.artifacts[0].useful}')
size=$(kc -n pg get policy pg -o jsonpath='{.status.artifacts[0].sizeBytes}')
art=$(kc -n pg get policy pg -o jsonpath='{.status.artifacts[0].artifactID}')
[[ "$healthy" == "true" ]] || die "Policy.backupHealthy=$healthy"
[[ "$useful" == "true" ]] || die "artifact useful=$useful"
[[ -n "$art" ]] || die "missing ArtifactID"
if [[ "${size:-0}" -lt 65536 ]]; then
  die "dump sizeBytes=$size want >= 64KiB"
fi
store_n=$(find "$STORE" -type f 2>/dev/null | wc -l | tr -d ' ')
[[ "$store_n" -ge 1 ]] || die "object store $STORE is empty"
pass "useful backup: dump ${size}B id=$art stored"

# --- restore to dest: not Succeeded without dest Ready+probe; data must land ---
apply_action restore-pg Restore
wait_action restore-pg Succeeded 60
rst_msg=$(kc -n pg get action restore-pg -o jsonpath='{.status.message}')
rst_probe=$(kc -n pg get action restore-pg -o jsonpath='{.status.workloads[0].probeOK}')
rst_ready=$(kc -n pg get action restore-pg -o jsonpath='{.status.workloads[0].ready}')
echo "  restore: $rst_msg"
[[ "$rst_msg" == *dest=dst* ]] || die "restore did not record dest=dst: $rst_msg"
[[ "$rst_probe" == "true" ]] || die "restore probeOK=$rst_probe (class probe required)"
[[ "$rst_ready" == "true" ]] || die "restore ready=$rst_ready"
kd -n pg get sts pg >/dev/null || die "dest missing STS"
kd -n pg wait pod/pg-0 --for=condition=Ready --timeout=180s || die "dest postgres not Ready"
wait_psql "$DST_CTX" || die "dest postgres never accepted connections"
dest_probe=$(kd -n pg exec pg-0 -- pg_isready -U postgres || true)
[[ "$dest_probe" == *accepting* ]] || die "dest pg_isready: $dest_probe"
dest_rows=$(kd -n pg exec pg-0 -- psql -U postgres -tAc "SELECT count(*) FROM blob" 2>/dev/null || echo 0)
[[ "${dest_rows// /}" == "32" ]] || die "dest blob rows=${dest_rows// /} want 32 (dump not replayed — empty dest postgres is not a restore)"
src_rows2=$(kc -n pg exec pg-0 -- psql -U postgres -tAc "SELECT count(*) FROM blob")
[[ "${src_rows2// /}" == "32" ]] || die "source blob lost after restore"
pass "restore dest=dst Ready+pg_isready with 32 blob rows (source intact)"

# --- cluster objects: live API graph including CRDs, dest Get is the probe ---
kc create ns apps || true
kd create ns apps || true
kc -n apps create configmap app-config --from-literal=k=v1 --dry-run=client -o yaml | kc apply -f -
cat <<'EOF' | kc apply -f -
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.stable.example.com
spec:
  group: stable.example.com
  scope: Namespaced
  names:
    plural: widgets
    singular: widget
    kind: Widget
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                k:
                  type: string
EOF
kc wait --for=condition=Established crd/widgets.stable.example.com --timeout=60s
cat <<'EOF' | kc apply -f -
apiVersion: stable.example.com/v1
kind: Widget
metadata:
  name: w1
  namespace: apps
spec:
  k: v1
EOF
cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Policy
metadata:
  name: objects
  namespace: apps
spec:
  clusterPair: kind-pair
  selector:
    namespaces: [apps]
  backup:
    enabled: true
    requireUseful: true
  clusterObjects:
    enabled: true
  replicate:
    enabled: true
    rpo: 15m
  renderer:
    kind: Sanitize
EOF
cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: backup-objects
  namespace: apps
spec:
  type: Backup
  policyRef: objects
EOF
wait_action_in apps backup-objects Succeeded
obj_art=$(kc -n apps get policy objects -o jsonpath='{.status.artifacts[0].artifactID}')
[[ -n "$obj_art" ]] || die "object-graph backup missing ArtifactID"
cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: restore-objects
  namespace: apps
spec:
  type: Restore
  policyRef: objects
EOF
wait_action_in apps restore-objects Succeeded 60
kd -n apps get configmap app-config >/dev/null || die "dest missing ConfigMap after object restore"
dest_k=$(kd -n apps get configmap app-config -o jsonpath='{.data.k}')
[[ "$dest_k" == "v1" ]] || die "dest ConfigMap k=$dest_k want v1"
kd get crd widgets.stable.example.com >/dev/null || die "dest missing CRD widgets.stable.example.com"
kd wait --for=condition=Established crd/widgets.stable.example.com --timeout=60s || die "dest CRD not Established"
dest_w=$(kd -n apps get widget w1 -o jsonpath='{.spec.k}' 2>/dev/null || true)
[[ "$dest_w" == "v1" ]] || die "dest Widget spec.k=$dest_w want v1 (CRD+CR must restore)"
pass "cluster-objects restore dest Get app-config k=v1 and Widget/CRD"

wait_action_in apps replicate-objects CatchingUp 40
kc -n apps create configmap app-config --from-literal=k=v2 --dry-run=client -o yaml | kc apply -f -
kc -n apps patch widget w1 --type merge -p '{"spec":{"k":"v2"}}'
synced=0
for _ in $(seq 1 20); do
  dest_k=$(kd -n apps get configmap app-config -o jsonpath='{.data.k}' 2>/dev/null || true)
  dest_w=$(kd -n apps get widget w1 -o jsonpath='{.spec.k}' 2>/dev/null || true)
  echo "  live-sync dest cm=$dest_k widget=$dest_w"
  if [[ "$dest_k" == "v2" && "$dest_w" == "v2" ]]; then
    synced=1
    break
  fi
  sleep 3
done
[[ "$synced" == "1" ]] || die "dest ConfigMap/Widget not live-synced to v2"
repl_phase=$(kc -n apps get action replicate-objects -o jsonpath='{.status.phase}')
[[ "$repl_phase" == "CatchingUp" ]] || die "live replicate must stay CatchingUp, got $repl_phase"
[[ "$repl_phase" != "Succeeded" ]] || die "live replicate must not Succeeded"
pass "cluster-objects replicate stays CatchingUp and live-updated dest k=v2"

# --- PVC incremental: VolSync restic ObjectStore, dest bytes not just lastSyncTime ---
if [[ "${VOLSYNC}" == "1" ]]; then
  kc create ns files || true
  kd create ns files || true
  # local-path volumes are root-owned; VolSync restic is non-root unless allowed.
  kc annotate ns files volsync.backube/privileged-movers=true --overwrite >/dev/null
  kd annotate ns files volsync.backube/privileged-movers=true --overwrite >/dev/null
  cat <<'EOF' | kc apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data, namespace: files }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 64Mi } }
EOF
  cat <<'EOF' | kd apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data, namespace: files }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 64Mi } }
EOF
  write_marker() {
    local ctx=$1 val=$2
    kubectl --context "$ctx" -n files delete job write-marker --ignore-not-found >/dev/null 2>&1 || true
    cat <<EOF | kubectl --context "$ctx" -n files apply -f -
apiVersion: batch/v1
kind: Job
metadata: { name: write-marker }
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: w
        image: busybox:1.36
        command: ["sh", "-c", "echo ${val} > /data/marker && dd if=/dev/zero bs=1024 count=64 >> /data/marker"]
        volumeMounts: [{ name: data, mountPath: /data }]
      volumes:
      - name: data
        persistentVolumeClaim: { claimName: data }
EOF
    kubectl --context "$ctx" -n files wait --for=condition=complete job/write-marker --timeout=120s
    # Completed Job pods still hold RWO PVCs; VolSync Direct cannot mount until gone.
    kubectl --context "$ctx" -n files delete job write-marker --wait=true >/dev/null 2>&1 || true
  }
  read_marker() {
    local ctx=$1
    kubectl --context "$ctx" -n files delete job read-marker --ignore-not-found >/dev/null 2>&1 || true
    cat <<'EOF' | kubectl --context "$ctx" -n files apply -f -
apiVersion: batch/v1
kind: Job
metadata: { name: read-marker }
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: r
        image: busybox:1.36
        command: ["sh", "-c", "head -n 1 /data/marker"]
        volumeMounts: [{ name: data, mountPath: /data }]
      volumes:
      - name: data
        persistentVolumeClaim: { claimName: data }
EOF
    if kubectl --context "$ctx" -n files wait --for=condition=complete job/read-marker --timeout=90s >/dev/null 2>&1; then
      kubectl --context "$ctx" -n files logs job/read-marker
      kubectl --context "$ctx" -n files delete job read-marker --wait=true >/dev/null 2>&1 || true
    else
      echo ""
    fi
  }
  release_dest_mover() {
    kd -n files delete replicationdestination --all --wait=true >/dev/null 2>&1 || true
    sleep 8
  }
  write_marker "$SRC_CTX" v1
  cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Policy
metadata:
  name: files
  namespace: files
spec:
  clusterPair: kind-pair
  selector:
    namespaces: [files]
  replicate:
    enabled: true
    rpo: 1m
  renderer:
    kind: Sanitize
EOF
  wait_action_in files replicate-files CatchingUp 40
  got=""
  for _ in $(seq 1 30); do
    src_sync=$(kc -n files get replicationsources.volsync.backube -o jsonpath='{.items[0].status.lastSyncTime}' 2>/dev/null || true)
    dst_sync=$(kd -n files get replicationdestinations.volsync.backube -o jsonpath='{.items[0].status.lastSyncTime}' 2>/dev/null || true)
    echo "  volsync lastSyncTime src=${src_sync:-none} dest=${dst_sync:-none}"
    kc -n files get replicationsources.volsync.backube,pod,job 2>/dev/null || echo "  no src ReplicationSource"
    kd -n files get replicationdestinations.volsync.backube,pod,job,pvc 2>/dev/null || echo "  no dest ReplicationDestination"
    if [[ -n "$src_sync" && -n "$dst_sync" ]]; then
      release_dest_mover
      got=$(read_marker "$DST_CTX" | tail -n 1 | tr -d '[:space:]' || true)
      echo "  dest marker=$got"
      [[ "$got" == v1* ]] && break
    fi
    sleep 10
  done
  [[ "$got" == v1* ]] || die "dest PVC missing marker v1 (lastSyncTime is not dest bytes)"
  pass "PVC dest bytes after restic sync (lastSyncTime + marker v1)"
  kc -n files delete replicationsource --all --wait=true >/dev/null 2>&1 || true
  write_marker "$SRC_CTX" v2
  got=""
  for _ in $(seq 1 30); do
    dst_sync=$(kd -n files get replicationdestinations.volsync.backube -o jsonpath='{.items[0].status.lastSyncTime}' 2>/dev/null || true)
    echo "  volsync dest lastSyncTime=${dst_sync:-none}"
    if [[ -n "$dst_sync" ]]; then
      release_dest_mover
      got=$(read_marker "$DST_CTX" | tail -n 1 | tr -d '[:space:]' || true)
      echo "  dest marker after increment=$got"
      [[ "$got" == v2* ]] && break
    fi
    sleep 10
  done
  [[ "$got" == v2* ]] || die "dest PVC did not pick up incremental v2"
  pass "PVC incremental dest marker v2"
else
  echo "SKIP: VolSync PVC byte assert (helm not installed)"
fi

# --- cutover freeze: source writes paused ---
apply_action cutover-pg Cutover
frozen=0
for _ in $(seq 1 20); do
  repl=$(kc -n pg get sts pg -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
  echo "  source replicas=$repl"
  if [[ "$repl" == "0" ]]; then
    frozen=1
    break
  fi
  sleep 3
done
[[ "$frozen" == "1" ]] || die "cutover did not scale source to 0"
kd -n pg get sts pg >/dev/null || die "dest STS gone after cutover freeze"
pass "cutover froze source (replicas=0); dest STS still present"

echo "kind e2e ok: $PASSES product checks"
