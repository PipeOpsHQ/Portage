#!/usr/bin/env bash
# Product e2e on two kind clusters:
#   classify → useless backup fails → useful dump stored → dest restore
#   is dest (not source), dest pg_isready, dest has the seeded table,
#   cutover freezes source.
# E2E_FULL=1 also helm-installs VolSync (does not assert live WAL).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC=portage-src
DST=portage-dst
STORE=/tmp/portage-e2e-store
LOG=/tmp/portage-controller.log
CTRL_PID=""
PASSES=0

cleanup() {
  if [[ -n "${CTRL_PID}" ]]; then kill "${CTRL_PID}" 2>/dev/null || true; fi
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
  tail -n 120 "$LOG" 2>/dev/null || true
  exit 1
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

kind delete cluster --name "$SRC" 2>/dev/null || true
kind delete cluster --name "$DST" 2>/dev/null || true
kind create cluster --name "$SRC"
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

if [[ "${E2E_FULL:-}" == "1" ]] && command -v helm >/dev/null; then
  helm repo add backube https://backube.github.io/helm-charts/ >/dev/null
  helm repo update >/dev/null
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$SRC_CTX"
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$DST_CTX"
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
cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: ClusterPair
metadata:
  name: kind-pair
spec:
  source:
    name: src
  destination:
    name: dst
    kubeconfigSecret:
      name: dest-kubeconfig
      namespace: portage-system
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
PORTAGE_STORE_DIR="$STORE" KUBECONFIG=/tmp/portage-src.kubeconfig /tmp/portage-controller \
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

kc -n apps create configmap app-config --from-literal=k=v2 --dry-run=client -o yaml | kc apply -f -
kc -n apps patch widget w1 --type merge -p '{"spec":{"k":"v2"}}'
cat <<'EOF' | kc apply -f -
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: replicate-objects
  namespace: apps
spec:
  type: Replicate
  policyRef: objects
EOF
wait_action_in apps replicate-objects Succeeded 40
dest_k=$(kd -n apps get configmap app-config -o jsonpath='{.data.k}')
[[ "$dest_k" == "v2" ]] || die "dest ConfigMap not live-synced k=$dest_k want v2"
dest_w=$(kd -n apps get widget w1 -o jsonpath='{.spec.k}' 2>/dev/null || true)
[[ "$dest_w" == "v2" ]] || die "dest Widget not live-synced spec.k=$dest_w want v2"
pass "cluster-objects replicate live-updated dest ConfigMap+Widget k=v2"

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
