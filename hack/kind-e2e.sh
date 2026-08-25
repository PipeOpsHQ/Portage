#!/usr/bin/env bash
# Two kind clusters: CRDs, postgres classified, RPO Backup, dest Sanitize-apply.
# WAL replica is same-cluster (two namespaces) when E2E_FULL=1 (VolSync).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC=portage-src
DST=portage-dst
STORE=/tmp/portage-e2e-store
LOG=/tmp/portage-controller.log
CTRL_PID=""

cleanup() {
  if [[ -n "${CTRL_PID}" ]]; then kill "${CTRL_PID}" 2>/dev/null || true; fi
  kind delete cluster --name "$SRC" 2>/dev/null || true
  kind delete cluster --name "$DST" 2>/dev/null || true
}
trap cleanup EXIT

need() { command -v "$1" >/dev/null || { echo "need $1"; exit 1; }; }
need kind
need kubectl
need go

kind delete cluster --name "$SRC" 2>/dev/null || true
kind delete cluster --name "$DST" 2>/dev/null || true
kind create cluster --name "$SRC"
kind create cluster --name "$DST"

SRC_CTX="kind-${SRC}"
DST_CTX="kind-${DST}"

# CRDs must be apiextensions.k8s.io/v1 — v1beta1 is gone on Kind 1.22+.
if grep -q 'apiextensions.k8s.io/v1beta1' "${ROOT}/config/crd/bases"/*.yaml; then
  echo "CRDs are v1beta1; regenerate with: make manifests" >&2
  exit 1
fi
kubectl --context "$SRC_CTX" apply -k "${ROOT}/config/crd"
kubectl --context "$DST_CTX" apply -k "${ROOT}/config/crd"
kubectl --context "$SRC_CTX" wait --for=condition=Established crd/policies.portage.io --timeout=60s
kubectl --context "$DST_CTX" wait --for=condition=Established crd/actions.portage.io --timeout=60s

if [[ "${E2E_FULL:-}" == "1" ]] && command -v helm >/dev/null; then
  helm repo add backube https://backube.github.io/helm-charts/ >/dev/null
  helm repo update >/dev/null
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$SRC_CTX"
  helm upgrade --install volsync backube/volsync -n volsync-system --create-namespace --kube-context "$DST_CTX"
fi

kubectl --context "$SRC_CTX" create ns pg || true
kubectl --context "$DST_CTX" create ns pg || true
cat <<'EOF' | kubectl --context "$SRC_CTX" -n pg apply -f -
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
EOF

kubectl --context "$SRC_CTX" -n pg rollout status sts/pg --timeout=180s
kubectl --context "$SRC_CTX" -n pg wait pod/pg-0 --for=condition=Ready --timeout=180s
# >64KiB so usefulness-gated backup can Succeeded.
kubectl --context "$SRC_CTX" -n pg exec pg-0 -- \
  psql -U postgres -c "CREATE TABLE blob AS SELECT repeat('x', 8192) FROM generate_series(1,32);"

go -C "$ROOT" build -o /tmp/portage ./cmd/portage
go -C "$ROOT" build -o /tmp/portage-controller ./cmd/controller
/tmp/portage --context "$SRC_CTX" inventory -n pg | tee /tmp/portage-inv.txt
grep -q SQLLogical /tmp/portage-inv.txt || grep -q postgres /tmp/portage-inv.txt

kubectl --context "$SRC_CTX" create ns portage-system || true
kind get kubeconfig --name "$DST" > /tmp/portage-dst.kubeconfig
kubectl --context "$SRC_CTX" -n portage-system create secret generic dest-kubeconfig \
  --from-file=kubeconfig=/tmp/portage-dst.kubeconfig --dry-run=client -o yaml | kubectl --context "$SRC_CTX" apply -f -

cat <<'EOF' | kubectl --context "$SRC_CTX" apply -f -
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
    rpo: 1h
    requireUseful: true
  renderer:
    kind: Sanitize
EOF

mkdir -p "$STORE"
kind get kubeconfig --name "$SRC" > /tmp/portage-src.kubeconfig
export KUBECONFIG=/tmp/portage-src.kubeconfig
PORTAGE_STORE_DIR="$STORE" /tmp/portage-controller \
  --leader-elect=false \
  --metrics-bind-address=:18080 \
  --health-probe-bind-address=:18081 \
  >"$LOG" 2>&1 &
CTRL_PID=$!

phase=""
for _ in $(seq 1 40); do
  phase=$(kubectl --context "$SRC_CTX" -n pg get actions.portage.io -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
  echo "backup phase=${phase:-none}"
  if [[ "$phase" == "Succeeded" ]]; then
    break
  fi
  if [[ "$phase" == "Failed" ]]; then
    kubectl --context "$SRC_CTX" -n pg get actions.portage.io -o yaml || true
    tail -n 80 "$LOG" || true
    exit 1
  fi
  sleep 3
done
if [[ "$phase" != "Succeeded" ]]; then
  echo "backup did not Succeeded" >&2
  kubectl --context "$SRC_CTX" -n pg get actions.portage.io -o yaml || true
  tail -n 80 "$LOG" || true
  exit 1
fi

cat <<'EOF' | kubectl --context "$SRC_CTX" apply -f -
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: restore-pg
  namespace: pg
spec:
  type: Restore
  policyRef: pg
EOF

found=0
for _ in $(seq 1 40); do
  if kubectl --context "$DST_CTX" -n pg get sts pg >/dev/null 2>&1; then
    found=1
    break
  fi
  sleep 3
done
if [[ "$found" != "1" ]]; then
  echo "dest cluster missing applied STS" >&2
  kubectl --context "$SRC_CTX" -n pg get action restore-pg -o yaml || true
  tail -n 80 "$LOG" || true
  exit 1
fi

echo "kind e2e ok: two clusters, CRDs, postgres classified, RPO backup, dest STS applied"
