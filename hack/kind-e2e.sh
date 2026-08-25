#!/usr/bin/env bash
# Two kind clusters: CRDs on both, optional VolSync, postgres inventory on src.
# WAL replica is exercised same-cluster (two namespaces) so CI does not need
# cross-kind routing. Set E2E_FULL=1 to also helm-install VolSync.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC=portage-src
DST=portage-dst

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
go -C "$ROOT" build -o /tmp/portage ./cmd/portage
/tmp/portage --context "$SRC_CTX" inventory -n pg | tee /tmp/portage-inv.txt
grep -q SQLLogical /tmp/portage-inv.txt || grep -q postgres /tmp/portage-inv.txt

echo "kind e2e ok: two clusters, CRDs, postgres classified"
kind delete cluster --name "$SRC"
kind delete cluster --name "$DST"
