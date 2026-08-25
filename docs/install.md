# Install

## Prerequisites

- Kubernetes 1.27+
- `kubectl`, Go 1.24+ (for building from source)
- CSI snapshot controller if you use VolumeSnapshots
- Optional: [VolSync](https://volsync.readthedocs.io/) on source and dest
- Optional: S3-compatible bucket (MinIO, R2, AWS)

## CRDs + controller

```bash
kubectl apply -k https://github.com/PipeOpsHQ/Portage/config/default?ref=v0.1.0
# or from a clone:
kubectl apply -k config/default
```

Helm:

```bash
helm upgrade --install portage charts/portage \
  --namespace portage-system --create-namespace
# VolSync as a subchart:
helm upgrade --install portage charts/portage \
  --set volsync.enabled=true
```

## CLI

```bash
go install github.com/PipeOpsHQ/portage/cmd/portage@v0.1.0
# or download from GitHub Releases
portage inventory -n my-tenant
```

## Images

```text
ghcr.io/pipeopshq/portage:<tag>
```

Controller binary is `/controller` in the image (`cmd/controller`).

## Object store

| Variable | Purpose |
|---|---|
| `PORTAGE_STORE_DIR` | Filesystem/PVC dump store |
| `PORTAGE_S3_ENDPOINT` | MinIO/R2/AWS-compatible endpoint |
| `PORTAGE_S3_BUCKET` | Bucket |
| `PORTAGE_S3_PREFIX` | Key prefix |
| `AWS_ACCESS_KEY_ID` / `PORTAGE_S3_ACCESS_KEY` | SigV4 |
| `AWS_SECRET_ACCESS_KEY` / `PORTAGE_S3_SECRET_KEY` | SigV4 |
| `AWS_REGION` | Default `us-east-1` |

Without these, the hub uses an in-process memory store (dev only).

## Next

Hub is up. Next is how Portage decides what to copy and when a restore is
actually done:

- [Concepts](concepts.md) — classify, usefulness, dual cluster, class probes
- [CRDs](crds.md) — ClusterPair, Policy, Action
- [Backup & restore](backup-restore.md)
