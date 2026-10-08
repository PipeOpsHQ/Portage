# Kind e2e

Two kind clusters. The hub runs against source; dest is reached via a
kubeconfig Secret (cloud identity is [Cluster auth](cluster-auth.md)). This is
the product check, not “STS exists.”

```bash
make e2e
# or: bash hack/kind-e2e.sh
```

| Check | Pass means |
|---|---|
| Classify | `portage inventory` reports `SQLLogical` postgres |
| ClusterPair | dest API reachable; `source.address` is dest→source WAL/NodePort |
| Usefulness gate | empty-DB `Backup` **Failed** (dump too small — not live PGDATA `du`) |
| Useful backup | dump ≥ 64 KiB in the object store, `Policy.status.backupHealthy` |
| Restore | `dest=dst`, dest Ready, `pg_isready`, **seeded rows on dest**, source intact |
| Cluster objects | dest ConfigMap + CRD/CR exist after Restore; Replicate stays CatchingUp and live-updates dest |
| PVC bytes | VolSync restic `lastSyncTime` on **`portage-data`** (not a cache CR) **and** dest PVC marker file; second write lands (incremental) |
| Cutover freeze | source replicas **0**, dest STS still present |

CI: `.github/workflows/e2e.yaml` (40 minute timeout). Snapshot CRDs + Helm VolSync.
`ClusterPair.spec.source.address` is dest→source WAL (NodePort on the src kind node).

The PVC check fails if a ReplicationSource `sourcePVC` starts with `volsync-`.
VolSync cache volumes (`volsync-src-*`, `volsync-dst-*`, legacy
`volsync-*-cache`) are mover scratch; classifying them nests
ReplicationSources (cache-of-cache) and starves `files/data`. Dest-local
K8up/Velero must not mount them.

## Object store

The script starts SeaweedFS (`weed mini`) in the source kind node's network
namespace. Mover pods on both clusters use `http://<src-node-ip>:9000`.
Bucket `portage`, access key `portage`, secret `portageportage`. The image
defaults to `chrislusf/seaweedfs:4.48`; override it with `S3_IMAGE`.

S3 listens on port 9000. WebDAV, Admin, Iceberg, and Lance stay off so they
do not bind ports on the kind node. `quay.io/minio` and Docker Hub
`minio/minio` reject anonymous pulls, which is why CI uses this image.
A real hub can use any path-style S3 endpoint (`PORTAGE_S3_*`); see
[Install](install.md).

## Prerequisites

`kind`, `kubectl`, `go`, `docker`, and `curl`. When `helm` is on `PATH` the
script installs VolSync on both clusters and runs the PVC byte checks.
Without helm those two checks are skipped.

## Next

- [Architecture](architecture.md)
- [Contributing](contributing.md)
