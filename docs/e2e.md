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
MinIO shares the source kind node's netns (`SRC_IP:9000`) so mover pods on both
clusters can reach it. Images come from `quay.io/minio/minio` (Docker Hub no
longer serves `minio/minio`). `ClusterPair.spec.source.address` is dest→source WAL
(NodePort on the src kind node).

The PVC check fails if a ReplicationSource `sourcePVC` starts with `volsync-`.
VolSync cache volumes are mover scratch; classifying them nests
ReplicationSources (cache-of-cache) and starves `files/data`.

## Next

- [Architecture](architecture.md)
- [Contributing](contributing.md)
