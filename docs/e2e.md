# Kind e2e

Two kind clusters. The hub runs against source; dest is reached via a
kubeconfig Secret. This is the product check, not “STS exists.”

```bash
make e2e
# or: bash hack/kind-e2e.sh
E2E_FULL=1 bash hack/kind-e2e.sh   # also helm-install VolSync
```

| Check | Pass means |
|---|---|
| Classify | `portage inventory` reports `SQLLogical` postgres |
| ClusterPair | dest API reachable |
| Usefulness gate | empty-DB `Backup` **Failed** (dump too small — not live PGDATA `du`) |
| Useful backup | dump ≥ 64 KiB in the object store, `Policy.status.backupHealthy` |
| Restore | `dest=dst`, dest Ready, `pg_isready`, **seeded rows on dest**, source intact |
| Cluster objects | dest ConfigMap + CRD/CR exist after Restore; Replicate live-updates dest |
| Cutover freeze | source replicas **0**, dest STS still present |

CI: `.github/workflows/e2e.yaml` (25 minute timeout).

!!! note
    Live WAL across two Kind clusters needs dest→source:5432 routing. Default
    CI does not assert VolSync byte sync (`E2E_FULL=1` only installs the chart).

## Next

- [Architecture](architecture.md)
- [Contributing](contributing.md)
