# Backup and restore

## Backup

1. Classify source workloads.
2. Exec logical dump (`pg_dumpall`, `mysqldump`, …) when the engine supports it.
3. Put the dump in the object store (SigV4 S3, `PORTAGE_STORE_DIR`, or memory).
4. Optionally create CSI VolumeSnapshots (same-cloud safety net).
5. Evaluate **usefulness**. Certs-only ~12 KiB Postgres **fails** the Action.
6. Write `Policy.status.artifacts` + `backupHealthy`.

```bash
kubectl apply -f config/samples/portage_v1alpha1_policy.yaml
kubectl apply -f - <<EOF
apiVersion: portage.io/v1alpha1
kind: Action
metadata: { name: backup-1, namespace: tenant-a }
spec: { type: Backup, policyRef: tenant-continuity }
EOF
kubectl -n tenant-a get action backup-1 -w
```

## Restore

1. Preflight: refuse if any stateful artifact is not useful.
2. Export source objects, **Sanitize** (drop zone pins, remap StorageClass).
3. Apply to **dest** (PVC first, then STS/Deploy).
4. Rehydrate: PVC-from-snapshot **by original name**, or replay dump via `psql` stdin.
5. Heal Pending topology/SC.
6. Wait Ready + `pg_isready` (or class probe). **Never Succeeded without that.**

`Policy.spec.restore.auto: true` creates one `restore-auto-<policy>` Action when
a covered PVC is gone **and** backups are useful. Bound PVCs are not overwritten.

## Probes

| Engine | Probe |
|---|---|
| postgres / timescale | `pg_isready` |
| mysql / mariadb | `mysqladmin ping` |
| redis / valkey / dragonfly | `PING` |
| mongo | `hello` |
| GenericPVC | volume-mounted / HTTP |

## Next

- [Replication & cutover](cutover.md)
- [Configuration](configuration.md)
