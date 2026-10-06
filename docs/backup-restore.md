# Backup and restore

## Backup

1. Classify source workloads.
2. Exec logical dump (`pg_dump` of the engine database, `mysqldump`, …).
   Commands read the container environment (`POSTGRES_USER`/`POSTGRES_DB`,
   `REDIS_PASSWORD`) so marketplace images dump. Redis writes a temp RDB
   then cats it — `redis-cli --rdb /dev/stdout` exits 1 after a successful
   transfer because it fsyncs the pipe. Postgres is **not** judged on live
   PGDATA size. Generic PVCs (no dump command, no pod, no CSI snapshot)
   take size from the VolSync restic ReplicationSource
   (`latestMoverStatus.logs` bytes processed / `lastSyncTime`).
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

1. Preflight: restore each workload independently. Stateless and workloads
   with a useful artifact proceed. A missing PVC artifact is **skipped**
   (reported on that workload) and does not abort the Action. The Action
   fails only when nothing in the inventory is restorable.
2. Export source objects, render dest (`Sanitize`, `Git`, or `Webhook`; always
   sanitized after). `runtimeClassName` is remapped via
   `ClusterPair.spec.runtimeClassMap` or stripped when unmapped. Bound dest
   PVCs (live Replicate) are left; existing dest STS/Deploy are updated.
3. Apply to **dest** (PVC first, then STS/Deploy) using the ClusterPair dest
   client (kubeconfig or cloud identity) — not the hub cache.
4. Rehydrate: PVC-from-snapshot **by original name**, or replay dump via `psql`
   stdin. Dump apply runs **once** (re-psql every reconcile hangs exec).
5. Heal Pending topology/SC.
6. Wait Ready + `pg_isready` (or class probe). Empty dest Postgres that is Ready
   is **not** a restore until the dump lands. **Never Succeeded without that.**

`Policy.spec.restore.auto: true` creates one `restore-auto-<policy>` Action when
a covered PVC is gone **and** backups are useful. Bound PVCs are not overwritten.

## Cluster objects (API graph)

Opt in with `Policy.spec.clusterObjects.enabled: true`. This is **not** Velero
etcd backup. The Kubernetes API is the data plane:

1. **Backup** — live list → sanitize → JSON snapshot in the object store.
2. **Restore** — snapshot → sanitize → dest create-or-update. `Succeeded` only
   after dest Get (CRDs: `Established`).
3. **Replicate** — live list on every reconcile, dest update (active
   restoration). The Action **stays CatchingUp** and re-attests dest Get.
   It does not Succeeded and stop.

**CRDs are always in the graph** when this is enabled — unknown CRs cannot
restore without them — unless listed in `excludeGVKs`. Each form matches
both the CRD object and its instances:

- CRD name: `backendtlspolicies.gateway.networking.k8s.io`
- `group/kind`: `gateway.networking.k8s.io/BackendTLSPolicy` (also
  `gateway.networking.k8s.io/GatewayClass` → `gatewayclasses`)
- `group/version/kind`: `gateway.networking.k8s.io/v1alpha3/BackendTLSPolicy`
- resource: `backendtlspolicies`, `gatewayclasses`

Dest admission, Gateway API experimental-vs-standard channel, and version
skew are the usual reasons.
Other cluster-scoped APIs (Namespaces in the selector,
ClusterRoles/Bindings, ClusterIssuers, …) are included by default
(`includeClusterScoped: true`). Nodes, PVs, StorageClasses, CSI, ServiceCIDRs,
CapsuleConfiguration, VolSync CRs, admission webhooks, and `system:` RBAC stay
dest-local. Source objects with `deletionTimestamp` are Deleted on dest.
Dest Update retries 409 Conflict.

STS/Deploy/DS/PVC stay on the workload movers. Unknown CRs stay in the graph.
`403` list is skipped (not silently dropped from a GVR we could read).

## Probes

| Engine | Probe |
|---|---|
| postgres / timescale | `pg_isready` |
| mysql / mariadb | `mysqladmin ping` |
| redis / valkey / dragonfly | `PING` |
| mongo | `hello` |
| GenericPVC | volume-mounted / HTTP |
| ClusterObjects | dest Get (CRD Established) |

Swap the engine with a [Plugin](plugins.md) (`backup.mover: velero`) — Portage
still gates `Succeeded` on Ready + probe.

## Next

- [Plugins](plugins.md)
- [Replication & cutover](cutover.md)
- [Configuration](configuration.md)
