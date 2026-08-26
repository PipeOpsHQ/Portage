# Replication and cutover

## Replicate (warm dest)

- **Postgres:** `CREATE ROLE replicator`, source Service `portage-pg-primary`,
  dest ConfigMap `portage-standby-<name>`, `pg_basebackup -R` Job, STS mount at
  `/etc/portage-standby`. Dest must reach source:5432.
- **Generic PVC:** VolSync `ReplicationSource` / `ReplicationDestination`.
  ObjectStore → **restic** (chunked incremental; `portage-restic` secret).
  Dest **schedule-pulls** (a one-shot manual trigger was the live-sync hole).
  `copyMethod: Direct` unless `ClusterPair.spec.snapshotClassMap` is set
  (CSI Snapshot). Override `Policy.spec.moverOverrides.GenericPVC: rclone`
  for the old rclone hop. Direct transport → rsyncTLS (`portage-rsync-tls` PSK).

Replicate is a **live loop**, not a one-shot. The Action stays `CatchingUp` and
re-attests dest (Ready + probe, dest Get for objects). `Policy.spec.replicate.enabled`
keeps one `replicate-<policy>` Action running.

`Succeeded` is only for dry-run. Dest in sync is `CatchingUp` with
`replica lag=0; dest probed; live-sync`. Lag or dest miss stays CatchingUp
until dest attests — it does not freeze as Succeeded and drift.

When `clusterObjects.enabled` is set, each reconcile live-lists source and
create-or-update dest. That is active restoration for ConfigMaps, Secrets,
Services, RBAC, CRDs, and unknown CRs.

Install VolSync on both clusters (Helm subchart `volsync.enabled=true` or
`E2E_FULL=1 bash hack/kind-e2e.sh`).

## Cutover

```
Freeze source (replicas=0, remember original)
  → lag=0
  → promote dest (pg_promote / scale up)
  → traffic webhook POST { action: switch }
  → dest Ready + probe
  → Succeeded
```

Failback:

```yaml
spec:
  type: Cutover
  policyRef: tenant-continuity
  rollback: true
```

Unfreezes source and POSTs `{ action: rollback }`.

Traffic hook is an HTTP webhook (`Policy.spec.cutover.trafficHook`). PipeOps
router/DNS is an out-of-tree implementation of `pkg/traffic.Hook`.

## Next

- [Configuration](configuration.md)
- [CLI](cli.md)
