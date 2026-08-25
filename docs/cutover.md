# Replication and cutover

## Replicate (warm dest)

- **Postgres:** `CREATE ROLE replicator`, source Service `portage-pg-primary`,
  dest ConfigMap `portage-standby-<name>`, `pg_basebackup -R` Job, STS mount at
  `/etc/portage-standby`. Dest must reach source:5432.
- **Generic PVC:** VolSync `ReplicationSource` / `ReplicationDestination`.
  ObjectStore → rclone (`portage-rclone` secret). Direct → rsyncTLS
  (`portage-rsync-tls` PSK, never rotated).

Replicate `Succeeded` only after VolSync `lastSyncTime` on **source and dest**,
or Postgres `pg_basebackup` complete — not when the CR is applied.

When `clusterObjects.enabled` is set, Replicate also live-syncs the API graph
(create-or-update dest). That is active restoration for ConfigMaps, Secrets,
Services, RBAC, and unknown CRs. Dest Get is the probe; apply-returned is not.

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
