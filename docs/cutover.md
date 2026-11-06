# Replication and cutover

## Replicate (warm dest)

- **Postgres:** `CREATE ROLE replicator`, source Service `portage-pg-primary`,
  dest ConfigMap `portage-standby-<name>`, `pg_basebackup -R` Job, STS mount at
  `/etc/portage-standby`. Dest must reach source:5432.
- **Generic PVC:** VolSync `ReplicationSource` / `ReplicationDestination`.
  ObjectStore → **restic** (chunked incremental). Each workload gets its own
  repository `<ClusterPair URL>/<namespace>/<name>` and secret
  `portage-restic-<workload>`. One password per namespace against one
  shared URL made the next tenant fail with `wrong password or no key found`.
  Dest **schedule-pulls** (a one-shot manual trigger was the live-sync hole).
  The ReplicationDestination is applied only after the ReplicationSource
  has `status.lastSyncTime`. Creating both at once makes each mover
  `restic init` the empty repository, and the second write leaves the
  key unreadable (`ciphertext verification failed`).
  `copyMethod: Direct` unless `ClusterPair.spec.snapshotClassMap` is set
  (CSI Snapshot). Override `Policy.spec.moverOverrides.GenericPVC: rclone`
  for the old rclone hop. Direct transport → rsyncTLS (`portage-rsync-tls` PSK).
  The classifier does **not** inventory VolSync cache PVCs
  (`volsync-src-*-cache`, `volsync-dst-*-cache`, legacy
  `volsync-<owner>-cache`). Treating them as workloads creates nested
  ReplicationSources and the user PVC never syncs. Replicate marks those
  claims so dest-local K8up/Velero skip them (`k8up.io/backup=false`,
  `velero.io/exclude-from-backup=true`) and deletes a backup Job that is
  already attached, which otherwise Multi-Attaches the RWO cache and
  leaves the dest mover in ContainerCreating.
  Replicate creates the dest PVC (same name, size from source, StorageClass
  via `ClusterPair.spec.storageClassMap`) before setting VolSync
  `destinationPVC`. StatefulSet claims use the realized name
  `<template>-<sts>-<ordinal>`, not the volumeClaimTemplate name.
  Stateless workloads report dest Ready only if the dest object exists and
  is Ready — they are not auto-true.
  Restore against a dest that Replicate already Bound skips re-applying PVC
  spec (`volumeName` is immutable). Dest VolSync Direct movers copy
  dest-pod scheduling; Replicate strips dest STS/Deploy hostname
  `nodeSelector`, deletes dest pods that still carry a source-only
  hostname (AffinityFromVolume copies those onto the next mover), and
  deletes dest mover Jobs pinned to a hostname that is not a dest node.
  Probe reports the pin if GitOps re-injects it.
  Dest restic does not set `fsGroup`. Sanitize drops `fsGroup: 0` on every
  engine, and any `fsGroup` on Postgres, before the dest object is created.
  Heal still clears the template afterwards. That order matters: kubelet
  ORs `0660` onto files already on the volume at the first mount, and
  clearing the template later does not chmod `server.key` back to `0600`.
  Cold Replicate fills the dest PVC before Restore, so the first pod is
  mounting files restic already wrote.
  While a Running user pod mounts the dest PVC, the ReplicationDestination
  is paused and its mover Job is deleted. Direct copyMethod would otherwise
  schedule the mover onto that same node and overwrite live PGDATA
  (`postmaster.pid` then contains the source PID). Replication does not
  copy new bytes once that pod is Running, and Cutover refuses to promote
  from a paused dest. Cutover is cold Replicate into an empty dest PVC,
  then Restore.
  A source PVC that is gone drops its ReplicationSource and
  ReplicationDestination. Leftover pairs whose workload left the inventory
  are pruned on the next Replicate reconcile.

`Policy.spec.replicate` syncs data and ancillary objects, not workload
manifests. Deploying Deployment/StatefulSet specs onto dest is a Restore
Action (`Sanitize` renderer). Restore remaps `spec.runtimeClassName` with
`ClusterPair.spec.runtimeClassMap`; unmapped classes are stripped so dest
does not reject a RuntimeClass that exists only on source.

Replicate is a **live loop**, not a one-shot. The Action stays `CatchingUp` and
re-attests dest (Ready + probe, dest Get for objects). `Policy.spec.replicate.enabled`
keeps one `replicate-<policy>` Action running. Setting `enabled: false` deletes
that reserved Action even when it has no `portage.io/live-replica` label, so
the loop stops. A Replicate Action you created yourself under a different name
is left in place. Backup and Restore reconcile on their own queue, and a new
Action is logged as Pending before its status write. Failure backoff caps at
30s. A ClusterPair with dest
kubeconfig/cloud auth **must** resolve dest; Portage will not write
ReplicationDestination on the source cluster. Restic movers copy the workload
`runAsUser` and any non-zero `fsGroup` (always with
`fsGroupChangePolicy: OnRootMismatch`) into `moverSecurityContext` on
source. `fsGroup: 0` is not copied, because root movers do not need it. Dest restic
sets `runAsUser` only. Dest pods drop `fsGroup: 0` (Postgres drops any
`fsGroup`) so kubelet does not chmod PGDATA `g+rw` when the app mounts.
Workloads that declare neither (or only `fsGroup: 0`) get
the namespace annotation `volsync.backube/privileged-movers=true` so VolSync
grants `DAC_OVERRIDE`.

`Succeeded` is only for dry-run. Dest in sync is `CatchingUp` with
`replica lag=0; dest probed; live-sync`. Lag or dest miss stays CatchingUp
until dest attests — it does not freeze as Succeeded and drift.

When `clusterObjects.enabled` is set, each reconcile live-lists source and
create-or-update dest. That is active restoration for ConfigMaps, Secrets,
Services, RBAC, CRDs, and unknown CRs.

Install VolSync on both clusters (Helm subchart `volsync.enabled=true`, or
`make e2e`, which installs the Backube chart when `helm` is on `PATH`).

## Cutover

```
Preflight (source Postgres must survive a restart; nothing frozen yet)
  → freeze source (replicas=0, remember original)        ┐
  → lag=0 (post-freeze sync landed on dest)              │ freezeTimeout
  → promote dest (pg_promote / scale up)                 │ (default 30m)
  → traffic webhook POST { action: switch }              ┘
  → every inventory workload Ready on dest + class probe   readyTimeout (default 15m)
  → Succeeded
```

**lag=0** means:

- Postgres streaming standby: `pg_is_in_recovery()` and no received WAL
  left to replay. A dest that is not a standby (a restored primary) is not
  "lag 0". It falls through to the VolSync check.
- VolSync: source and dest `lastSyncTime` are both after the freeze. With
  restic, dest is also not behind source. A ReplicationDestination with
  `spec.paused` (Replicate pauses it while a running dest pod mounts the
  PVC) cannot catch up. The Cutover aborts before promote and unfreezes
  source, with the reason in status. Scale the dest workload to 0, or cut
  over into a dest that has never run a pod, and retry.

**Source restart preflight.** Cutover restarts the source (freeze, then
unfreeze on rollback or abort). For each source Postgres that sets
`fsGroup`, preflight execs into the running pod and refuses unless all of
these hold:

- `fsGroupChangePolicy: OnRootMismatch`. `Always`, the default, rewrites
  the whole volume on every mount.
- The data volume root has gid = `fsGroup`, `g+rwx`, and setgid, so
  kubelet skips the rewrite. This is not checked for `subPath` mounts,
  where the root is not visible.
- `server.key` has no group or world access (`0600`, or `0640` if owned
  by root), or `ssl` is off.

The message names the failing rule. A typical repair is `chmod 600` on the
key.

**Timeouts.** `Action.spec.freezeTimeout` covers freeze through switch.
On expiry the Action fails and source replicas are restored: traffic never
moved. `Policy.spec.cutover.readyTimeout` covers `WaitingReady`. On expiry
the Action fails naming the blocking workload. Traffic stays on dest and
source stays frozen, because dest may already have taken writes. Decide,
then fail back with `spec.rollback`.

Stateless workloads are not frozen. They must be Ready on dest (deployed
by GitOps or Restore) for the Cutover to succeed.

Failback, either patched onto the existing Action (any phase, including
`Succeeded` and `Failed`) or set on a new one:

```yaml
spec:
  type: Cutover
  policyRef: tenant-continuity
  rollback: true
```

POSTs `{ action: rollback }`, then restores the source replica counts the
freeze recorded. Workloads that were never frozen are not touched. If
traffic may already be on dest and the hook fails, source stays frozen
(two writable primaries is worse) and the rollback retries every 10s.

Traffic hook is an HTTP webhook (`Policy.spec.cutover.trafficHook`). PipeOps
router/DNS is an out-of-tree implementation of `pkg/traffic.Hook`.

## Next

- [Configuration](configuration.md)
- [CLI](cli.md)
