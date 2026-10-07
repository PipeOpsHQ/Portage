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
  Dest restic does not set `fsGroup`. Dest workloads drop `fsGroup: 0`,
  and Postgres drops any `fsGroup`, so the app pod's mount does not chmod
  restored PGDATA `g+rw` (Postgres requires `server.key` mode 0600).

`Policy.spec.replicate` syncs data and ancillary objects, not workload
manifests. Deploying Deployment/StatefulSet specs onto dest is a Restore
Action (`Sanitize` renderer). Restore remaps `spec.runtimeClassName` with
`ClusterPair.spec.runtimeClassMap`; unmapped classes are stripped so dest
does not reject a RuntimeClass that exists only on source.

Replicate is a **live loop**, not a one-shot. The Action stays `CatchingUp` and
re-attests dest (Ready + probe, dest Get for objects). `Policy.spec.replicate.enabled`
keeps one `replicate-<policy>` Action running. A ClusterPair with dest
kubeconfig/cloud auth **must** resolve dest; Portage will not write
ReplicationDestination on the source cluster. Restic movers copy the workload
`fsGroup`/`runAsUser` into `moverSecurityContext` on source. Dest restic
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
