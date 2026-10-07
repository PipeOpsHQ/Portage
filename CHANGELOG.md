# Changelog

## Unreleased

### Added

- `Policy.spec.clusterObjects.excludeGVKs` skips matching CRDs and CRs
  (Gateway API experimental channel, dest admission, version skew).

### Fixed

- Dest workloads drop `fsGroup: 0`, and Postgres drops any `fsGroup`.
  Kubelet ORs `0660` onto the volume when the dest pod mounts, which is
  after restic has written `server.key` as `0600`. Clearing it only on
  the mover left the restored Postgres pod in CrashLoopBackOff. Dest
  restic no longer sets `fsGroup` at all (runAsUser stays the engine UID).
- A NotReady Node object whose name is a source hostname no longer counts
  as a dest node. VolSync Direct resolves `spec.nodeName` of the pod that
  mounts the PVC and will pin the mover to that ghost.
- Dest Replicate deletes dest user pods (not only mover Jobs) whose
  `kubernetes.io/hostname` pin is not a dest node. VolSync Direct
  AffinityFromVolume copies that pin from a Pending dest STS pod onto the
  next mover Job, which is why deleting the Job alone did not stick.
- Dest restic `moverSecurityContext` no longer sets `fsGroup: 0`. Kubelet
  was chmod-ing restored PGDATA `g+rw`, and Postgres then refused
  `server.key` (`u=rw (0600)`). Dest movers restore as the engine UID
  (999 for Postgres/Redis). `fsGroup: 0` still enables privileged movers
  (DAC_OVERRIDE) so source restic can read mode 700 data.
- Dest-local K8up/Velero no longer mount VolSync restic cache PVCs
  (`volsync-*-cache`, including legacy `volsync-portage-*-cache`).
  Replicate/Backup label those claims `velero.io/exclude-from-backup=true`,
  annotate `k8up.io/backup=false`, and delete backup Jobs already attached
  so dest movers are not stuck Multi-Attach. Cluster-object sync skips
  `k8up.io` / `velero.io` / `stash.appscode.com` so dest backup Schedules
  stay dest-local. User replica PVCs remain backupable.
- Logical dumps read `POSTGRES_USER`/`POSTGRES_DB` and `REDIS_PASSWORD`
  from the container environment (marketplace images do not use the
  engine defaults). Redis dumps to a temp file then stdout so
  `redis-cli --rdb /dev/stdout` fsync-on-pipe no longer fails a successful
  transfer.
- Generic PVC Backup uses the VolSync restic ReplicationSource (bytes
  processed / `lastSyncTime`) instead of returning "volsync is replicate,
  not backup" and depending on CSI snapshots or a live pod.
- Restore preflight skips a workload that lacks a useful artifact and
  restores the rest of the namespace; the Action fails only when nothing
  is restorable.
- `excludeGVKs` CRD-name, `group/kind`, `group/version/kind`, and resource
  forms all match the CRD object and its instances (including empty
  discovery Kind and GatewayClass → gatewayclasses).
- Dest Replicate strips source `kubernetes.io/hostname` pins from dest
  STS/Deploy/DS and deletes dest VolSync mover Jobs pinned to a hostname
  that does not exist on dest. Probe reports the pin if it is re-injected.
- Restore no longer patches Bound dest PVCs (VolSync already provisioned
  them). Clearing `spec.volumeName` was rejected as immutable and failed
  preflight whenever Replicate was already running.
- Restore updates existing dest StatefulSet/Deployment specs so
  RuntimeClass / nodeSelector sanitization actually lands.
- Dest VolSync restic/rclone/rsyncTLS sets empty `moverTolerations`;
  Sanitize strips hostname nodeSelector and gvisor RuntimeClass
  tolerations so Direct movers are not pinned to source-only nodes.
- Cluster-object sync retries dest Update on 409 Conflict (stale
  resourceVersion) instead of failing the CRD pass.
- Sanitize maps `spec.runtimeClassName` via `ClusterPair.spec.runtimeClassMap`
  (pod, workload template, CronJob). Unmapped classes are stripped so dest
  admission does not reject gvisor/kata/WASM/nvidia that exist only on source.
- Restic movers set `volsync.backube/privileged-movers=true` on the namespace
  when the workload declares neither `fsGroup` nor `runAsUser`, so VolSync
  grants `DAC_OVERRIDE` for marketplace images that own PVC data (mode 700,
  UID 999) via capabilities instead of a pod security context.
- Object-graph sync skips dest-local `ServiceCIDR` / `IPAddress` (immutable
  cluster CIDR). Applying source `ServiceCIDR/kubernetes` onto dest used to
  fail the whole pass, so namespaces never landed.
- Replicate provisions dest PVCs (source size, `storageClassMap`) before
  VolSync `destinationPVC`, uses realized StatefulSet claim names
  (`<template>-<sts>-<ordinal>`), and does not report Stateless Ready unless
  dest exists.
- Dual-cluster Replicate no longer falls back dest→source on Resolve
  failure (that wrote ReplicationDestination on the source cluster).
  Misplaced VolSync CRs are deleted; cluster-object sync skips VolSync
  and CapsuleConfiguration; source deletions Delete dest instead of
  Update; restic `moverSecurityContext` copies the workload fsGroup.

## [0.2.0] - 2026-09-15

### Added

- `Policy.spec.clusterObjects`: live API-graph backup / replicate / restore
  (dest Get is the probe; not an etcd dump)
- Live Replicate: Action stays CatchingUp and re-attests dest; Policy
  `replicate.enabled` keeps one `replicate-<policy>` Action running
- VolSync ObjectStore uses restic (incremental) and dest schedule-pull;
  `ClusterRef.address` for dest→source Postgres WAL; e2e asserts dest PVC bytes
- Kind e2e installs VolumeSnapshot CRDs (VolSync requires them even for Direct),
  copies restic secrets to dest, and serves MinIO on the src kind node IP
- `ClusterRef` azure / aws / gcp auth (Entra ID, IAM, ADC) in addition to kubeconfig
- `Plugin` CR + `Policy.spec.backup/restore/replicate.mover` to hot-swap
  Velero, restic, or any webhook mover without rebuilding the hub
- Kind e2e pulls MinIO from `quay.io/minio` (Docker Hub `minio/minio` is gone)

### Fixed

- Classifier skips VolSync cache/clone PVCs so restic does not nest
  ReplicationSources (cache-of-cache) and starve the user PVC sync

### Docs

- Classifier, cutover, CLI, and e2e pages document the VolSync cache skip
  and that ObjectStore defaults to restic (rclone is an override)
- Push to `main` with `feat`/`fix` since the last tag cuts a GitHub Release
  (GoReleaser + `ghcr.io/pipeopshq/portage:<tag>`). `docs:`/`chore:`/`ci:` do not.

## [0.1.0] - 2026-08-25

### Added

- CRDs: ClusterPair, Policy, Action
- Classifier, usefulness-gated backup, restore/cutover probe gates
- Dual-cluster clients, object-store dumps, dest Sanitize-apply
- VolSync rclone.conf + rsyncTLS PSK secrets
- Postgres replicator role, source Service, dest pg_basebackup Job
- SigV4 S3 store, Helm chart, kind two-cluster e2e
- CLI `portage inventory`, GitHub Pages docs at https://pipeopshq.github.io/Portage/
