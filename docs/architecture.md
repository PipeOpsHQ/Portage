# Portage architecture

**Portage by PipeOps** is a Kubernetes-native **workload mobility** operator:
classify state, keep a useful copy, optionally keep a warm replica on another
cluster, and cut over only when dest workloads are Ready *and* a class probe
attests the data.

It is not a Velero clone. Backup tools copy objects and bytes. Portage is the
control plane that decides *what* to copy, *how* (plugin movers), *how to
shape* dest manifests (plugin renderers), and *when the job is actually done*.

## Why this exists

- CSI has no portable multi-cloud volume replication. EBS cannot replicate to
  GCE PD. **VolSync** is the storage-agnostic data plane; Portage is the
  orchestrator.
- Velero restore "Completed" means API objects applied. Pods often stay
  Pending (zone topology, StorageClass, `selected-node`), CrashLoop (empty STS
  claim), or Ready on empty data.
- Application-consistent databases need engine-native dumps or streaming
  replicas, not a blind filesystem rsync of `pgdata`.

## Design constraints

1. **Hub orchestrator, plugin movers.** In-tree movers wrap VolSync, CSI
   snapshots, K8up/restic dumps, postgres streaming, etc.
2. **Desired dest shape is rendered, not cloned.** Default renderer is
   `Sanitize` (strip cluster-local fields). PipeOps implements `Webhook`
   from its desired-state control plane; other platforms can too.
3. **Three CRDs:** `ClusterPair`, `Policy`, `Action`.
4. **Completion gate:** `pkg/actionphase.CanSucceed` — stateful workloads
   require `Ready && ProbeOK`.
5. **PipeOps-stewarded, platform-pluggable.** Module
   `github.com/PipeOpsHQ/portage`. Core works on any two clusters. PipeOps
   (and other platforms) integrate via Renderer / TrafficHook / Mover
   plugins — they do not live in `pkg/` as product imports.

## Control plane

```
                 ┌──────────────────────────────┐
                 │     Portage hub controller    │
                 │  Policy / Action reconcilers  │
                 └───────┬────────────┬──────────┘
                         │            │
              classify   │            │  render + apply
              backup     │            │  probe + attest
              replicate  │            │  traffic hook
                         ▼            ▼
                   source cluster   dest cluster
                   (VolSync/K8up/   (rehydrated PVCs,
                    CSI snapshots)    sanitized specs)
```

The hub holds kubeconfigs for both clusters (`ClusterPair`). It does **not**
require an in-cluster agent on day one. VolSync/K8up/CSI already run there.

Transport between clouds is usually **ObjectStore** (rclone hop). Direct
rsync-TLS is optional when clusters can peer.

## CRDs

### ClusterPair (cluster-scoped)

Source + dest cluster refs, transport, StorageClass maps.

### Policy (namespaced)

Selector, backup RPO, replicate RPO, restore auto flag, renderer, optional
mover overrides, traffic hook.

Status: classified `inventory`, artifact usefulness, phase.

### Action (namespaced)

One run: `Backup | Restore | Replicate | Cutover`.

Phases include Preflight, Quiescing, Rehydrating, WaitingReady, Healing,
Attesting. **Succeeded is illegal without probes.**

## Plugins

```go
// pkg/movers.Mover — CSI for data
Discover / Backup / Replicate / Restore / Quiesce / Promote / Probe

// pkg/render.Renderer — dest manifests
Sanitize | Git | Webhook

// pkg/traffic.Hook — DNS / ingress / mesh
Noop | Webhook | out-of-tree
```

Register movers in the hub. First capable mover for a class wins, unless
`Policy.spec.moverOverrides` pins one.

## Classifier

`pkg/classify.Walk` lists STS / Deploy / DaemonSet / leftover PVCs.

| Signal | Class |
|---|---|
| Engine image or CRD catalog hit | SQLLogical, KVLogical, SearchFS, QueueDurable, ObjectStore |
| PVC, unknown image | GenericPVC |
| PVC, no owner | UnknownStateful (alert, still backup) |
| No PVC, unknown image | Stateless |

Unknown is **opt-out**, never opt-in.

## Transform

`pkg/transform` drops `selected-node`, zone labels, cloud LB annotations,
`volumeName`, `clusterIP`, nodeAffinity, Velero restore labels, and remaps
StorageClass. Applied by the Sanitize renderer and as defense in depth for
Webhook/Git renderers.

StatefulSet restore **must** bind the restored PVC by name before the STS is
started. `volumeClaimTemplates` creating a sibling empty claim is a failed
Action, not a healer retry loop.

## Action machines

**Restore:** Preflight (artifact useful? never overwrite newer) → quiesce dest
→ provision dest PVCs → rehydrate → apply rendered objects (PVC then
workload) → start ordered → WaitReady + probe → bounded heal → Attest.

**Cutover:** Warm replicate → freeze source → catch-up (lag=0) → promote dest
→ traffic hook → attest → hold source → reap.

Active-passive only. Eventual-consistency multi-writer (Syncthing) is not a
database path.

## What this repo will not grow

- A new restic, kopia, or rsync implementation
- Storage-native multi-cloud (Dell/Portworx/RBD) as the default path
- Object-clone of live source specs as the restore engine
- Always-on replication of every volume (egress economics are a Policy)

## Roadmap (implementation order)

1. Classifier + `portage inventory` + Policy status
2. Usefulness-gated backup + Restore Action gated on class probes
3. Healers (SC map, topology strip, PVC-by-name) + PVC-from-snapshot rehydrate
4. ClusterPair + VolSync mover
5. Cutover freeze/promote
6. Traffic webhook + attestation
7. Native DB movers (postgres streaming)
8. `Policy.spec.restore.auto`
9. rclone object-store transport as default multi-cloud hop

All nine original waves plus the closing gap are in-tree:

- Dual-cluster `Resolve` (source vs dest kubeclients)
- Object-store dumps (`pg_dumpall` → Store) that survive a cloud boundary
- Dest Sanitize-apply (PVC/STS typed create)
- Postgres standby ConfigMap on dest; cutover rollback unfreezes source
- Dual-client test: dest STS exists and dump is in the store
- Helm chart + manager Deployment + Prometheus metrics

Operational pieces now in-tree:

- SigV4 S3 (`objectstore.NewS3` / AWS_* + PORTAGE_S3_*)
- VolSync `rclone.conf` + rsyncTLS PSK secrets (not rotated)
- Postgres `CREATE ROLE replicator`, source Service, dest `pg_basebackup` Job, STS standby mount
- `hack/kind-e2e.sh` + `.github/workflows/e2e.yaml` (two kind clusters, classify postgres)

Helm: `volsync.enabled=true` pulls the Backube chart.

## Next

- [Contributing](contributing.md)
- [Install](install.md)
