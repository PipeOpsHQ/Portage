# Concepts

## Classify first

Portage walks StatefulSets, Deployments, DaemonSets, and leftover PVCs.

| Signal | Class |
|---|---|
| Engine image / CRD catalog | SQLLogical, KVLogical, SearchFS, QueueDurable, ObjectStore |
| PVC, unknown image | GenericPVC |
| PVC, no owner | UnknownStateful (still backed up) |
| No PVC | Stateless (re-render only) |

Unknown + disk is **in** the graph. Skipping it is how you ship 12 KiB of certs
and call it a Postgres backup.

## Useful ≠ Completed

A restic/CSI/Velero job that finished is not a backup. Logical engines need a
dump (or live size) above 64 KiB. CSI `ReadyToUse` alone does **not** pass
Postgres.

`Policy.status.backupHealthy` is false until every stateful workload has a
useful artifact. Portable copies live in the object store (`ArtifactID`).

## Dual cluster

`ClusterPair` holds two kubeconfigs. Actions resolve **source** vs **dest**
clients. Empty dest secret means in-cluster (same API).

Transport:

- `ObjectStore` (default) — dumps + VolSync rclone hop
- `Direct` — VolSync rsyncTLS (clusters must peer)

## Done means Ready + probe

`Action` `Succeeded` is illegal unless every stateful workload is Ready **and**
its class probe passed (`pg_isready`, `PING`, …). That is the Velero trap
Portage exists to close.

## Plugins

| Interface | In-tree |
|---|---|
| `pkg/movers.Mover` | VolSync, rclone transport, postgres-streaming |
| `pkg/render.Renderer` | Sanitize |
| `pkg/traffic.Hook` | Noop, HTTP webhook |
| `pkg/objectstore.Store` | Memory, Dir, SigV4 S3 |
