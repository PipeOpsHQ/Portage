# Portage by PipeOps

[![CI](https://github.com/PipeOpsHQ/Portage/actions/workflows/ci.yaml/badge.svg)](https://github.com/PipeOpsHQ/Portage/actions/workflows/ci.yaml)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-teal)](https://pipeopshq.github.io/Portage/)
[![Go Reference](https://pkg.go.dev/badge/github.com/PipeOpsHQ/portage.svg)](https://pkg.go.dev/github.com/PipeOpsHQ/portage)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/PipeOpsHQ/Portage)](https://github.com/PipeOpsHQ/Portage/releases)

**Asynchronous, class-aware workload mobility for Kubernetes.**

Portage is an open source project by [PipeOps](https://pipeops.io). It
classifies every workload in a namespace, keeps a *useful* copy of its state,
can maintain a warm replica on another cluster (including another cloud), and
will not call restore or cutover complete until the destination is **Ready and
a class probe attests the data**.

A portage is carrying a vessel between two waterways. That is the job.

Anyone with two kubeconfigs can run it. PipeOps is the steward and the
reference platform (desired-state renderer + traffic switch); other platforms
plug in the same way.

```text
source cluster  ──replicate / restore──►  dest cluster
   (live)            Portage hub              (warm or promoted)
```

## This is not Velero

| | Velero / K8up | Portage |
|---|---|---|
| Unit | backup of objects + volumes | classified *workload* |
| Done | CR `Completed` | Ready **and** `pg_isready` / `PING` / mount probe |
| Dest specs | restore source objects | **render** (sanitize or webhook) |
| Multi-cloud PVC | full copy after snapshot | VolSync / engine replica as plugins |
| Topology | copied (zone pins, `selected-node`) | stripped / remapped |
| Unclassified PVC | easy to miss | **in the graph**, alerted |

Use Velero for cluster-state backup if you want it. Use Portage to move
*applications* and know they actually came up.

## Documentation

**[pipeopshq.github.io/Portage](https://pipeopshq.github.io/Portage/)** — install, CRDs, backup/restore, cutover, architecture.

Go module: `go get github.com/PipeOpsHQ/portage@v0.1.0`  
Binaries: [GitHub Releases](https://github.com/PipeOpsHQ/Portage/releases)  
Image: `ghcr.io/pipeopshq/portage:v0.1.0`

## Status

v0.1.0 — operator, CLI, dual-cluster clients, object-store dumps, dest apply,
VolSync secrets, Postgres standby Job, kind e2e. See
[docs](https://pipeopshq.github.io/Portage/).

## CRDs

Three kinds, on purpose:

- **`ClusterPair`** — two clusters, transport (`Direct` \| `ObjectStore`), StorageClass maps
- **`Policy`** — what to back up, replicate, auto-restore, how to render dest manifests
- **`Action`** — one run (`Backup` \| `Restore` \| `Replicate` \| `Cutover`)

## Quick start (classifier)

```bash
make build
./bin/portage inventory -n some-namespace
./bin/portage inventory -n some-namespace -o json
```

Install CRDs (after `make manifests`):

```bash
kubectl apply -k config/crd
kubectl apply -f config/samples/portage_v1alpha1_policy.yaml
```

The controller writes `Policy.status.inventory` with a class per workload.

## Backup and restore (this slice)

A backup is **not** healthy because a Job completed. `Policy.status.backupHealthy`
is true only when every stateful workload has a **useful** artifact (size floor
and dump shape — a 12 KiB certs-only Postgres snapshot is a failure).

A Restore `Action` is **not** `Succeeded` because a mover finished. The
controller execs the class probe (`pg_isready`, `PING`, …) and will sit in
`WaitingReady` until that passes.

Healers strip zone pins and remap StorageClass; missing PVCs are recreated
**by original name** from a CSI VolumeSnapshot (`dataSource`).

`ClusterPair` + VolSync (rsyncTLS or rclone object-store hop) keep a warm
replica. `Action` `type: Cutover` freezes the source, waits lag=0, promotes
(Postgres `pg_promote` when that mover applies), fires the traffic webhook,
then attests. `Policy.spec.restore.auto` creates a Restore Action only when
backups are useful and a PVC is gone.

```bash
kubectl apply -f config/samples/portage_v1alpha1_policy.yaml
kubectl apply -f config/samples/portage_v1alpha1_action.yaml
# spec.type: Backup | Restore
```

## Plugins (extension points)

Portage is CSI for mobility. Plugins keep the operator usable without
PipeOps, and they are how PipeOps itself integrates.

| Interface | Package | In-tree |
|---|---|---|
| Data path | `pkg/movers.Mover` | (VolSync, CSI snapshot, engine dumps — coming) |
| Dest manifests | `pkg/render.Renderer` | `Sanitize` |
| Traffic switch | `pkg/traffic.Hook` | `Noop`, HTTP `Webhook` |

PipeOps (and any other control plane that already knows desired state)
implements `Renderer` as `Webhook` rather than cloning live objects. Sanitize
is the default so Portage still works on any two kubeconfigs with no extra
product.

## Principles

1. Orchestrate existing CNCF data planes. Do not write another rsync.
2. Unknown + PVC is still backed up.
3. `Completed` from a backup CR is not success.
4. Apache-2.0, DCO, Kubernetes-style CRDs. PipeOps-the-product is an adapter, not an import.

Install the hub:

```bash
kubectl apply -k config/default
# or: helm install portage charts/portage
```

## Build

```bash
go 1.24+

make test
make build
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

Copyright 2026 PipeOps and the Portage Authors. **Portage by PipeOps.**

Docs: [pipeopshq.github.io/Portage](https://pipeopshq.github.io/Portage/)
