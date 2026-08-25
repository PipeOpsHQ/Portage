# Portage by PipeOps

**Asynchronous, class-aware workload mobility for Kubernetes.**

Portage classifies every workload, keeps a *useful* copy of its state, can
warm-replicate to another cluster (including another cloud), and will not call
restore or cutover complete until the destination is **Ready and a class probe
attests the data**.

A portage is carrying a vessel between two waterways. That is the job.

```text
source cluster  ──replicate / restore──►  dest cluster
   (live)            Portage hub              (warm or promoted)
```

## This is not Velero

| | Velero / K8up | Portage |
|---|---|---|
| Unit | backup of objects + volumes | classified *workload* |
| Done | CR `Completed` | Ready **and** `pg_isready` / `PING` |
| Dest specs | restore source objects | **render** (sanitize or webhook) |
| Multi-cloud PVC | full copy after snapshot | VolSync / engine replica / object-store dump |
| Topology | copied (zone pins) | stripped / remapped |

Use Velero for cluster-state backup. Use Portage to move applications and
know they actually came up.

## Three CRDs

- **ClusterPair** — source + dest kubeconfigs, Direct or ObjectStore transport
- **Policy** — backup / replicate / auto-restore / renderer / traffic hook
- **Action** — one `Backup` \| `Restore` \| `Replicate` \| `Cutover`

## Go module

```bash
go get github.com/PipeOpsHQ/portage@v0.1.0
```

[pkg.go.dev/github.com/PipeOpsHQ/portage](https://pkg.go.dev/github.com/PipeOpsHQ/portage)

## Next

- [Install](install.md)
- [Concepts](concepts.md)
- [Architecture](architecture.md)
