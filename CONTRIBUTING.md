# Contributing to Portage by PipeOps

## Developer Certificate of Origin

This project uses the [Developer Certificate of Origin](https://developercertificate.org/).
Every commit must be signed off:

```bash
git commit -s -m "pkg/classify: detect orphan PVCs as UnknownStateful"
```

## Principles

1. **Orchestrate, don't copy bytes.** New data movers belong behind `pkg/movers.Mover`. Do not add a Portage-owned restic/rsync.
2. **PipeOps is the steward, not an import.** Do not import PipeOps control-plane packages into `pkg/` or `api/`. PipeOps integrates through the same Mover / Renderer / TrafficHook plugins as everyone else (`adapters/`).
3. **`Action` Succeeded means Ready + class probe.** Never treat a VolSync/K8up/Velero CR as done.
4. **Unknown + PVC is in the graph.** Skipping unclassified disks is a bug.
5. **Go fmt, tests, race detector.** `make test` must pass.

## Layout

| Path | What belongs there |
|---|---|
| `api/v1alpha1` | CRDs only |
| `pkg/classify` | Workload graph |
| `pkg/movers` | Plugin interface + in-tree movers |
| `pkg/transform` | Cluster-local field stripping |
| `pkg/render` | Dest manifests (Sanitize / Git / Webhook) |
| `pkg/traffic` | Cutover traffic hooks |
| `cmd/portage` | CLI |
| `cmd/controller` | Hub operator |
| `internal/` | CLI wiring, reconcilers — not a public API |

## First useful PRs

See `docs/architecture.md` — classifier, usefulness-gated backup, Restore Action that waits on probes, then VolSync, then cutover.
