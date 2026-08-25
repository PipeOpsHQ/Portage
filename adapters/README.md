# Adapters

**Portage by PipeOps** — core (`api/`, `pkg/`, `cmd/`) has no product imports.
PipeOps is the first-party platform adapter and lives out of tree (or in a
submodule here later), implementing the same contracts as any other consumer:

| Contract | Where |
|---|---|
| `pkg/movers.Mover` | extra engines, storage-native replication |
| `pkg/render.Renderer` (`Webhook` / `Git`) | dest manifests from desired state, not live clone |
| `pkg/traffic.Hook` | DNS, ingress, service mesh, platform router |
| `classify.Register` | extra image/CRD engines |

Keep the PipeOps adapter (desired-state render, router/DNS cutover) in a
module that *depends on* `github.com/PipeOpsHQ/portage`, not the other way
around. Other platforms do the same.
