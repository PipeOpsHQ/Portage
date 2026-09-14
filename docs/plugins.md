# Plugins

Portage is the control plane. **How** bytes move is a plugin. In-tree movers
(VolSync restic/rclone, postgres-streaming, CSI dump) ship in the hub image.
Anything else — Velero, K8up/restic, a vendor snapshot API — is a **Plugin**
CR that the hub calls over HTTP. Change the CR or `Policy.*.mover` and the
next reconcile uses the new engine. No rebuild, no Go plugin ABI.

## Swap backup and restore

```yaml
apiVersion: portage.io/v1alpha1
kind: Plugin
metadata:
  name: velero
spec:
  type: Mover
  webhookURL: http://portage-velero.portage-system.svc:8080
  classes: [GenericPVC, UnknownStateful]
  backup: true
  restore: true
---
apiVersion: portage.io/v1alpha1
kind: Policy
metadata:
  name: tenant-continuity
spec:
  backup:
    mover: velero     # Plugin name
  restore:
    mover: velero
  replicate:
    mover: volsync    # in-tree
  moverOverrides:
    SQLLogical: postgres-streaming   # wins over backup.mover for this class
```

Selection order:

1. `Policy.spec.moverOverrides[<class>]`
2. `backup.mover` / `restore.mover` / `replicate.mover` (per Action type)
3. In-tree discovery (postgres-streaming, then VolSync)

Empty `backup.mover` keeps the built-in dump + CSI path (kind e2e). Setting
it to a Plugin name **replaces** that path for covered workloads.

## Webhook contract

The hub `POST`s JSON to `spec.webhookURL`:

```json
{
  "operation": "backup",
  "plugin": "velero",
  "workload": {
    "namespace": "tenant-a",
    "name": "data",
    "kind": "PersistentVolumeClaim",
    "class": "GenericPVC",
    "pvcNames": ["data"]
  },
  "source": { "name": "aws" },
  "dest": { "name": "gcp" },
  "artifact": null
}
```

`operation` is one of `backup`, `restore`, `replicate`, `quiesce`, `promote`,
`probe`. Restore includes `"artifact": { "id": "velero/b1", ... }` from the
Backup response.

Response (HTTP 2xx):

```json
{
  "artifact": { "id": "velero/b1", "sizeBytes": 1048576, "useful": true, "message": "" },
  "probe": { "ok": true, "message": "" },
  "error": ""
}
```

Non-2xx or `"error": "..."` fails the Action for that workload. Portage still
requires dest **Ready + class probe** before Restore `Succeeded`.

Timeout: `spec.timeoutSeconds` (default 120). The adapter should wait until
Velero/restic finishes, then return.

## In-tree names

| Name | Role |
|---|---|
| `postgres-streaming` | WAL replica + `pg_promote` |
| `volsync` | ObjectStore restic / Direct rsyncTLS |
| `rclone` | VolSync rclone hop (`moverOverrides.GenericPVC: rclone`) |

A Plugin **must not** reuse those names unless you intend to hide the in-tree
mover (`Get` returns the first registered match — in-tree is registered first).

## Writing an adapter

A tiny HTTP service that creates a Velero `Backup`/`Restore` CR and waits is
enough. Keep it out of `pkg/` (see [adapters](https://github.com/PipeOpsHQ/Portage/blob/main/adapters/README.md)).
The hub never imports Velero.

## Next

- [CRDs](crds.md)
- [Backup & restore](backup-restore.md)
- [Architecture](architecture.md)
