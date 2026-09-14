# CRDs

API group: `portage.io/v1alpha1`.

## ClusterPair (cluster-scoped)

Source and destination cluster refs, `Direct` or `ObjectStore` transport,
StorageClass maps.

```yaml
apiVersion: portage.io/v1alpha1
kind: ClusterPair
metadata:
  name: aws-to-gcp
spec:
  source:
    name: aws
    address: 192.0.2.10:30432   # dest WAL / NodePort; empty = in-cluster DNS
    aws:
      clusterName: prod
      region: us-east-1
  destination:
    name: gcp
    gcp:
      project: my-project
      location: us-central1
      cluster: dr
    objectStore:
      url: s3://portage-dumps/gcp
  transport: ObjectStore
  storageClassMap:
    gp3: standard-csi
```

Each `ClusterRef` uses **exactly one** auth method (or none = in-cluster):

| Field | Identity |
|---|---|
| *(empty)* | Hub cluster (in-cluster config) |
| `kubeconfigSecret` | Static kubeconfig in a Secret |
| `azure` | Entra ID → AKS (`DefaultAzureCredential` or SP secret) |
| `aws` | IAM → EKS token (`IRSA` / instance role / keys, optional `roleARN`) |
| `gcp` | ADC / Workload Identity / `key.json` → GKE |

Do not set two methods on the same ref. Full YAML, IAM, and private-endpoint
notes: [Cluster auth](cluster-auth.md).

## Plugin (cluster-scoped)

Hot-swappable mover. The hub POSTs Backup/Restore/Replicate JSON to
`webhookURL`. Select it with `Policy.spec.backup.mover` (the Plugin name).

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
```

See [Plugins](plugins.md).

## Policy (namespaced)

Desired continuity for a selector.

```yaml
apiVersion: portage.io/v1alpha1
kind: Policy
metadata:
  name: tenant-continuity
  namespace: tenant-a
spec:
  clusterPair: aws-to-gcp
  selector:
    namespaces: [tenant-a]
  backup:
    enabled: true
    rpo: 24h
    requireUseful: true
    mover: velero            # Plugin name; empty = dump + CSI
  replicate:
    enabled: true
    rpo: 15m
    mover: volsync           # or a Plugin
  restore:
    auto: false
    neverOverwriteNewer: true
    mover: velero
  renderer:
    kind: Sanitize   # Sanitize | Git | Webhook
  clusterObjects:
    enabled: true              # live API graph (not etcd); CRDs always
    includeClusterScoped: true # Namespaces, ClusterRoles, cluster-scoped CRs
    excludeNamespaces: []
  cutover:
    trafficHook: https://hooks.example.com/portage/switch
    holdSource: 24h
```

Status: `inventory`, `artifacts` (including `artifactID`), `backupHealthy`.

## Action (namespaced)

One run. Types: `Backup`, `Restore`, `Replicate`, `Cutover`.

```yaml
apiVersion: portage.io/v1alpha1
kind: Action
metadata:
  name: backup-tenant-a
  namespace: tenant-a
spec:
  type: Backup
  policyRef: tenant-continuity
```

Cutover failback:

```yaml
spec:
  type: Cutover
  policyRef: tenant-continuity
  rollback: true
```

`status.phase` is `Succeeded` only after Ready + class probe. Dry-run stops
after preflight.

## Next

- [Cluster auth](cluster-auth.md)
- [Backup & restore](backup-restore.md)
- [Replication & cutover](cutover.md)
