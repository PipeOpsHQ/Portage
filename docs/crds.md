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
  destination:
    name: gcp
    kubeconfigSecret:
      name: gcp-kubeconfig
      key: kubeconfig
    objectStore:
      url: s3://portage-dumps/gcp
  transport: ObjectStore
  storageClassMap:
    gp3: standard-csi
```

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
  replicate:
    enabled: true
    rpo: 15m
  restore:
    auto: false
    neverOverwriteNewer: true
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

- [Backup & restore](backup-restore.md)
- [Replication & cutover](cutover.md)
