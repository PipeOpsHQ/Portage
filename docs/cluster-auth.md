# Cluster auth

The hub talks to source and dest APIs using `ClusterPair.spec.source` and
`.destination`. Each `ClusterRef` sets **exactly one** auth method, or none
(in-cluster). Two methods on the same ref is an error.

Cloud auth mints short-lived tokens in-process. The hub image does **not**
ship `aws`, `kubelogin`, or `gcloud`. Prefer Workload Identity / IRSA / ADC
on the hub ServiceAccount over keys in a Secret.

| Field | When | Hub identity |
|---|---|---|
| *(omit)* | Hub is that cluster | in-cluster config |
| `kubeconfigSecret` | Kind, air-gap, or a file you already have | Secret (default key `kubeconfig`) |
| `azure` | AKS | Entra ID (`DefaultAzureCredential` or SP) |
| `aws` | EKS | IAM (IRSA / instance role / keys, optional AssumeRole) |
| `gcp` | GKE | ADC / Workload Identity / `key.json` |

`ClusterPair.status.sourceReachable` / `destinationReachable` is a list-namespaces
ping. Failed resolve (bad ARM id, missing `eks:DescribeCluster`, expired keys)
shows on `status.message`.

## Kubeconfig

```yaml
destination:
  name: dst
  kubeconfigSecret:
    name: dest-kubeconfig
    key: kubeconfig          # default
    namespace: portage-system  # default: hub namespace
```

Tokens inside a kubeconfig expire. An exec-plugin kubeconfig (`aws eks get-token`,
`kubelogin`, `gke-gcloud-auth-plugin`) also fails unless those binaries exist
in the hub pod — they do not. Use `azure` / `aws` / `gcp` instead.

## AWS (EKS)

```yaml
destination:
  name: eks-dr
  aws:
    clusterName: prod          # EKS name, not ARN
    region: us-east-1
    roleARN: arn:aws:iam::123456789012:role/portage-eks  # optional AssumeRole
    # endpoint: https://xxxx.gr7.us-east-1.eks.amazonaws.com  # private override
    # credentialsSecret:
    #   name: aws-keys
```

Default credential chain: IRSA, instance role, then env. Optional Secret keys:
`accessKey` / `AWS_ACCESS_KEY_ID`, `secretKey` / `AWS_SECRET_ACCESS_KEY`,
`sessionToken` / `AWS_SESSION_TOKEN`, `roleARN` / `AWS_ROLE_ARN`.

IAM on the hub identity (or `roleARN`):

- `eks:DescribeCluster` on the cluster
- `sts:GetCallerIdentity` (always)
- `sts:AssumeRole` if `roleARN` is set
- Kubernetes RBAC: map the IAM principal in `aws-auth` / EKS access entries

The bearer token is `k8s-aws-v1.` + a presigned STS URL (same as
`aws eks get-token`).

## Azure (AKS)

```yaml
destination:
  name: aks-dr
  azure:
    resourceID: /subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.ContainerService/managedClusters/prod
    # tenantID / clientID: only if not using DefaultAzureCredential
    # usePrivateFQDN: true
    # credentialsSecret:
    #   name: azure-sp
```

`DefaultAzureCredential` covers Azure Workload Identity, managed identity, and
`AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / federated token. Optional Secret keys:
`tenantID`, `clientID`, `clientSecret`.

Azure RBAC on the hub identity:

- `Microsoft.ContainerService/managedClusters/read`
- `Microsoft.ContainerService/managedClusters/listClusterUserCredentials/action`

Then Kubernetes RBAC on the AKS cluster for that Entra identity. Local-account
admin kubeconfigs (client cert) are used as-is when the user kubeconfig has
one; otherwise Portage mints an Entra token for the AKS server app
(`6dae42f8-4368-4678-94ff-3960e28e3630`, override with `serverID`).

## GCP (GKE)

```yaml
destination:
  name: gke-dr
  gcp:
    project: my-project
    location: us-central1    # region or zone
    cluster: prod
    # usePrivateEndpoint: true
    # credentialsSecret:
    #   name: gcp-sa
    #   key: key.json          # default
```

Default: Application Default Credentials (GKE Workload Identity on the hub,
or `GOOGLE_APPLICATION_CREDENTIALS`). Optional Secret: service-account JSON
at `key.json` or `credentials.json`.

GCP IAM on the hub identity:

- `container.clusters.get` (`roles/container.clusterViewer` is enough to fetch
  endpoint + CA)
- Kubernetes RBAC on the GKE cluster for that Google identity

## Private API servers

| Provider | Field |
|---|---|
| AKS | `azure.usePrivateFQDN: true` |
| EKS | `aws.endpoint: https://…` (overrides DescribeCluster) |
| GKE | `gcp.usePrivateEndpoint: true` |

The hub must be able to route to that address (peered VPC, Private Link, etc.).
Auth still uses the same identity; only the Host/CA change.

## Cross-cloud pair

Hub on EKS, dest AKS, dumps in S3:

```yaml
apiVersion: portage.io/v1alpha1
kind: ClusterPair
metadata:
  name: eks-to-aks
spec:
  source:
    name: eks
    # empty auth = this hub cluster
  destination:
    name: aks
    azure:
      resourceID: /subscriptions/…/resourceGroups/rg/providers/Microsoft.ContainerService/managedClusters/dr
    objectStore:
      url: s3://portage-dumps/aks
  transport: ObjectStore
```

## Next

- [CRDs](crds.md)
- [Configuration](configuration.md)
- [Install](install.md)
