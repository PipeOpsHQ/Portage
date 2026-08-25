# Configuration

## Controller flags

| Flag | Default |
|---|---|
| `--metrics-bind-address` | `:8080` |
| `--health-probe-bind-address` | `:8081` |
| `--leader-elect` | `true` |

Metrics: `portage_backup_useful{policy,namespace}`,
`portage_actions_total{type,phase}`.

## Environment

See [Install](install.md) for object-store variables.

## Kind e2e

```bash
bash hack/kind-e2e.sh
E2E_FULL=1 bash hack/kind-e2e.sh   # also helm-install VolSync
```

Two kind clusters, CRDs, a Postgres StatefulSet, `portage inventory` must
classify it.

## Samples

- `config/samples/portage_v1alpha1_clusterpair.yaml`
- `config/samples/portage_v1alpha1_policy.yaml`
- `config/samples/portage_v1alpha1_action.yaml`
