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

| Variable | Purpose |
|---|---|
| `PORTAGE_VOLSYNC_SCHEDULE` | Cron for VolSync source **and dest pull** (default `*/5 * * * *`) |

`ClusterPair.spec.source.address` (`host:port`) is how dest Postgres reaches
source WAL. Empty = in-cluster DNS only.

## Kind e2e

See [E2e](e2e.md). `make e2e` runs the product checks (usefulness, dest probe,
replayed rows, PVC dest bytes, cutover freeze).

## Samples

- `config/samples/portage_v1alpha1_clusterpair.yaml`
- `config/samples/portage_v1alpha1_policy.yaml`
- `config/samples/portage_v1alpha1_action.yaml`

## Next

- [CLI](cli.md)
- [E2e](e2e.md)
- [Architecture](architecture.md)
