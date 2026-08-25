# Changelog

## Unreleased

### Added

- `Policy.spec.clusterObjects`: live API-graph backup / replicate / restore
  (dest Get is the probe; not an etcd dump)

## [0.1.0] - 2026-08-25

### Added

- CRDs: ClusterPair, Policy, Action
- Classifier, usefulness-gated backup, restore/cutover probe gates
- Dual-cluster clients, object-store dumps, dest Sanitize-apply
- VolSync rclone.conf + rsyncTLS PSK secrets
- Postgres replicator role, source Service, dest pg_basebackup Job
- SigV4 S3 store, Helm chart, kind two-cluster e2e
- CLI `portage inventory`, GitHub Pages docs at https://pipeopshq.github.io/Portage/
