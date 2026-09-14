# Security

**Portage by PipeOps.** Report vulnerabilities privately to PipeOps security
(do not open a public issue) for a suspected leak of kubeconfig material,
mover credentials, or a restore path that could overwrite live data.

Portage treats kubeconfig Secrets, cloud credential Secrets, and object-store
keys as the trust boundary. Prefer Azure Workload Identity, EKS IRSA, or GKE
Workload Identity on the hub ServiceAccount over long-lived keys in Secrets.
Movers run with the permissions of the identity you bind; do not grant the hub
cluster-admin on tenant clusters if a narrower Role will do.

A restore/cutover Action must never mark `Succeeded` without class probes.
If you find a path that does, that is a security bug (silent empty restore).
