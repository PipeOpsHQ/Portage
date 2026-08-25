# Security

**Portage by PipeOps.** Report vulnerabilities privately to PipeOps security
(do not open a public issue) for a suspected leak of kubeconfig material,
mover credentials, or a restore path that could overwrite live data.

Portage treats kubeconfig Secrets and object-store keys as the trust
boundary. Movers run with the permissions of the ServiceAccount you bind;
do not grant the hub cluster-admin on tenant clusters if a narrower Role
will do.

A restore/cutover Action must never mark `Succeeded` without class probes.
If you find a path that does, that is a security bug (silent empty restore).
