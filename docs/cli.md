# CLI

```bash
go install github.com/PipeOpsHQ/portage/cmd/portage@v0.1.0
portage --help
```

| Command | Purpose |
|---|---|
| `portage inventory -n <ns>` | Classify workloads (`table` or `-o json`) |
| `portage inventory -A` | All namespaces |
| `portage version` | Build metadata |
| `portage completion bash\|zsh\|fish\|powershell` | Shell completion |

Global flags: `--kubeconfig`, `--context`, `-n/--namespace`.

## Next

- [Architecture](architecture.md)
- [Contributing](contributing.md)
