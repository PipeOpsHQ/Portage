# Contributing

See [CONTRIBUTING.md](https://github.com/PipeOpsHQ/Portage/blob/main/CONTRIBUTING.md)
in the repo. DCO sign-off (`git commit -s`) is required.

```bash
make test
make build
```

Do not import PipeOps control-plane packages into `pkg/` or `api/`. Adapters
implement Mover / Renderer / TrafficHook.

## Next

Back to [Install](install.md) or the [home page](index.md).
