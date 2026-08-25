# Contributing

See [CONTRIBUTING.md](https://github.com/PipeOpsHQ/Portage/blob/main/CONTRIBUTING.md)
in the repo. DCO sign-off (`git commit -s`) is required.

```bash
make test
make build
make docs    # mkdocs serve — site is https://pipeopshq.github.io/Portage/
```

Edit Markdown under `docs/` and `mkdocs.yml`. CI builds with
`mkdocs build --strict` (`.github/workflows/docs.yaml`) and deploys GitHub
Pages via `deploy-pages@v4`.

Pages source must be **GitHub Actions**. If Settings → Pages is “Deploy from
a branch” (`main` `/docs`), Jekyll overrides the MkDocs site. The docs
workflow runs `configure-pages` so Actions stays the source.

Do not import PipeOps control-plane packages into `pkg/` or `api/`. Adapters
implement Mover / Renderer / TrafficHook.

## Next

Back to [Install](install.md) or the [home page](index.md).
