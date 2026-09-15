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
a branch” (`main` `/docs`), Jekyll overrides the MkDocs site (generator
Jekyll v3, no Material theme). The docs workflow PUTs `build_type=workflow`
and fails if Pages is still legacy.

Do not import PipeOps control-plane packages into `pkg/` or `api/`. Adapters
implement Mover / Renderer / TrafficHook.

## Releases

Push to `main` cuts a GitHub Release when there is a `feat`, `fix`, `perf`,
or `e2e` commit since the last `v*` tag (`hack/next-version.sh`). `feat` →
minor, `fix` → patch, `feat!` / `BREAKING CHANGE:` → major. `docs:`,
`chore:`, `ci:`, and `test:` do not bump.

That run tags `vX.Y.Z`, GoReleaser publishes binaries, and the image is
`ghcr.io/pipeopshq/portage:vX.Y.Z` (+ `:latest`). A manual `git push origin vX.Y.Z`
still works. `docs:`-only pushes do not cut a release.

## Next

Back to [Install](install.md) or the [home page](index.md).
