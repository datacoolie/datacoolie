# Contributing

Full guidelines live in [`CONTRIBUTING.md`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/CONTRIBUTING.md)
at the repository root. This page summarises the parts most relevant to
documentation.

## Standard local environment

Use a single `.venv` at the root of the active checkout as the normal working
environment for DataCoolie.

- Run docs, tests, and local validation from that same environment.
- Do not create a separate `.docs-venv` for routine docs work.
- If DataCoolie is nested inside a larger workspace, the root `.venv` can live
   above the `datacoolie/` folder.
- If DataCoolie is later split into its own standalone repository, the same
   convention becomes `<repo>/.venv`.

## Documentation workflow

1. **Edit markdown** under `datacoolie/docs/`.
2. **Build locally**:
   ```powershell
   # activate the root .venv for the current checkout
   # nested workspace example: ..\.venv\Scripts\Activate.ps1
   # standalone repo example: .\.venv\Scripts\Activate.ps1
   poetry install --only main,docs --extras "spark polars db api excel aws iceberg deltalake"
   poetry run mkdocs serve
   ```
   Default local docs port is `8000`. If another project already uses that
   port, run DataCoolie docs on another port, for example:
   ```powershell
   poetry run mkdocs serve -a 127.0.0.1:8001
   ```
3. **Check strict mode passes**:
   ```powershell
   poetry run mkdocs build --strict
   ```
4. Open a PR — the `docs` GitHub Actions workflow runs
   `mkdocs build --strict` on pull requests that touch docs-related files.
   Pushes to `main` for those paths deploy `latest` via `mike`.

## Documentation style

- Follow the **Diátaxis** tier for the page you're editing:
  - `getting-started/` → tutorials (task + learning outcome).
  - `concepts/` → explanation (theory, diagrams).
  - `how-to/` → recipes (prerequisites + end state + steps).
  - `reference/` → information (facts, tables, API signatures).
- Use Material admonitions (`!!! note`, `!!! warning`) for asides.
- Prefer Mermaid over ASCII art.
- Keep code examples **runnable** where feasible — it prevents rot.

## Python docstrings

Public API pages are rendered by `mkdocstrings` from docstrings under
`src/datacoolie/**`.

Use clear Google-style docstrings when you add or substantially rewrite public
API documentation, but note that the current repo Ruff config does **not**
enable `pydocstyle` / `D` rules globally. Missing docstrings do not, by
themselves, fail the build today.

What can break the docs build is malformed docstring content, broken imports,
or API reference pages that no longer match the importable module surface.

## ADRs

We keep ADRs only for load-bearing decisions that affect plugin authors or
external consumers. Before 1.0 you may edit existing ADRs in place as the
design evolves. After 1.0, overturned decisions get a new ADR marked
"Supersedes N" rather than in-place edits.
