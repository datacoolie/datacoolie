---
title: Contributing Guide for DataCoolie
description: "Contributor guide for DataCoolie: project setup, code quality rules, ADR conventions, documentation workflow, and how to submit changes."
---

# Contributing

Full guidelines live in [`CONTRIBUTING.md`](https://github.com/datacoolie/datacoolie/blob/main/CONTRIBUTING.md)
at the repository root, including contribution terms and commit sign-off. This
page covers checkout setup, documentation and choosing validation checks.

## Standard local environment

All commands below run from the **product root**: the directory containing
`pyproject.toml`, `properdocs.yml`, `src/`, `docs/` and `usecase-sim/`. In a
workspace containing this product under `datacoolie/`, change into that folder
first. In a standalone clone, the clone root is already the product root.

Use the existing checkout-root `.venv` for docs, tests and local validation.
It may be one level above the product in a larger workspace. Do not create a
second environment when that one already exists. The CI baseline uses Python
3.11 and Poetry 2.3.4; the package's Python range is declared in `pyproject.toml`.

For a new standalone clone on Windows, create and activate the environment once:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

For an existing nested workspace, activate its shared environment from the
product root instead: `..\.venv\Scripts\Activate.ps1`. On Linux/macOS use
`python3.11 -m venv .venv` for a new standalone clone and
`source .venv/bin/activate` (or `source ../.venv/bin/activate` for the shared one).
Install Poetry 2.3.4 if it is unavailable (`pipx install poetry==2.3.4` with
pipx available, as in CI); then verify it and the selected
environment before installing a task's dependencies:

```powershell
poetry --version
python -c "import sys; print(sys.executable)"
```

After the first Poetry install, check `poetry env info --path` and
`poetry run python -c "import datacoolie, sys; print(sys.executable); print(datacoolie.__file__)"`.
The interpreter must belong to the shared `.venv`; the package must resolve to
this checkout's `src/datacoolie`. Commands use `poetry run` to bind tools to that
environment. Install the profiles needed for the task below; a docs-only
environment does not establish engine or database qualification.

## Documentation workflow

1. **Edit markdown** under `docs/`, relative to the product root.
2. **Build locally**:
   ```powershell
    # From the product root, with the shared .venv active
    poetry install --only main,docs
   poetry run properdocs serve
   ```
   Default local docs port is `8000`. If another project already uses that
   port, run DataCoolie docs on another port, for example:
   ```powershell
   poetry run properdocs serve -a 127.0.0.1:8001
   ```
3. **Check strict mode passes**:
   ```powershell
   poetry run properdocs build --strict
   poetry run python scripts/verify_docs_seo.py --site-dir site
   ```
4. Open a PR — the `docs` GitHub Actions workflow runs
   the strict build and built-site SEO verification on pull requests that touch
   docs-related files. Pushes to `main` for those paths deploy the site to
   `gh-pages`.

`docs/llms.txt` is the short public routing index. `docs/llms-full.txt` is only
a source placeholder: the `docs/scripts/gen_llms.py` ProperDocs hook generates
the published long-form companion from a bounded selection of canonical pages
and rendered reference pages. Edit the owning page, not the generated output.
The same rule applies to `docs/schema/`: schema bytes come from
`src/datacoolie/project/schemas/` and are copied during the docs build.

Review the built page for the changed commands, links and generated content.
A strict build checks site construction; it does not execute every recipe.

## Choose checks for your change

| Changed area | Checks |
|---|---|
| Markdown, navigation, generators or public docstrings | The strict build and SEO commands above; source/command and rendered-page review |
| Framework code | Install `dev` and required extras, then run the affected `tests/` paths; [testing strategy](testing.md) explains default selection and skips |
| Optional dependency declarations or release scripts | `poetry check --lock --strict` and `poetry run pytest -c pyproject.toml scripts/tests/ -n 0 -rs` |
| AI skills/build schemas | `poetry run python ai/skills/tests/run_all.py`; this is separate from core pytest |
| Simulator scenario/validator | The selected scenario through `run_scenario.py`; use [expected-failure assertions](expected-failure-scenarios.md) for negative cases |
| Spark, database or persisted-format behavior | Select the relevant [qualification cell](testing.md#datatype-qualification) and its prerequisites; record cells you did not run |
| Release version/tag or distributions | The complete local release gate below |

For a normal non-Spark source check, after selecting the shared environment:

```powershell
poetry install --with dev -E polars-delta -E polars-hash -E polars-sql
poetry run pytest tests/ -n 0 -rs
```

That command is serial for a readable local receipt; omitting `-n 0` uses the
repository's parallel default. Scope to affected test paths during development.
Neither command opts into cloud, benchmarks or datatype/runtime qualification.
Read skip reasons before claiming the changed behavior is verified.

## Release verification

Before committing a release change or creating a PyPI tag, run the repository's
local release gate from the product root:

```powershell
poetry install --with dev --with docs -E polars-delta -E polars-hash -E polars-sql -E polars-iceberg -E source-api -E metadata-db -E aws -E source-excel-polars -E cli
poetry run python -m pip install --upgrade -r scripts/requirements-release.txt
poetry run python scripts/verify_release.py
```

It validates version parity, the schema index and Poetry lock, distributions,
isolated wheel/CLI installation, strict docs, serial non-Spark tests and release
contract tests. The wheel smoke may download dependencies; the gate builds
`dist/` and `site/` and does not require a clean worktree. It does not publish.
All stages must pass before committing a release change or creating a tag.
For Spark changes, install `spark-delta` in the same environment before using
`poetry run python scripts/verify_release.py --with-spark`; the Spark stage
remains local-only. CI also runs the separate AI skills gate.

## Documentation style

- Follow the **Diátaxis** tier and the public information architecture:
  - `introduction/` → product boundary, ecosystem and adoption decisions.
  - `guide/` → runnable onboarding, task recipes, providers, platforms, CLI and operations.
  - `examples/` → small verified templates and complete walkthroughs.
  - `reference/` → explanation concepts, generated contracts and API signatures.
  - `extensions/` → plugin-author contracts.
  - `project/` → contributor workflow, benchmarks, tests and public ADRs.
- Keep Studio-specific material under `studio/`; it describes the companion
  application and does not redefine framework runtime contracts.
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
