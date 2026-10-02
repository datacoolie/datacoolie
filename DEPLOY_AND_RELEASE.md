# DataCoolie Deploy and Release Guide

This maintainer guide describes the workflows checked into the standalone
`https://github.com/datacoolie/datacoolie` repository. It stays outside
`docs/` and is not published on the documentation site.

## Current automation

| Workflow | Trigger | Current behavior |
|---|---|---|
| `.github/workflows/ci.yml` | Push to `main`, any pull request | Poetry 2.3.4; shared local release verifier; non-Spark pytest is blocking |
| `.github/workflows/docs.yml` | Relevant docs/source/config changes | Strict ProperDocs build; deploys `main` to `gh-pages` with `properdocs gh-deploy --force` |
| `.github/workflows/publish-pypi.yml` | Tag matching `v*` | Shared release verifier, then trusted-publisher upload to PyPI |
| `.github/workflows/release.yml` | Tag matching `v*` | Creates the GitHub Release with generated release notes |

The tag push is the release trigger. There is no checked-in changelog file or
manual production publish workflow.

## One-time repository settings

### GitHub Pages

Configure Pages to deploy from the `gh-pages` branch, root folder. The docs
workflow creates/updates that branch; it does not use `mike`.

### PyPI trusted publisher

Configure the `datacoolie` PyPI project with:

- owner: `datacoolie`
- repository: `datacoolie`
- workflow: `.github/workflows/publish-pypi.yml`
- environment: `pypi`

The workflow requests `id-token: write` and uses
`pypa/gh-action-pypi-publish`, so the normal release path needs no stored PyPI
API token.

### Branch protection

Use `main` as the release branch and tag only commits already merged into it.
Require the checks that the team considers release-blocking. The shared local
release verifier is the CI gate for package, docs, and non-Spark tests.

## Release prerequisites

- Python 3.11
- Poetry 2.3.4 for parity with GitHub Actions, installed outside the project
  virtual environment (`pipx` is recommended)
- Release validation tools in the active Poetry environment (`poetry run python -m pip install --upgrade -r scripts/requirements-release.txt`)
- Git access to the `datacoolie/datacoolie` repository
- a clean standalone repository checkout

Run release commands from the repository root.

## Prepare a release

1. Update `version` in `pyproject.toml`.
2. Refresh the installed project metadata with `poetry install --only-root`
   in the configured development environment. `datacoolie.__version__` reads
   distribution metadata automatically; do not edit it or add component versions.
3. Confirm `poetry run dc --version` matches and the intended tag is `v<version>`.
4. Review package metadata and generated release-note inputs (merged pull
   requests and commit messages).
5. Run the local release verifier and resolve every failure before committing
   or tagging.

There is no `docs/changelog.md` in the current repository. GitHub release notes
are generated from the tag by `.github/workflows/release.yml`.

### Version policy

`pyproject.toml` is the single release-version source. Runtime code reads
`datacoolie.__version__`; CLI responses, persisted logs, inspection and build
manifests report it as `datacoolie_version`. Documentation renders the version
from the project manifest at build time. Never copy the current release number
into runtime constants or rewrite historical log fixtures during a release.

During `0.x`, patch releases preserve compatibility; breaking changes require a
minor release and clear release notes. From `1.0`, use semantic versioning:
patch for compatible fixes, minor for compatible features, major for breaking
changes. CLI and logging schema counters are independent: retain them for
compatible optional fields and increment them for incompatible field removal,
renaming, type changes or semantic changes. Consumers ignore unknown fields;
older outputs may omit `datacoolie_version`. Metadata schemas keep their own
existing version-selection policy.

## Validate locally

Run the shared local gate before creating the commit that will be tagged. Do
not run `poetry sync` in a shared or already-customized virtual environment:
`sync` removes packages that are not in the selected groups and extras. The
standard and Spark commands below select different extras, so running both
with `sync` can also remove dependencies installed by the other command.

Use `poetry install` for local validation. It installs the locked dependencies
that are missing without pruning unrelated packages:

```bash
poetry install --with dev --with docs -E cli -E polars-delta -E polars-hash -E polars-sql -E polars-iceberg -E source-api -E metadata-db -E aws -E source-excel-polars
poetry run python -m pip install --upgrade -r scripts/requirements-release.txt
poetry run python scripts/verify_release.py
```

The verifier checks version parity, lock metadata, distributions, Twine
metadata, a clean-wheel install/import smoke test, the strict docs build, and
the default non-Spark test suite. Any failed stage is a release blocker.
The wheel smoke also checks all three CLI invocation forms, successful/error
JSON envelopes, the default metadata schema target, and real local
system/job/dataflow logs in snapshot and batch modes against the wheel version.

Spark is intentionally local-only. After the standard gate, add the Spark
dependencies with `poetry install` and run the explicit local gate:

```bash
poetry install --with dev --with docs -E spark-delta
poetry run python scripts/verify_release.py --with-spark
```

The Spark command preserves the non-Spark dependencies already installed by
the first command. If the Spark gate is run independently, include the
standard extras and the Spark extras in the same `poetry install` command.
Use a dedicated Poetry virtual environment or checkout when the current
environment must remain completely unchanged; `poetry install` may still add
or repair the project's dependencies.

A Poetry-free diagnostic is also possible when equivalent dependencies are
already installed:

```bash
python -m build
python -m twine check dist/*
python -m properdocs build --strict
python -m pytest
```

The Poetry command above is the release gate of record; the diagnostic form is
useful only for investigating an environment that cannot use Poetry.

## Commit and tag

Example for version `0.1.3`:

```bash
git switch main
git pull --ff-only origin main
git add pyproject.toml src/datacoolie/__init__.py
git commit -m "chore(release): 0.1.3"
git push origin main
git tag v0.1.3
git push origin v0.1.3
```

Do not create or push the tag until the local verifier has passed on the exact
working tree that will be committed.

Before pushing the tag, verify:

```bash
git show v0.1.3:pyproject.toml
git show v0.1.3:src/datacoolie/__init__.py
```

Do not reuse or move a published version tag. If a release is bad, fix forward
with a new version.

## Verify the automated release

After the tag push, confirm:

1. `publish-pypi` built and checked both distributions and published through
   OIDC.
2. `release` created the GitHub Release and generated notes.
3. PyPI shows the new version and its metadata.
4. A clean environment can install the released package.
5. GitHub automatically exposes the source zip and tarball.

Docs deployment is driven by changes merged to `main`, not by the version tag.
Confirm the relevant `docs` workflow run succeeded and
`https://datacoolie.github.io/datacoolie/` serves the expected version.

## Release checklist

- versions match in `pyproject.toml` and `src/datacoolie/__init__.py`
- Poetry metadata and package build pass
- `twine check dist/*` passes
- shared local release verifier passes, including wheel install smoke test
- strict docs build passes
- non-Spark tests pass; Spark is run locally when the release touches Spark
- release commit is on `main`
- immutable `vX.Y.Z` tag is pushed
- PyPI trusted-publisher workflow succeeds
- GitHub Release is created with generated notes
- installation and docs smoke checks pass

## Unresolved questions

- None.
