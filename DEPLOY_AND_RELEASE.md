# DataCoolie Deploy and Release Guide

This maintainer guide describes the workflows checked into the standalone
`https://github.com/datacoolie/datacoolie` repository. It stays outside
`docs/` and is not published on the documentation site.

## Current automation

| Workflow | Trigger | Current behavior |
|---|---|---|
| `.github/workflows/ci.yml` | Push to `main`, any pull request | Poetry 2.3.4; `poetry check`; package build; pytest is advisory (`continue-on-error`) |
| `.github/workflows/docs.yml` | Relevant docs/source/config changes | Strict ProperDocs build; deploys `main` to `gh-pages` with `properdocs gh-deploy --force` |
| `.github/workflows/publish-pypi.yml` | Tag matching `v*` | Poetry build, `twine check`, then trusted-publisher upload to PyPI |
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
Require the checks that the team considers release-blocking. Note that the
current CI workflow treats pytest as advisory; branch protection cannot turn
that step into a hard gate without changing the workflow.

## Release prerequisites

- Python 3.11
- Poetry 2.3.4 for parity with GitHub Actions
- Git access to the `datacoolie/datacoolie` repository
- a clean standalone repository checkout

Run release commands from the repository root.

## Prepare a release

1. Update `version` in `pyproject.toml`.
2. Update `__version__` in `src/datacoolie/__init__.py`.
3. Confirm both values are identical and the intended tag will be `v<version>`.
4. Review package metadata and generated release-note inputs (merged pull
   requests and commit messages).

There is no `docs/changelog.md` in the current repository. GitHub release notes
are generated from the tag by `.github/workflows/release.yml`.

## Validate locally

For workflow parity:

```bash
poetry check
poetry build
python -m twine check dist/*
poetry run properdocs build --strict
poetry run pytest
```

The strict docs build and package checks must pass before tagging. Even though
CI currently marks pytest advisory, treat a local test failure as unresolved
unless the release owner has explicitly accepted it.

A Poetry-free diagnostic is also possible when equivalent dependencies are
already installed:

```bash
python -m build
python -m twine check dist/*
python -m properdocs build --strict
python -m pytest
```

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
- strict docs build passes
- tests pass, or an explicit release decision records the exception
- release commit is on `main`
- immutable `vX.Y.Z` tag is pushed
- PyPI trusted-publisher workflow succeeds
- GitHub Release is created with generated notes
- installation and docs smoke checks pass

## Unresolved questions

- Should `.github/workflows/ci.yml` stop treating pytest as advisory and make
  test failures release-blocking?
