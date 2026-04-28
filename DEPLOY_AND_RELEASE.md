# DataCoolie Local Deploy And Release Guide

This guide is for the local maintainer workflow only.

It assumes the final standalone project repository is:

- GitHub account: `datacoolie`
- GitHub repository: `datacoolie`
- PyPI account: `datacoolie`
- PyPI project: `datacoolie`

This file is intentionally kept outside `docs/` so it does not become part of
the published documentation site.

## 1. Target State

The final release source must be the standalone repository:

- `https://github.com/datacoolie/datacoolie`

That standalone repository should contain this project as its root, including:

- `.github/workflows/ci.yml`
- `.github/workflows/docs.yml`
- `.github/workflows/publish-pypi.yml`
- `.github/workflows/release.yml`
- `mkdocs.yml`
- `pyproject.toml`
- `src/datacoolie/`
- `docs/`

The root workspace repository is not the long-term release source.

## 2. One-Time Setup

Do these steps once when preparing the standalone repository.

### Step 1: Create the GitHub repository

Create an empty repository named `datacoolie` under the `datacoolie` GitHub
account.

Recommended settings:

- default branch: `main`
- no auto-generated README if you plan to push this project tree directly
- branch protection on `main`
- require pull request review before merge
- require CI checks before merge

### Step 1A: Use a simple branch strategy

Recommended branch model for DataCoolie:

- `main` is the only release branch
- create short-lived branches for normal work
- merge changes back into `main` through pull requests
- create version tags only from commits already on `main`

Recommended branch naming examples:

- `feat/<topic>`
- `fix/<topic>`
- `docs/<topic>`
- `chore/<topic>`

For this project, prefer trunk-based development over long-lived `develop` or
release branches unless you later need a more complex multi-version support
model.

Practical rule:

- do feature work on a branch
- merge to `main` after review and green CI
- release from `main`

### Step 2: Push the project into the standalone repository

From your local machine, work from the `datacoolie/` folder.

If you are creating the standalone repository from this project tree, the
typical flow is:

```bash
cd datacoolie
git init
git add .
git commit -m "Initial standalone repository setup"
git branch -M main
git remote add origin git@github.com:datacoolie/datacoolie.git
git push -u origin main
```
- if `origin` uses `git@github.com:...`, your machine must have an SSH key registered with the `datacoolie` GitHub account before the first push will work
- if you do not want to use SSH, switch `origin` to `https://github.com/datacoolie/datacoolie.git` instead

If the standalone repo already exists, add the correct remote and push the
current project state there instead of reinitializing.

### Step 3: Enable GitHub Pages

In the standalone GitHub repo:

1. Open `Settings`.
2. Open `Pages`.
3. Set source to `Deploy from a branch`.
4. Select branch `gh-pages`.
5. Select folder `/`.
6. Save.

The docs workflow uses `mike` and will push the published site to
`gh-pages`.

### Step 4: Configure PyPI Trusted Publisher

In the `datacoolie` project on PyPI, configure a Trusted Publisher for the
GitHub repository.

Important distinction:

- linking your PyPI user account to GitHub is not enough by itself
- the `datacoolie` PyPI project must explicitly trust the GitHub repository and workflow
- for the primary GitHub Actions release path, do not create or store a PyPI API token

Use these values:

- owner: `datacoolie`
- repository: `datacoolie`
- workflow file: `.github/workflows/publish-pypi.yml`
- environment: `pypi`

This matches the workflow already prepared in this project.

The intended publishing model is:

- GitHub Actions + PyPI Trusted Publisher
- tokenless publishing via OpenID Connect
- no `PYPI_API_TOKEN` secret in GitHub for the normal release path
- releases are created from pushed version tags, not from manual publish runs

### Step 5: Confirm repository files

Before the first release, confirm these files exist in the standalone repo:

- `.github/workflows/ci.yml`
- `.github/workflows/docs.yml`
- `.github/workflows/publish-pypi.yml`
- `.github/workflows/release.yml`
- `mkdocs.yml`
- `pyproject.toml`
- `README.md`
- `docs/changelog.md`
- `src/datacoolie/__init__.py`

### Step 6: Decide the GitHub Release model

Recommended model for DataCoolie:

- publish installable artifacts to PyPI
- create a GitHub Release from the version tag
- let GitHub provide the automatic `Source code (zip)` and `Source code (tar.gz)` archives
- do not manually upload wheel or sdist files to the GitHub Release unless you intentionally want duplicate distribution channels

This keeps roles clean:

- PyPI is the installation source
- GitHub Releases is the release-notes surface
- GitHub automatic source archives are enough for source downloads

## 3. Local Prerequisites

Before validating or releasing locally, make sure your machine has:

- Python 3.11
- the root `.venv` for the active checkout activated, or an equivalent Python 3.11 environment
- `poetry` installed if you want to use the Poetry workflow locally
- `build` available if you want a Poetry-free package build check
- Git authenticated against `github.com`

Standard local convention for this project:

- use one `.venv` at the root of the active checkout
- run docs, tests, and release validation from that same environment
- do not rely on a separate `.docs-venv` for normal maintenance work

Practical interpretation:

- in the current umbrella workspace, that root `.venv` lives above `datacoolie/`
- in the standalone DataCoolie repository, the same convention becomes `<repo>/.venv`

From this workspace, your working directory for release work should be:

```bash
cd datacoolie
```

## 4. Before Every Release

Do these steps for each release.

### Step 1: Sync with the release branch

```bash
git checkout main
git pull --ff-only origin main
```

Release best practice:

- do not create release tags from feature branches
- do not publish from unmerged branches
- always tag the exact commit on `main` that you want to release

### Step 2: Update the version

Update the version in both files so they match exactly:

- `pyproject.toml`
- `src/datacoolie/__init__.py`

Example release version:

- `0.1.1`

### Step 3: Update the changelog

Edit `docs/changelog.md`.

Release flow:

1. move release items out of `Unreleased`
2. create a new dated release section such as `## [0.1.1] - 2026-05-01`
3. leave `Unreleased` in place for the next cycle

### Step 4: Review package metadata

Confirm these values are still correct:

- package name: `datacoolie`
- repository URL: `https://github.com/datacoolie/datacoolie`
- documentation URL: `https://datacoolie.github.io/datacoolie/`

## 5. Local Validation Commands

Run these commands before creating the release tag.

### Option A: Poetry-based validation

```bash
poetry check
poetry build
poetry run mkdocs build --strict
poetry run pytest tests/ --tb=short
```

### Option B: Minimal validation without Poetry

```bash
python -m build
python -m mkdocs build --strict
python -m pytest tests/ --tb=short
```

If you are running multiple local docs sites or services across projects,
change only the local serve port. The environment convention does not need to
change.

Do not continue to tagging until package build and strict docs build both pass.

## 6. Commit And Push The Release Preparation

After validation passes:

```bash
git add pyproject.toml src/datacoolie/__init__.py docs/changelog.md
git commit -m "Release 0.1.1"
git push origin main
```

Replace `0.1.1` with the real version.

## 7. Create The Release Tag

Create a tag that matches the version.

Example:

```bash
git tag v0.1.1
git push origin v0.1.1
```

The prepared PyPI and GitHub Release workflows are triggered only by tags that
match `v*`.

Best practice:

- do not use ad-hoc manual publish runs for production releases
- treat the pushed version tag as the single release trigger
- if you need a dry run, validate locally before pushing the tag

## 8. What GitHub Actions Should Do

After you push the tag to the standalone `datacoolie/datacoolie` repo:

- `ci.yml` should validate the project on pushes and pull requests
- `docs.yml` should keep the docs site updated from `main`
- `publish-pypi.yml` should build distributions and publish them to PyPI
- `release.yml` should create a GitHub Release from the tag with generated notes

For PyPI publishing specifically, the workflow should authenticate through
Trusted Publisher and the GitHub OIDC token, not through a stored PyPI API
token.

For release orchestration specifically, the release should start from the tag
push, not from a manual workflow trigger.

Expected outcomes:

- GitHub Pages site: `https://datacoolie.github.io/datacoolie/`
- PyPI package page: `https://pypi.org/project/datacoolie/`
- GitHub Release page for the new `vX.Y.Z` tag

The GitHub Release should automatically show:

- `Source code (zip)`
- `Source code (tar.gz)`

Those automatic source archives are normally sufficient. You do not need to
manually attach the Python wheel or sdist to the GitHub Release for this
project.

## 9. Post-Release Checks

After the workflows finish, verify:

1. the new version appears on PyPI
2. the docs site still builds and serves correctly
3. the repository tag exists on GitHub
4. the GitHub Release page exists for the new version
5. the automatic source archives appear on the GitHub Release page
6. the README renders correctly on GitHub and PyPI

## 10. Common Mistakes

Avoid these mistakes:

- releasing from the root workspace repo instead of the standalone project repo
- releasing from a feature branch instead of `main`
- updating `pyproject.toml` but forgetting `src/datacoolie/__init__.py`
- tagging before local validation passes
- using manual workflow runs as the normal production release path
- leaving `mkdocs.yml` or workflow paths with workspace-only prefixes
- assuming GitHub account association on PyPI automatically enables publishing
- using PyPI API tokens when Trusted Publisher is already configured
- manually treating GitHub Releases as the main Python package distribution channel

## 11. Quick Release Checklist

Use this short list before each release:

- version updated in both files
- changelog updated
- package build passes
- strict docs build passes
- tests pass
- commit pushed to `main`
- tag pushed as `vX.Y.Z`
- GitHub Actions completed successfully
- GitHub Release created successfully
- PyPI shows the new version

If all items are green, the release is complete.