---
title: DataCoolie Skills — AI-Assisted Project Workflow
description: Install the official DataCoolie Skills and use Discover, Design, Build, Provision, and Release to create verified data projects safely.
---

# Build projects with DataCoolie Skills

DataCoolie Skills are the official AI-assisted workflow for turning data
requirements into verified, releasable DataCoolie projects. The Python package
runs ETL pipelines; the Skills guide a compatible AI agent through discovery,
design, implementation, infrastructure preparation, and release.

The workflow keeps one canonical metadata model as the authoring source of
truth. Environment overlays and platform/engine-specific runners supply paths,
catalogs, credentials, engines, and target runtime details without duplicating
the pipeline's business intent.

## Prerequisites

- Python 3.11 or newer and the appropriate [DataCoolie runtime extras](installation.md).
- Node.js with `npm` and `npx` available.
- An AI runner that supports installed Agent Skills and repository instructions.
- Credentials supplied through environment variables, ambient identity, or a
  supported secret store—not pasted into prompts or committed metadata.

Check the required tools:

```bash
python --version
node --version
npx --version
```

## 1. Install the five Skills

Run this from the project where your AI runner should discover the Skills:

```bash
npx skills add datacoolie/datacoolie
```

The installer lets you select the target agent and installation scope. Select
all five DataCoolie Skills, then verify the project installation:

```bash
npx skills list
```

You should see:

```text
datacoolie-discover
datacoolie-design
datacoolie-build
datacoolie-provision
datacoolie-release
```

The installer and the Python package are separate. Updating one does not
implicitly update the other.

## 2. Create a project workspace

DataCoolie projects use a `{project_name}_dcws/` control workspace. Preserve an
existing workspace and its `AGENTS.md`; bootstrap the contract only for a new
workspace.

=== "PowerShell"

    ```powershell
    $projectName = "sales_analytics"
    $workspace = "${projectName}_dcws"
    New-Item -ItemType Directory -Path $workspace -Force | Out-Null
    Invoke-WebRequest `
      -Uri "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md" `
      -OutFile "$workspace/AGENTS.md"
    Set-Location $workspace
    ```

=== "Bash"

    ```bash
    project_name="sales_analytics"
    workspace="${project_name}_dcws"
    mkdir -p "$workspace"
    curl -o "$workspace/AGENTS.md" \
      https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/AGENTS.md
    cd "$workspace"
    ```

The workspace contract is public at
[`ai/AGENTS.md`](https://github.com/datacoolie/datacoolie/blob/main/ai/AGENTS.md).
It routes the agent by the outcome you request, not through mandatory phases
that do not apply.

## 3. Route work by outcome

| Skill | Owns | Use it when |
|---|---|---|
| `datacoolie-discover` | Verified, read-only source facts | Starting a project or resolving a new, changed, missing, or contradictory source fact |
| `datacoolie-design` | Material system intent and architecture | Defining or changing stages, contracts, grain, keys, load behavior, platform boundaries, or release policy |
| `datacoolie-build` | Runnable, immutable, verified builds | Creating metadata, overlays, runners, functions, tests, and local verification |
| `datacoolie-provision` | Required environment resources | A requested environment lacks verified infrastructure |
| `datacoolie-release` | Deployment lifecycle for one exact build | Deploying, promoting, or rolling back a verified build |

Common routes are deliberately short:

```text
New project                         discover → design → build
Compatible change or local test                         build
Missing target resources              build/release → provision → resume
Deploy an existing verified build                       release
```

Provisioning is conditional, and release happens only when explicitly
requested. A failed artifact returns to the Skill that owns it instead of being
wrapped in a successful handoff.

## 4. Understand the project state

| Category | Typical contents | Rule |
|---|---|---|
| Durable source | `config.yaml`, `architecture/current.md`, `discover/`, `metadata/`, `runners/`, optional `functions/` | Edit and review these project sources |
| Generated build | `.builds/artifacts/{build_id}/`, `.builds/current/` | Immutable artifact plus disposable current projection; never edit either |
| Runtime state | `.runtime/{env}/logs/`, `.runtime/{env}/watermarks/` | Mutable, persistent, and isolated by environment |
| Control evidence | `.approvals/`, `provision/evidence/`, `.releases/` | Bind approvals and receipts to exact scope and artifacts |

Discovery evidence supports design but is not a runtime dependency. Release
resolves an exact verified build and never rebuilds it.

## 5. Respect the approval boundaries

| Action | What is allowed before approval | Required approval |
|---|---|---|
| Material design | Gather facts and prepare the final architecture | Approval bound to the final architecture hash |
| Provisioning | Inventory, plan, and dry-run | Approval for the exact environment and persisted plan before mutation |
| Release | Preflight a verified build and target | Target-specific authorization before deployment, promotion, or production mutation |

Design or implementation approval never authorizes infrastructure mutation or
deployment. If an approved architecture changes materially, review and approve
the new final architecture before Build continues.

## 6. Start a new project

Give the agent requirements and credential *names*, not secret values. For
example:

```text
Create a DataCoolie project named sales_analytics.
The source is PostgreSQL; credentials are available through DB_HOST,
DB_NAME, DB_USER, and DB_PASSWORD. Discover the declared source read-only,
then design and build a local Polars + Delta pipeline. Stop at every required
approval gate. Do not provision or deploy anything.
```

The agent should start with Discover, present material design for approval,
then use Build to create and verify the project. Provision and Release remain
out of scope until separately requested.

## Troubleshooting

| Problem | Check |
|---|---|
| `npx` is not recognized | Install a supported Node.js release, restart the shell, and rerun `node --version` and `npx --version` |
| A Skill is missing | Run `npx skills list`; reinstall and select all five Skills for the same agent and scope |
| The agent ignores workspace rules | Start the agent from the `_dcws` directory and confirm `AGENTS.md` exists there |
| An existing workspace behaves differently | Review its pinned `AGENTS.md` before replacing it; contract updates can change project behavior |
| A cloud action pauses for approval | Confirm the exact environment, plan/build identity, state intent, and mutation scope; do not reuse a broader or stale approval |

## Next

- Watch the [WWI multi-cloud walkthrough](../tutorials/wwi-medallion-multicloud.md).
- Learn the [metadata model](../concepts/metadata-model.md).
- Use [DataCoolie Studio](../datacoolie-studio.md) to inspect metadata, assets,
  lineage, sources, and run evidence.
