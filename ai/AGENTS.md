# DataCoolie AI Workflow

## Purpose

Turn data requirements into verified, releasable DataCoolie projects through the shortest route
that matches the current state. Skills own outcomes, not mandatory phases.

## Core Contract

- Prefer DataCoolie metadata, registered components, and `DataCoolieDriver.run(...)` whenever the
  installed framework supports the required path. Custom code is limited to a verified unsupported
  boundary.
- Treat `discover/` as design-time evidence. Runtime metadata, code, builds, and releases must not
  depend on it.
- Keep durable project sources as the authoring source of truth. `.builds/artifacts/{build_id}` is
  the immutable all-configured-environment source; `.builds/current` is its verified runnable projection for normal
  local integration and validation.
- Release resolves build `current` once to an exact canonical artifact. Each runner exposes one stable
  target current; candidates are temporary and release-addressed, never build-addressed.
- Keep mutable logs and watermarks outside `.builds/` in an environment-isolated control-storage
  namespace. Deployed metadata is an immutable build projection, not another authoring source.
- Skills resolve their own bundled resources relative to `SKILL.md`. Cross-skill handoffs use
  workspace artifacts and typed receipts, never another skill's script path.
- A consumer validates the exact handoff invariants it needs and fails closed; optional generated
  automation is never a prerequisite for interactive validation.
- The installed `datacoolie` package is the runtime framework. Project lifecycle automation belongs
  to skills or generated project files, not a framework lifecycle CLI.
- Preserve established IaC/deployment automation. For direct operations, prefer the official AWS
  or Databricks CLI; route Fabric by control plane: Azure/ARM uses Azure CLI and Fabric-native work
  uses Fabric CLI. Owning skills define fallback and safe installation.
- A build contains no custom-function artifact or exactly one approved WHL/ZIP artifact. Design
  selects its format; Build packages and verifies it; Provision prepares reusable target
  capability; Release attaches the exact immutable artifact; runners only allowlist its fixed
  project import prefix and invoke DataCoolie. Only `metadata` and optional `functions` have fixed
  component names; runners are platform-native and all runtime/activation references target-defined.

## Project State

Durable sources:

```text
{workspace}/
  AGENTS.md
  config.yaml
  architecture/current.md             # when material design exists
  discover/                           # required source evidence for a new project
  metadata/
  runners/
  functions/                          # optional
  automation/                         # optional, project-owned
  provision/                          # optional
```

Derived and runtime state:

```text
.builds/artifacts/{build_id}/         # immutable generated build
.builds/evidence/{build_id}/{env}/    # build verification receipts
.builds/current/build.json            # exact source build ID for the runnable projection
.builds/current/{env}/                # generated metadata/ set and runner artifacts
.builds/current/functions/            # latest generated functions artifact when present
.runtime/{env}/logs/                  # local default; mutable, persistent
.runtime/{env}/watermarks/            # local default; mutable, critical state
.approvals/                           # required manual approvals only
.releases/                            # deploy/promote/rollback receipts
provision/evidence/{env}/             # provision plans and receipts when needed
```
Normal run, test, and validation execute build `current`; historical verification selects an exact artifact.
Release may select `current`, then pins `current/build.json` and reads canonical artifacts.
Build current is disposable, never release identity. Never edit or symlink generated state.

## Skill Ownership

| Skill | Sole outcome | Trigger |
|---|---|---|
| `datacoolie-discover` | Verified source facts | Every new project; otherwise a new, changed, missing, or contradictory source fact |
| `datacoolie-design` | System intent and material decisions | New project or material contract/architecture change |
| `datacoolie-build` | Runnable, immutable, verified build | Bootstrap, metadata, runners, functions, implementation, local run, test, materialization, or build CI |
| `datacoolie-provision` | Required environment resources | A requested target lacks infrastructure |
| `datacoolie-release` | Deployment lifecycle for one verified build | Deploy, promote, rollback, or consume-only release CI/CD |

No skill redefines another skill's artifact semantics. Return a failed artifact or receipt to its
owner.

## State-Based Routing

1. Inspect only state relevant to the requested outcome.
2. Discover every declared source before designing a new project. For an existing project,
   discover only a new or changed source or facts that are missing or contradictory.
3. Design only when architecture is absent for a new project or the request changes stages,
   contracts, modeling, load behavior, platform/resource boundaries, or release policy.
4. Build owns all compatible implementation and local verification. It bootstraps missing project
   structure as part of the requested work.
5. Provision only when required resources are missing, drifted, inaccessible, or unknown; resume
   the blocked build or release after verification.
6. Release only on an explicit deployment-lifecycle request. It consumes an exact verified build
   and never rebuilds it.

Shortest common routes:

| Request | Route |
|---|---|
| New project | `discover -> design -> build` |
| Existing project with a new, changed, or unresolved source | `discover -> design/build` |
| Compatible implementation or local test | `build` |
| Missing infrastructure | `build/release -> provision -> resume` |
| Deploy an existing verified build | `release` |

## Gates

- Read-only discovery, compatible implementation, lint, tests, and local verification need no
  manual gate unless they introduce a material decision.
- Material design requires approval bound to the final architecture hash. Whenever
  `architecture/current.md` exists, build requires its exact matching receipt and rejects a
  missing, malformed, misnamed, or stale receipt.
- Provision planning and dry-run are allowed in scope; external resource mutation requires explicit
  approval for the target and plan.
- Implementation or design approval never authorizes deployment. Protected-target release follows
  its environment policy, and production mutation requires explicit current-session authorization.
- Release verifies required resources, environment-isolated runtime paths, watermark-state
  compatibility, and authorization before staging. It qualifies the runner slice before replacing
  stable target current and observing its marker. Authorization binds the exact build, target,
  candidate/current references, runtime paths, state intent, and scope.

## Handoff

Handoffs identify the owning artifact, exact path or ID, verification evidence, skipped checks, and
unresolved questions. Never advance by wrapping a failed check in a success receipt.

Load detailed capability, metadata, runner, operation, materialization, provisioning, or release contracts only from the skill that owns the requested outcome.
