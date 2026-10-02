# Testing datacoolie-build

Run the CLI-first contract checks from the product repository:

```bash
python ai/skills/tests/run_build.py
python -m pytest -o addopts='' ai/skills/tests/unit/test_build_tooling_contract.py \
  ai/skills/tests/unit/test_project_automation.py -q

# Product-owned project/runner coverage (run from the product repository root):
python -m pytest -o addopts='' tests/unit/cli/test_metadata_commands.py \
  tests/unit/project/test_metadata_validation_contract.py \
  tests/unit/project/test_metadata_overlays.py \
  tests/unit/project/test_schema_enums.py \
  tests/unit/project/test_transform_metadata_schema.py \
  tests/unit/project/test_workspace_config_and_runner_contract.py \
  tests/unit/project/test_workspace_materialization.py \
  tests/unit/docs/test_runner_job_parameters.py \
  tests/unit/docs/test_runner_platform_adapters.py \
  tests/unit/docs/test_operational_runner_contract.py \
  tests/unit/docs/test_runner_operational_safety.py -q
```

The framework CLI is the only deterministic project/build implementation. The
tests cover:

- `datacoolie.yml` with one metadata root and multiple SQL/functions roots;
- wrapped metadata fragments and environment overlays;
- inline, relative, and `artifact:/` query references;
- per-root `auto`/wheel/root-init ZIP/copy packaging decisions;
- all-environment builds with one root manifest and an exact `current` copy;
- local manifest inventory/hash validation and current-to-retained comparison;
- portable standalone artifact validation with an explicit limited-scope result;
- direct CLI metadata conversion and project-owned automation wrappers.
- canonical public runner behavior, host adapters, replay/maintenance guards
  and Spark teardown under `docs/examples/files/runners`; Skills do not carry
  a duplicate runner source tree.

Do not reintroduce a skill-local materializer, merge helper, checksum sidecar,
current pointer, or config file. Project preparation belongs to the installed
DataCoolie CLI; runtime execution remains a project-owned runner concern, and
there is no CLI workload-run command.

## Unresolved questions

None.
