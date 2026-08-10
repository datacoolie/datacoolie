# DataCoolie AI Skill Tests

The suite validates the five outcome-owned lifecycle skills and their deterministic helpers.

## Fast verification

From the DataCoolie repository root:

```bash
python ai/skills/tests/run_all.py
```

The default run:

1. Executes all unit tests.
2. Validates `discover`, `design`, `build`, `provision`, and `release` skill contracts.
3. Validates every behavioral-eval definition without calling a model.
4. Runs local discovery fixture checks and build-owned metadata schema validation.
5. Does not start Docker or make model calls.

Run one validator:

```bash
python ai/skills/tests/run_all.py build
python ai/skills/tests/run_all.py release
```

A selected run executes only the shared workflow/harness tests, that skill's owned unit modules,
and its validator. Use the unfiltered command for the complete merge or CI gate.

## Behavioral eval evidence

Behavioral execution is intentionally external so the repository does not depend on an LLM vendor.
After an eval tool produces one successful `grading.json` per declared case, in declaration order,
bind those results to the exact skill bytes:

```bash
python ai/skills/tests/verify_behavioral_evidence.py create \
  ai/skills/datacoolie-build \
  .scratch/skill-evals/datacoolie-build/evidence.json \
  <ordered-grading.json> [<ordered-grading.json> ...]

python ai/skills/tests/verify_behavioral_evidence.py verify \
  ai/skills/datacoolie-build \
  .scratch/skill-evals/datacoolie-build/evidence.json
```

The verifier rejects failed, partial, reordered, or stale evidence. Skill or eval-definition changes
require rerunning the external eval; editing a receipt cannot make an old digest current.

## External integration fixtures

```bash
python -m pip install -r ai/skills/tests/requirements-integration.txt
python ai/skills/tests/run_all.py --integration
```

This starts only PostgreSQL, MySQL, SQL Server, MinIO, Iceberg REST, and Trino; seeds SQL Server and
Iceberg; supplies test-only connection locators to the discovery child process; and removes the
containers and volumes in `finally`. Docker Desktop, the SQL Server ODBC Driver 18, and a compatible
daemon must already be available. Use `--keep-integration` only when the same fixture state is needed
for investigation. Oracle, Hive, and mock API remain opt-in fixtures under the Compose `extended`
profile and are not claimed by the default integration gate.

## Validators

| Runner | Contract |
|---|---|
| `run_discover.py` | Source-evidence boundary and local introspection scripts |
| `run_design.py` | Material-design ownership and approval artifact |
| `run_build.py` | Framework-first build, schemas, materialization, and automation resources |
| `run_provision.py` | Conditional infrastructure and explicit apply approval |
| `run_release.py` | Consume-only immutable release and CI references |
| `verify_behavioral_evidence.py` | Current skill/eval digest binding for externally graded behavior |

Detailed manual/forward cases live in the matching `TESTING_datacoolie-*.md` file.

## Core regression assertions

- Exactly five lifecycle skills remain.
- `AGENTS.md` and each main `SKILL.md` stay within their context budgets.
- No maintained workflow references removed skills, phase journals, or cross-skill script paths.
- Metadata has one canonical modular authoring layout.
- Equal build inputs are reusable; changed inputs create another immutable ID.
- Generated runners preserve platform/engine identity, persistent runtime paths, and ordered stage
  groups.
- Release verifies and consumes the exact build without rebuilding it.
- No project lifecycle CLI is added to the DataCoolie package.

## Unresolved questions

- None.
