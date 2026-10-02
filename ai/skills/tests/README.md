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
4. Runs local discovery fixture checks and verifies that metadata contract
   guidance resolves to the published project-owned schema and installed CLI.
5. Does not start Docker or make model calls.

Framework-owned metadata, CLI, project-build and canonical runner behavior
tests live under the product `tests/unit/cli`, `tests/unit/project` and
`tests/unit/docs` suites. This harness keeps only Skill routing, handoff and
resource-consumption checks so a product contract is not counted twice.

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
  .scratch/skill-evals/datacoolie-build/evidence.json \
  --gradings <ordered-grading.json> [<ordered-grading.json> ...]
```

With `--gradings`, verification requires one original grading file per declared case in declaration
order. It revalidates each file and compares its actual SHA-256 and complete derived result with
the receipt, rejecting missing, modified, reordered, or mismatched artifacts. Python callers use
`validate_evidence(skill_dir, evidence, grading_paths=ordered_paths)` for the same check.

Omitting `--gradings` preserves the receipt v1 integrity-only check: receipt structure, declared
expectations, passing claims, and current skill/eval digests. It does not check original grading
bytes or establish that the recorded grading hashes or evidence text came from those originals.
Neither mode authenticates execution or evaluator identity, or establishes that evidence claims
are true. Hashes bind bytes; anyone able to rewrite artifacts can recompute them. Skill or
eval-definition changes require fresh external evaluations under the maintainer protocol below;
the verifier cannot establish that those evaluations actually happened.

Versioned eval catalogs classify every stable case ID under exactly one capability family and one
kind. `decision` cases assess routing and choices and must have no fixture files. `execution` cases
must declare existing repository fixture paths plus observable command, exit-code, and retained
output/evidence expectations. Report pass rates by capability and kind; a raw assertion total can
overweight families with several host variants. The default deterministic gate validates this
catalog contract but does not execute the model-facing cases.

Schema version 2 uses the top-level fields `skill_name`, `eval_schema_version`, `case_kinds`,
`capability_families`, and `evals`. Cases remain in ascending stable-ID order. Fixture paths are
unique, repository-contained relative POSIX paths so the catalog stays portable across hosts.

## Maintainer eval protocol

1. Label each comparison accurately: `previous` is the actual earlier skill revision, `current`
   is the candidate revision, and `no-skill` omits the skill. Never relabel one as another;
   record unavailable comparisons explicitly. Record revisions and skill digests, including the
   current candidate's `skill_digest(skill_dir)`, plus eval inputs and evaluator/run settings.
2. Run repeated trials on the same inputs for each available condition with comparable settings.
   Retain raw outputs and original gradings per case, condition, and trial outside the maintained
   skill directory, including failures, errors, and incomplete runs. Do not overwrite failures
   with successful retries; an all-passing receipt is only a subset of the evaluation record.
3. Report measured expectation pass rates with counts and trial variability. Record actual
   timing, token usage, or cost when measured; otherwise use `null`/`unavailable`, never invented
   values or zeros standing in for missing data. Explain missing trials and retain their results.
4. Create receipts only for all-passing runs and verify them against their ordered original
   gradings. Review raw outputs against expectations and report regressions and limitations.
   Deterministic tests check contracts and verifier behavior; they do not provide full behavioral
   proof or substitute for external trials. Model calls remain optional and outside default CI.

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
| `verify_behavioral_evidence.py` | Receipt integrity and optional original-grading comparison; no execution authentication |

Detailed manual/forward cases live in the matching `TESTING_datacoolie-*.md` file.

## Core regression assertions

- Exactly five lifecycle skills remain.
- `AGENTS.md` and each main `SKILL.md` stay within their context budgets.
- No maintained workflow references removed skills, phase journals, or cross-skill script paths.
- Metadata has one canonical project-owned schema and supports the documented
  modular authoring layouts.
- Equal build inputs are reusable; changed inputs create another immutable ID.
- Generated runners preserve platform/engine identity, persistent runtime paths, and unchanged
  stage passthrough.
- Release verifies and consumes the exact build without rebuilding it.
- Project preparation commands are owned by the installed DataCoolie CLI;
  workload execution remains in a project-owned runner.

## Unresolved questions

- None.
