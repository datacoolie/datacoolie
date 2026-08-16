# Testing `datacoolie-discover`

The discovery skill is script-first. Database, file, API, and lakehouse introspection scripts emit
the same 19-column `observations.csv` contract. Unit tests use local fixtures and mocks, so routine
verification requires no external source.

## Fast Validation

From `datacoolie/`:

```bash
python ai/skills/tests/run_discover.py
python -m pytest -o addopts="" ai/skills/tests/unit/test_introspect_db.py \
  ai/skills/tests/unit/test_introspect_files.py \
  ai/skills/tests/unit/test_introspect_api.py \
  ai/skills/tests/unit/test_introspect_lakehouse.py \
  ai/skills/tests/unit/test_discovery_evidence.py \
  ai/skills/tests/unit/test_discovery_assessment.py \
  ai/skills/tests/unit/test_discovery_dependencies.py -q
```

The suite checks:

- the exact shared CSV header and stable row identity;
- database, file, OpenAPI, and lakehouse output mapping;
- normalized scratch-only watermark shortlisting, compact object summaries, and explicit confirmed
  role validation;
- complete object-level assessment that derives exact annotations and report rows from one scratch
  decision document;
- deterministic annotation merging and rejection of unknown/duplicate keys;
- deterministic multi-probe merging, explicit API source-operation identity, atomic artifacts, and
  partial-probe status;
- explicit existing-source replacement, partial-evidence gates, and deterministic scratch diffs;
- one-statement, read-only SQL probe validation, row limits, rollback, and timeout reporting;
- bounded directory/catalog inspection and avoidance of source row-count scans;
- capability-specific dependency routing and current external CLI command contracts;
- rejection of secret-bearing process arguments;
- resource routing and the compact discovery output contract.

## Optional Integration Validation

Run Docker-backed source checks only when the test services and drivers are already available:

```bash
python -m pip install -r ai/skills/tests/requirements-integration.txt
python ai/skills/tests/run_all.py discover --integration
```

The orchestrator starts and seeds the default Docker stack, passes test-only connection locators to
the child process, validates the canonical observation header, and removes containers and volumes
afterward. `run_discover.py --docker` remains available when an operator already owns the services
and supplies `DATACOOLIE_TEST_POSTGRES_URL`, `DATACOOLIE_TEST_MYSQL_URL`, and
`DATACOOLIE_TEST_MSSQL_URL` through the environment. Do not place real connection values in commands,
reports, or fixtures.

## Expected Outputs

Final discovery output contains `discover/observations.csv` and `discover/report.md`. Raw probe
evidence is optional and must be safe to retain. A Markdown schema inventory is not part of the
contract.

## Unresolved questions

- None.
