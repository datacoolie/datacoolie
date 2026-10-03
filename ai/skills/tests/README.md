# Skill script tests

This suite tests scripts maintained by the Skills and their evidence tooling.
Public docs own framework guidance. Metadata, CLI validation/build, packaging,
project examples and runtime runners are tested by the product suites under
`tests/`. This suite does not assert Skill wording, headings, resource inventories
or public-docs content, and it does not run a model.

Run from the DataCoolie repository with its development dependencies and the
selected Discover script dependencies installed:

```bash
python ai/skills/tests/run_all.py
python ai/skills/tests/run_all.py discover
python ai/skills/tests/run_all.py build release
```

`run_all.py` delegates to pytest. Groups are the directories under `unit/`, so
new tests are collected automatically. The default command runs every unit
group, including local Discover CLI subprocess tests; it starts no services.

| Group | Scripts under test |
|---|---|
| `discover` | Relational, file, API and lakehouse adapters; bounded SQL probes; observation merge/refresh; watermark assessment and enrichment |
| `design` | Hash-bound approval recording and verification |
| `automation` (`build` alias) | Automation rendering and wrapper argument/exit handling with a stub CLI |
| `provision` | Approval-bound provisioning receipt validation |
| `release` | Upload record validation and local upload ordering/failure handling |
| `tools` | Evidence verifier using synthetic catalogs and grading files |

Direct pytest selection is also supported:

```bash
python -m pytest -o addopts= ai/skills/tests/unit/discover -q
```

## Source integration (opt-in)

Install the relevant Discover database/lakehouse dependencies and the fixture
dependencies. SQL Server requires ODBC Driver 18 on the host; Docker Compose
must be available and the fixture ports free.

```bash
python -m pip install -r ai/skills/datacoolie-discover/scripts/requirements-databases.txt
python -m pip install -r ai/skills/datacoolie-discover/scripts/requirements-lakehouse.txt
python -m pip install -r ai/skills/tests/integration/requirements.txt
python ai/skills/tests/run_all.py discover --integration
```

The pytest session fixture starts PostgreSQL, MySQL, SQL Server and the
MinIO/Iceberg REST/Trino stack, seeds the sources and invokes the Discover
scripts. Teardown removes this fixture stack and its volumes, including after
startup or seeding failure. `--keep-integration` retains them for diagnosis.
For integration only:

```bash
python -m pytest -o addopts= ai/skills/tests/integration -q
```

To remove a retained fixture stack:

```bash
docker compose -f ai/skills/tests/integration/compose.yaml down --volumes --remove-orphans
```

Only services with executable tests remain in the compose file. Local file and
API fixtures live under `fixtures/`; source-service fixtures are under
`integration/fixtures/`. Fixture regeneration helpers are under `tools/`.

## Optional agent evidence tooling

Agent trials and maintained eval catalogs remain separate from script tests.
An external evaluator supplies one original grading file per declared case, in
catalog order. The verifier binds successful gradings to maintained Skill and
catalog bytes:

```bash
python ai/skills/tests/tools/verify_behavioral_evidence.py verify-definitions ai/skills/datacoolie-build
python ai/skills/tests/tools/verify_behavioral_evidence.py create ai/skills/datacoolie-build /path/to/evidence.json /path/to/gradings/1.json ...
python ai/skills/tests/tools/verify_behavioral_evidence.py verify ai/skills/datacoolie-build /path/to/evidence.json --gradings /path/to/gradings/1.json ...
```

Use the full ordered grading list rather than the ellipsis shown above.
Verification without `--gradings` checks integrity only. Neither mode
authenticates execution or evaluator identity. Full-success receipts do not
represent failed or partial trials; retain those original observations separately.
