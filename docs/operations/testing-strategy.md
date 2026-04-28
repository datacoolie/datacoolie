# Testing strategy

DataCoolie uses plain `pytest` with `pytest-xdist`. The default repo behavior
is defined in `pyproject.toml`, not in a separate CLI wrapper.

## Default test run

```powershell
# From datacoolie/
python -m pytest
```

This picks up the current default pytest options from `pyproject.toml`:

- `-n auto`
- `--dist loadgroup`
- `-m "not spark"`
- `--tb=short`
- `--import-mode=importlib`

So the default run is a parallel **non-spark** test run. It does not execute
the Spark-marked tests unless you override the marker selection explicitly.

## Markers

| Marker | Description |
|---|---|
| default selection | `pytest` runs with `-m "not spark"`, so all non-spark tests are included by default. |
| `slow` | Defined marker. Still included by default unless you exclude it yourself. |
| `integration` | Defined marker. Still included by default unless you exclude it yourself. |
| `spark` | Spark-specific tests. Excluded by default by the repo pytest config. When run directly, the Spark module also uses `pytest.importorskip(...)` for `pyspark` and `delta-spark`. |

```powershell
python -m pytest -m "not slow and not integration and not spark"
```

## Coverage gate

The repo-wide coverage gate in `pyproject.toml` is **85%**, with branch
coverage enabled:

```toml
[tool.coverage.report]
fail_under = 85
show_missing = true
```

Coverage is collected for `src/datacoolie`, with these files omitted from the
repo-wide gate:

- `src/datacoolie/engines/spark_engine.py`
- `src/datacoolie/engines/spark_session_builder.py`

Some focused local test commands may use stricter `--cov-fail-under=100`
targets for individual packages, but that is not the current global gate.

## Parallel execution contract

`pytest-xdist` distributes by **test group** (`--dist loadgroup`). Tests
that share fixtures use the `@pytest.mark.xdist_group(...)` marker to pin into
the same worker. The current Spark engine module is grouped this way so one JVM
is reused safely.

## Scope

The main automated test surface is the pytest suite under `tests/`.
Separately, `usecase-sim/` provides coarse-grained execution scenarios and
runner scripts for end-to-end validation outside the core pytest unit suite.
