---
title: Testing Strategy — DataCoolie Operations
description: Understand DataCoolie testing layers, local validation patterns, coverage expectations, and how to keep pipelines safe to change.
---

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
- `--strict-markers`
- `--tb=short`
- `-q`
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

## Coverage

The current `pyproject.toml` does **not** configure `pytest-cov`, branch
coverage, omissions, or a repository-wide failure threshold. Run coverage
explicitly when needed, and do not treat a normal `python -m pytest` result as
a coverage gate.

## Parallel execution contract

`pytest-xdist` distributes by **test group** (`--dist loadgroup`). Tests
that share fixtures use the `@pytest.mark.xdist_group(...)` marker to pin into
the same worker. The current Spark engine module is grouped this way so one JVM
is reused safely.

## Scope

The main automated test surface is the pytest suite under `tests/`.
Separately, `usecase-sim/` provides coarse-grained execution scenarios and
runner scripts for end-to-end validation outside the core pytest unit suite.
