# Databricks — Deferred Scope

Databricks notebook assets for file metadata are now available in this folder:

- `databricks_use_cases.json` (file + delta subset)
- `sample_databricks_spark.ipynb`
- `sample_databricks_polars.ipynb`
- `sample_databricks_secrets.ipynb`
- `README.md`

## What is still deferred

- usecase-sim runner integration (`run.py`, `run_scenario.py`) for Databricks
- Scenario catalog/profile integration for Databricks in `scenarios.json`
- Advanced Databricks automation and CI coverage for these notebooks

## Prerequisites for deferred work

- [ ] Databricks execution profile design in runner layer
- [ ] Databricks credential strategy for non-notebook runner flows
- [ ] End-to-end validation matrix for Spark + Polars in Databricks CI

## Status

Partially implemented: notebook-first Databricks assets are in place.
Runner-level integration remains deferred.
