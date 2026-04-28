# Fabric — Deferred

Fabric runner integration is still deferred in usecase-sim.

Prepared assets are available in this folder:

- `fabric_use_cases.json` (file + delta focused metadata for OneLake paths)
- `sample_fabric_spark.ipynb` (primary notebook sample)
- `sample_fabric_polars.ipynb` (secondary notebook sample)
- `README.md` (usage notes and prerequisites)

## Planned Scope
- Runner scripts using `FabricPlatform` and engine runners
- Metadata: `fabric_use_cases.json`
- Auth: Azure AD / Entra ID service principal or managed identity
- Secret source: Azure Key Vault via Fabric workspace identity

## Prerequisites
- [ ] Access credentials configured
- [ ] Environment-specific connectors verified (OneLake, Lakehouse)
- [ ] Cloud resources provisioned (Fabric workspace, lakehouse, warehouse)

## Status
Execution integration deferred. Use prepared assets directly in Fabric notebooks
until `run.py` / `run_scenario.py` add `--platform fabric` support.
