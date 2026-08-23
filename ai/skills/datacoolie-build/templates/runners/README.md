# Runner template index

Use these files as bootstrap examples for durable workspace entrypoints. They are not a capability
catalog; prove installed platform, engine, provider, source, transform, and destination support
before selecting or adapting one.

| Need | Templates | Parameter transport |
|---|---|---|
| Local normal run | `run_local_*.py.example` | Python CLI |
| Databricks native normal run | `run_databricks_*.ipynb.example` | Databricks widgets |
| Databricks external normal run | `run_databricks_*_sdk.py.example` | Python CLI; adapt transport to the actual host |
| Fabric native normal run | `run_fabric_*.ipynb.example` | tagged parameter cell |
| Fabric external normal run | `run_fabric_*_azure_sdk.py.example` | Python CLI; adapt transport to the actual host |
| AWS Glue normal run | `run_aws_glue_*.py.example` | Glue job arguments |
| Replay | `replay_*.example` | execution-host transport plus replay parameters |
| Maintenance | `maintenance_*.example` | execution-host transport plus safety gates |

Read `references/platform-contract.md` before selecting native/external bootstrap and
`references/runner-contract.md` for common and normal-run behavior. Replay and maintenance also
require `references/operations-contract.md`. Copy or adapt the selected example into the workspace;
generated projects must not import this directory.
