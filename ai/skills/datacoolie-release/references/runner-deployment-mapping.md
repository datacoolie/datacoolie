# Runner Deployment Mapping

## Scope

Use this reference only to map an already-built runner artifact to its native execution resource.
Generic staging, qualification, activation, rollback, and receipt rules remain in
`deployment-contract.md`; function artifact handling remains in `python-functions-deployment.md`.

## Required Mapping

Before mutation, declare:

- one `runner_deployment_kind`;
- the native target identity and optional runtime kind;
- the immutable runner source artifact and digest;
- a release-addressed temporary upload/import reference (or target-assigned opaque identity) and a
  stable version-independent activation reference;
- how target-side identity, bytes, and runtime compatibility will be observed.

Do not infer deployment kind from platform, engine, or file extension. A Python file can be an
external process, Glue script, Lambda package input, or another target-specific resource.
`release_id` identifies the deployment attempt and `build_id` remains artifact provenance; native
targets do not need to express either identity as a filesystem folder.

## Target Kinds

| Kind | Runner projection | Required observation |
|---|---|---|
| `aws-glue` | Upload the script to S3 and bind its URI through the Glue Job command `ScriptLocation` | Job identity, command/runtime, exact S3 object digest or version, and staged execution result |
| `aws-lambda` | Build the target-approved Lambda ZIP or container image and publish/update the function/version/alias | Function/version identity, code hash or image digest, runtime/architecture, and invocation result |
| `databricks-notebook` | Import the notebook into Databricks Workspace and optionally bind it to a Job task | Workspace object identity, language/format, Job/task binding when used, and run result |
| `fabric-notebook` | Create or update a Fabric Notebook Item with its selected Spark or pure-Python definition | Workspace/item identity, runtime kind, item definition/version evidence, and run result |
| `external-python` | Install or copy the runner as a file, package, container, or service on the actual execution host | Host/scheduler identity, artifact digest, interpreter/runtime identity, and execution result |

Lambda deployment packaging is a runner deployment concern. It is distinct from the DataCoolie
custom-function artifact stored in the fixed target `functions` component.

## Component Mapping

The runner target may be a job, function, notebook item, workspace object, scheduler entry, or host
path; a folder named `runners` is not required. Independently map:

- the complete declared metadata set into a target component named `metadata`;
- the exact optional custom-function artifact into a target component named `functions`;
- configured logs and watermarks to their existing mutable runtime paths without transfer or
  overwrite.

When several runner targets share one physical `metadata` or `functions` component, preflight all
active consumers. Reject an incompatible digest replacement unless the approved target design
provides isolation or coordinated activation.

## Authoritative Platform References

Check current official documentation before generating commands:

- AWS Glue job command and `ScriptLocation`: https://docs.aws.amazon.com/glue/latest/webapi/API_JobCommand.html
- AWS Lambda deployment packages: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-package.html
- Databricks Workspace import: https://docs.databricks.com/aws/en/dev-tools/cli/reference/workspace-commands
- Fabric Notebook Items: https://learn.microsoft.com/rest/api/fabric/notebook/items

Record the exact CLI/API version and observable result used by the release. If the chosen native
mechanism is unavailable, stop for Provision or an explicit target-design decision; do not replace
it with a generic folder-copy convention.
