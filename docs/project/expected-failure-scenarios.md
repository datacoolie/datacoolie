---
title: Expected Failure Scenarios — DataCoolie Project
description: Model and validate expected DataCoolie failures so scenario runners and output checks can distinguish broken runs from planned negative tests.
---

# Expected-failure scenarios

**Prerequisites** · Run the checked-in `usecase-sim` scenario dispatcher from
the [product root with the shared contributor environment](contributing.md#standard-local-environment).
**End state** · The child run fails as expected, the validator checks the
failure evidence, and the scenario command itself returns success.

Some tests are meant to prove that the framework rejects bad configuration or
bad data. Treat these as first-class negative tests.

## Scenario contract

`usecase-sim/runner/run_scenario.py` supports declarative validation.
Declare the child `run.py` exit on `invocations[].expected_exit_code`. After all
invocations match their expected exits, the dispatcher applies the final console
and validator checks and returns `0` for a passing scenario. Keep final
`validation.expected_exit_code` at `0` (the default): the dispatcher supplies
`0` to that final validation stage. Putting a nonzero expected child exit only
in the top-level validation makes the final check fail. The child exit and
outer scenario exit are different assertions. Timeout is reported as `124`
by the child-execution helper and causes scenario failure.

## Recommended pattern

The checked-in `local_polars_startup_failure` scenario uses this validation
block:

```json
{
  "invocations": [{"label": "startup_failure", "expected_exit_code": 1}],
  "validation": {
    "expected_exit_code": 0,
    "required_console_text": ["DataCoolie session startup failed"],
    "script": "usecase-sim/scripts/validate_startup_failure_logs.py",
    "args": ["--job-id", "usecase-startup-failure"]
  }
}
```

`required_console_text` is optional. If it is present, every configured
substring must occur in the captured console log. A repository-local `script`
is also allowed for negative scenarios; the dispatcher runs it after the child
exit and console checks, from the checkout root, with the listed `args` and the
configured `timeout_seconds` (60 seconds by default). Use that validator for
structured evidence that cannot be established from console text alone.

## Runnable local startup failure

`local_polars_startup_failure` is a no-service negative test. It reads the
checked-in malformed input
[`startup_failure.json`](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/metadata/file/startup_failure.json),
whose `dataflows` value is deliberately a string, and uses the local Polars
engine. A Poetry development environment with the `polars` profile is enough;
Docker, cloud credentials, generated data, and metadata seeding are not
prerequisites.

From the product root, install the profile in that environment and run:

```powershell
poetry install --with dev -E polars
poetry run python usecase-sim/runner/run_scenario.py `
  --scenario local_polars_startup_failure
```

The child `run.py` is expected to exit `1` after startup validation. The
scenario dispatcher then runs
[`validate_startup_failure_logs.py`](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/scripts/validate_startup_failure_logs.py)
and returns `0` only when that validation succeeds. The dispatcher captures
these receipts:

- `usecase-sim/.runtime/logs/scenarios/local_polars_startup_failure.console.log`
- `usecase-sim/.runtime/logs/scenarios/local_polars_startup_failure.invocations.json`

The validator scans `usecase-sim/.runtime/logs` recursively for the job id. It requires a
`session.starting` event, a `session.startup_failed` event, exactly one
`job_run_log` record with `status = "failed"`, and no `dataflow_run_log` record.
The absence of a dataflow record is part of the startup contract: failure
happens before any dataflow runtime begins. Framework JSONL records are under
`usecase-sim/.runtime/logs/system_logs/` and `usecase-sim/.runtime/logs/execution_logs/`; their exact
partitioned filenames are runtime-generated.

## Recording failures in execution logs

Failures after a dataflow has started can produce `dataflow_run_log` rows with
`status = "failed"`, including the source, transform, or destination status,
error details, and partial timings available before the exception. A startup
failure is earlier: it produces the failed `job_run_log` checked by the
validator and no `dataflow_run_log`. On a scenario timeout, the runner first
signals the child and gives it 120 seconds to call `driver.close()` and flush
logs before hard-killing it.
The current logging pipeline does not mark a failure as "expected"
automatically, so downstream dashboards need a separate convention if you want
to suppress alerts for negative tests.
