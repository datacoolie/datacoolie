---
title: Expected Failure Scenarios — DataCoolie How-to
description: Model and validate expected DataCoolie failures so scenario runners and output checks can distinguish broken runs from planned negative tests.
---

# Expected-failure scenarios

**Prerequisites** · Using the `usecase-sim` runner or wrapping `usecase-sim/runner/run.py` in your own test harness.
**End state** · CI-friendly assertions that a specific run **fails** and surfaces the expected error text.

Some tests are meant to prove that the framework rejects bad configuration or
bad data. Treat these as first-class negative tests.

## Scenario contract

`usecase-sim/runner/run_scenario.py` supports declarative validation. The
runner still returns `124` for a timeout, but other non-zero child exits can be
declared as expected and must also match stable console text.

## Recommended pattern

Add a `validation` block to the scenario:

```json
{
  "validation": {
    "expected_exit_code": 2,
    "required_console_text": ["Column 'missing_order' not found"]
  }
}
```

The scenario passes only when the exit code and every required substring
match. Positive scenarios can additionally set a repository-local `script`,
`args`, and validator `timeout_seconds` to assert persisted outputs.

## Recording failures in ETL logs

Failed runs still produce `dataflow_run_log` rows with `status = "failed"`.
The row retains source, transform, or destination status, error details, and
partial timings that were available before the exception. On a scenario
timeout, the runner first signals the child and gives it 120 seconds to call
`driver.close()` and flush logs before hard-killing it.
The current logging pipeline does not mark a failure as "expected"
automatically, so downstream dashboards need a separate convention if you want
to suppress alerts for negative tests.
