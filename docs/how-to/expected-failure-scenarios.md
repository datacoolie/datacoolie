# Expected-failure scenarios

**Prerequisites** · Using the `usecase-sim` runner or wrapping `usecase-sim/runner/run.py` in your own test harness.
**End state** · CI-friendly assertions that a specific run **fails** and surfaces the expected error text.

Some tests are meant to prove that the framework rejects bad configuration or
bad data. Treat these as first-class negative tests.

## Current status

The current `usecase-sim` runner does **not** read scenario metadata such as
`validation.expect_success` or `validation.expected_error_contains`.

- `usecase-sim/runner/run_scenario.py` passes or fails scenarios from the child
  process exit code.
- `usecase-sim/runner/run.py` exits `0` on success and `2` on execution
  failure.
- `usecase-sim/scenarios/scenarios.json` does not currently define a built-in
  expected-failure schema.

So if you want an expected-failure scenario today, assert it in the wrapper or
CI job that launches the runner.

## Recommended pattern

1. Run the stage or scenario normally.
2. Expect a non-zero exit code.
3. Assert that stdout, stderr, or the scenario console log contains the error
   substring you expect.

Example in Python:

```python
import subprocess

proc = subprocess.run(
    [
        "python",
        "usecase-sim/runner/run.py",
        "--engine", "polars",
        "--metadata-source", "file",
        "--metadata-path", "usecase-sim/metadata/file/local_use_cases.json",
        "--stage", "bad_schema_stage",
    ],
    capture_output=True,
    text=True,
)

combined_output = proc.stdout + proc.stderr

assert proc.returncode != 0
assert "Column 'nonexistent' not found" in combined_output
```

If you use `run_scenario.py`, inspect the per-scenario console log written to
`usecase-sim/logs/scenarios/<scenario>.console.log`.

## Recording failures in ETL logs

Failed runs still produce `dataflow_run_log` rows with `status = "failed"`.
The current logging pipeline does not mark a failure as "expected"
automatically, so downstream dashboards need a separate convention if you want
to suppress alerts for negative tests.
