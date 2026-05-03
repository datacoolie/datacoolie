---
title: Benchmarks and Performance — DataCoolie Operations
description: Compare DataCoolie Polars and Spark benchmark results, workload profiles, and when each engine is a better operational fit.
---

# Benchmarks

DataCoolie ships a reproducible benchmark harness under
[`usecase-sim/runner/run_perf_benchmark.py`](https://github.com/datacoolie/closed-loop/blob/main/datacoolie/usecase-sim/runner/run_perf_benchmark.py).

## Running

```powershell
python datacoolie/usecase-sim/runner/run_perf_benchmark.py --engine polars
python datacoolie/usecase-sim/runner/run_perf_benchmark.py --engine spark
```

Outputs JSON results to `datacoolie/benchmark_results/`:

- `polars_results.json`
- `spark_results.json`
- `perf_report.md` — markdown summary (regenerated on each run).

## What it measures

The harness exercises each load type and each format against synthetic data
sized to match typical ingestion workloads. Per-run metrics:

- Rows read / written
- Wall-clock duration
- Read throughput (rows / s)
- Write throughput (rows / s)
- Peak memory (Linux only — via `resource`)

## Published report

See [`benchmark_results/perf_report.md`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/benchmark_results/perf_report.md)
for the last committed run.

## Interpretation caveats

- Numbers depend on disk type, CPU, and Spark cluster size — treat them as
  **relative comparisons**, not absolute guarantees.
- Polars is single-node; Spark numbers come from a `local[*]` driver which
  is not representative of cluster performance.
- Iceberg and Delta have different merge characteristics at scale — the
  harness runs both.
