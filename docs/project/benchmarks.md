---
title: Benchmarks and Performance — DataCoolie Project
description: Reproduce bounded local Polars and Spark workloads, verify benchmark result files, and interpret measurements within their recorded environment.
---

# Benchmarks

The [benchmark runner](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/runner/run_perf_benchmark.py)
compares selected local ETL workloads. This page explains reproduction and
result checks; dated results are examples, not a guarantee for another runtime.
Platform listing/read benchmarks are separate [opt-in tests](testing.md#real-cloud-integration-tests).

## Running

Use the [shared contributor environment](contributing.md#standard-local-environment).
Run commands from the product root containing `pyproject.toml` and `usecase-sim/`.
The input generator requires NumPy and PyArrow; the `dev` group supplies PyArrow.
For the first Polars run:

```powershell
poetry install --with dev -E polars-delta
poetry run python -m pip install numpy
poetry run python -c "import numpy, pyarrow, polars, deltalake; print(polars.__version__)"
poetry run python usecase-sim/runner/run_perf_benchmark.py --help
```

### First local workload

This bounded recipe generates only the 10K JSONL input and runs one
JSONL → Parquet overwrite case. It does not select Iceberg or require Docker.
Generation rewrites that simulator input; the timed case replaces its local
output under `usecase-sim/.runtime/data/perf/output/parquet/`.
Choose a new result directory for each measurement:

```powershell
poetry run python usecase-sim/scripts/generate_perf_data.py `
  --sizes 10k --formats jsonl --targets local
poetry run python usecase-sim/runner/run_perf_benchmark.py `
  --engine polars --stages perf_jsonl_parquet --max-size 10k --no-iceberg `
  --output-dir usecase-sim/.runtime/data/perf/benchmark_results/local-10k-001
```

Expect `polars_results.json` and a partial `perf_report.md` in that directory.
Confirm the JSON contains exactly the intended successful case:

```powershell
$check = @'
import json
from pathlib import Path

path = Path("usecase-sim/.runtime/data/perf/benchmark_results/local-10k-001/polars_results.json")
result = json.loads(path.read_text())
assert result["engine"] == "polars"
assert set(result["stages"]) == {"perf_jsonl_parquet"}
cases = result["stages"]["perf_jsonl_parquet"]
assert len(cases) == 1
case = cases[0]
assert case["dataflow"] == "perf_jsonl_parquet__10k"
assert case["size"] == "10k" and case["rows"] == 10000
assert case["status"] == "ok" and case["error"] is None
print("Expected benchmark case passed")
'@
poetry run python -c $check
```

### Full local comparison

The default stage selection includes Iceberg. Prepare local MinIO and the REST
catalog using the [simulator setup](https://github.com/datacoolie/datacoolie/tree/main/usecase-sim#2-prerequisites).
Add the Iceberg, AWS and Spark profiles, plus a Java runtime compatible with
your installed PySpark. The [Spark prerequisites](../guide/getting-started/installation.md#spark-prerequisites)
explain version and JVM artifact checks. The current Spark helper requests a
64 GB driver heap and always configures Iceberg support, even when
`--no-iceberg` skips those timed stages; review its resource/JAR requirements
before selecting Spark.

```powershell
poetry install --with dev -E polars-delta -E polars-iceberg -E aws -E spark-delta
poetry run python -m pip install numpy
poetry run python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest
poetry run python usecase-sim/scripts/generate_perf_data.py `
  --sizes 10k,50k,100k,500k,1m
```

Run each engine with the same stage/size selection and a clean output state:

```powershell
poetry run python usecase-sim/runner/run_perf_benchmark.py `
  --engine polars --max-size 1m --reset `
  --output-dir usecase-sim/.runtime/data/perf/benchmark_results/comparison-001
poetry run python usecase-sim/runner/run_perf_benchmark.py `
  --engine spark --max-size 1m --reset `
  --output-dir usecase-sim/.runtime/data/perf/benchmark_results/comparison-001
poetry run python usecase-sim/runner/run_perf_benchmark.py --report-only `
  --output-dir usecase-sim/.runtime/data/perf/benchmark_results/comparison-001
```

`--reset` clears simulator perf outputs locally, MinIO perf output prefixes and
the Iceberg `perf_dst` namespace. It preserves inputs and benchmark result files;
it is not a reset of only the chosen stage. Use the simulator's dedicated test
stores and review reset warnings before continuing. Generating inputs without
`--sizes` also includes 5M, 10M and 50M datasets; that is a larger workload than
this recipe. JSONL generation stops at 1M.

## What it measures

The [metadata](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/metadata/file/perf_test.json)
defines eight stages in seed/dependency order:

| Source → destination | Load type |
|---|---|
| JSONL → Parquet, JSONL → Delta | `overwrite` |
| JSONL → Iceberg | `merge_upsert` |
| Parquet → Parquet, Parquet → Delta | `overwrite` |
| Parquet → Iceberg | `merge_upsert` |
| Delta → Delta | `overwrite`; reads the seeded perf Delta output |
| Iceberg → Iceberg | `merge_upsert`; reads generated `perf_src` inputs |

This is a bounded workload set, not every format/load strategy supported by
DataCoolie. Selected derived stages need their inputs to exist; use the default
dependency order or prepare the seed separately when using `--stages`.

Each case records `dataflow`, `size`, nominal `rows`, `elapsed_s`, `status` and
`error`. The row count comes from the size label, not observed read/write counts.
Reported throughput is nominal rows divided by total dataflow elapsed time;
it does not separate read and write throughput. The runner does not measure
peak memory. Engine warmup is recorded separately as `session_warmup_s` and
excluded from timed dataflows. Each case has one timed sample per invocation.

## Verify and preserve results

Default outputs are under `usecase-sim/.runtime/data/perf/benchmark_results/`:
`polars_results.json`, `spark_results.json` and `perf_report.md`. A new engine
run overwrites that engine's JSON and regenerates the report from whichever
JSON files already exist. `--report-only` regenerates a report without measuring.

The process can exit `0` after recording case-level `failed` or `error` results.
A missing/unknown stage can also produce no cases. Before accepting a report:

1. Check the expected engine, stages, sizes and case count, not only file existence.
2. Require every selected case to have `status="ok"` and no error. Investigate
   partial, empty or `ERR` results; they are not successful performance evidence.
3. For comparisons, check both JSON timestamps and identical intended workloads.
   A result directory containing an old second-engine file can produce a stale
   comparison. Use a new directory for each comparison pair.
4. Preserve both JSON files with the report and record checkout revision,
   Python/engine/table-format versions, OS, hardware, JVM settings, workload,
   output reset state and repeated samples when making a performance claim.

The report's environment section is collected when it is rendered, including
with `--report-only`; it is not a complete per-engine execution manifest.
Record the environment at measurement time. Historical repository
`benchmark_results/` files, when present, retain their original provenance and
are not the current default output or a fresh qualification receipt.

## Interpretation caveats

- The driver runs one dataflow at a time, but this does not make the engine
  single-threaded. Polars and Spark can use internal parallelism.
- Spark uses `local[*]`; the numbers do not represent distributed cluster
  scaling, startup costs or managed cloud runtimes.
- Disk/cache state, CPU, input shape, versions and existing target state affect
  timings. Compare the same workload and repeat measurements before judging
  regressions; one warm sample is insufficient.
- Throughput and status alone do not prove persisted row/schema correctness.
  For that purpose use the relevant [datatype qualification](testing.md#datatype-qualification)
  and independent output assertions.
