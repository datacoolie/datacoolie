# DataCoolie Performance Benchmark: Polars vs Spark

_Generated: 2026-04-27 09:57 UTC_

## Environment

| Parameter | Value |
|-----------|-------|
| OS | Windows 10 |
| Python | 3.11.9 |
| PySpark | 3.5.8 |
| Polars | 1.39.3 |
| CPU logical cores | 12 |
| RAM | 30.7 GB |
| Polars run | 2026-04-27 09:43:27 UTC — max size: 50m — warmup: 0.03s (excluded) |
| Spark run | 2026-04-27 09:49:17 UTC — max size: 50m — warmup: 3.98s (excluded) |

## Results by Size

Each table shows one size bucket.  Rows = src→dest pipeline.  Columns = engine.  **Session startup / warmup time is excluded from all elapsed_s values.**

### 10K (10K rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | 0.10 | 98,039 rows/s | 6.97 | 1,436 rows/s | **68.3× faster** |
| jsonl → delta | 0.14 | 70,922 rows/s | 6.02 | 1,661 rows/s | **42.7× faster** |
| jsonl → iceberg | 1.34 | 7,457 rows/s | 3.82 | 2,615 rows/s | **2.9× faster** |
| parquet → parquet | 0.02 | 416,667 rows/s | 0.59 | 16,835 rows/s | **24.8× faster** |
| parquet → delta | 0.10 | 103,093 rows/s | 2.32 | 4,312 rows/s | **23.9× faster** |
| parquet → iceberg | 1.35 | 7,386 rows/s | 0.94 | 10,672 rows/s | 1.4× slower |
| delta → delta | 0.37 | 27,248 rows/s | 2.63 | 3,802 rows/s | **7.2× faster** |
| iceberg → iceberg | 1.14 | 8,772 rows/s | 1.42 | 7,047 rows/s | **1.2× faster** |

### 50K (50K rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | 0.03 | 1,515,152 rows/s | 1.00 | 50,150 rows/s | **30.2× faster** |
| jsonl → delta | 0.11 | 471,698 rows/s | 2.88 | 17,367 rows/s | **27.2× faster** |
| jsonl → iceberg | 0.71 | 70,423 rows/s | 0.86 | 57,803 rows/s | **1.2× faster** |
| parquet → parquet | 0.02 | 2,272,727 rows/s | 0.52 | 96,899 rows/s | **23.5× faster** |
| parquet → delta | 0.09 | 531,915 rows/s | 1.99 | 25,164 rows/s | **21.1× faster** |
| parquet → iceberg | 0.87 | 57,537 rows/s | 0.65 | 77,160 rows/s | 1.3× slower |
| delta → delta | 0.13 | 373,134 rows/s | 2.13 | 23,430 rows/s | **15.9× faster** |
| iceberg → iceberg | 0.77 | 65,020 rows/s | 1.28 | 39,062 rows/s | **1.7× faster** |

### 100K (100K rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | 0.05 | 1,960,784 rows/s | 0.81 | 123,916 rows/s | **15.8× faster** |
| jsonl → delta | 0.12 | 826,446 rows/s | 2.71 | 36,900 rows/s | **22.4× faster** |
| jsonl → iceberg | 0.78 | 127,551 rows/s | 0.96 | 104,058 rows/s | **1.2× faster** |
| parquet → parquet | 0.03 | 3,225,806 rows/s | 0.43 | 229,885 rows/s | **14.0× faster** |
| parquet → delta | 0.13 | 763,359 rows/s | 2.08 | 48,170 rows/s | **15.8× faster** |
| parquet → iceberg | 0.91 | 109,769 rows/s | 0.61 | 164,474 rows/s | 1.5× slower |
| delta → delta | 0.22 | 454,545 rows/s | 2.38 | 42,017 rows/s | **10.8× faster** |
| iceberg → iceberg | 0.86 | 116,009 rows/s | 1.05 | 95,602 rows/s | **1.2× faster** |

### 500K (500K rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | 0.18 | 2,747,253 rows/s | 1.40 | 358,423 rows/s | **7.7× faster** |
| jsonl → delta | 0.38 | 1,305,483 rows/s | 4.20 | 119,019 rows/s | **11.0× faster** |
| jsonl → iceberg | 1.28 | 390,930 rows/s | 1.89 | 264,690 rows/s | **1.5× faster** |
| parquet → parquet | 0.07 | 7,246,377 rows/s | 0.72 | 698,324 rows/s | **10.4× faster** |
| parquet → delta | 0.35 | 1,420,455 rows/s | 3.20 | 156,104 rows/s | **9.1× faster** |
| parquet → iceberg | 1.53 | 325,733 rows/s | 0.99 | 503,018 rows/s | 1.5× slower |
| delta → delta | 0.52 | 954,198 rows/s | 3.88 | 128,999 rows/s | **7.4× faster** |
| iceberg → iceberg | 1.39 | 359,195 rows/s | 1.63 | 306,937 rows/s | **1.2× faster** |

### 1M (1M rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | 0.40 | 2,525,253 rows/s | 2.06 | 484,262 rows/s | **5.2× faster** |
| jsonl → delta | 0.69 | 1,453,488 rows/s | 6.75 | 148,038 rows/s | **9.8× faster** |
| jsonl → iceberg | 1.70 | 588,235 rows/s | 2.81 | 355,999 rows/s | **1.7× faster** |
| parquet → parquet | 0.11 | 9,174,312 rows/s | 1.09 | 914,913 rows/s | **10.0× faster** |
| parquet → delta | 0.56 | 1,795,332 rows/s | 4.60 | 217,486 rows/s | **8.3× faster** |
| parquet → iceberg | 1.92 | 521,376 rows/s | 1.33 | 750,751 rows/s | 1.4× slower |
| delta → delta | 0.79 | 1,267,427 rows/s | 5.29 | 189,215 rows/s | **6.7× faster** |
| iceberg → iceberg | 1.77 | 563,698 rows/s | 2.23 | 449,438 rows/s | **1.3× faster** |

### 5M (5M rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | — | — | — | — | — |
| jsonl → delta | — | — | — | — | — |
| jsonl → iceberg | — | — | — | — | — |
| parquet → parquet | 0.53 | 9,398,496 rows/s | 3.38 | 1,481,043 rows/s | **6.3× faster** |
| parquet → delta | 2.43 | 2,056,767 rows/s | 25.11 | 199,132 rows/s | **10.3× faster** |
| parquet → iceberg | 7.47 | 669,434 rows/s | 4.07 | 1,228,803 rows/s | 1.8× slower |
| delta → delta | 2.40 | 2,082,466 rows/s | 20.14 | 248,250 rows/s | **8.4× faster** |
| iceberg → iceberg | 5.55 | 900,739 rows/s | 4.10 | 1,219,512 rows/s | 1.4× slower |

### 10M (10M rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | — | — | — | — | — |
| jsonl → delta | — | — | — | — | — |
| jsonl → iceberg | — | — | — | — | — |
| parquet → parquet | 1.48 | 6,761,325 rows/s | 7.98 | 1,252,819 rows/s | **5.4× faster** |
| parquet → delta | 4.66 | 2,145,923 rows/s | 28.09 | 356,024 rows/s | **6.0× faster** |
| parquet → iceberg | 14.42 | 693,529 rows/s | 8.00 | 1,250,156 rows/s | 1.8× slower |
| delta → delta | 4.57 | 2,188,184 rows/s | 22.36 | 447,287 rows/s | **4.9× faster** |
| iceberg → iceberg | 9.62 | 1,039,177 rows/s | 7.71 | 1,296,680 rows/s | 1.2× slower |

### 50M (50M rows)

| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |
|------------|----------:|--------------|----------:|-------------|---------------------|
| jsonl → parquet | — | — | — | — | — |
| jsonl → delta | — | — | — | — | — |
| jsonl → iceberg | — | — | — | — | — |
| parquet → parquet | 7.24 | 6,906,077 rows/s | 30.31 | 1,649,729 rows/s | **4.2× faster** |
| parquet → delta | 29.09 | 1,718,686 rows/s | 64.12 | 779,764 rows/s | **2.2× faster** |
| parquet → iceberg | 75.49 | 662,339 rows/s | 37.12 | 1,346,801 rows/s | 2.0× slower |
| delta → delta | 24.55 | 2,036,411 rows/s | 54.76 | 913,059 rows/s | **2.2× faster** |
| iceberg → iceberg | 59.68 | 837,830 rows/s | 36.71 | 1,362,064 rows/s | 1.6× slower |

## Speedup Matrix (Polars vs Spark)

Speedup = Spark elapsed / Polars elapsed. **Bold** = Polars faster; plain = Spark faster; — = data missing.

| Size | jsonl → parquet | jsonl → delta | jsonl → iceberg | parquet → parquet | parquet → delta | parquet → iceberg | delta → delta | iceberg → iceberg |
|------|-------|-------|-------|-------|-------|-------|-------|-------|
| 10k | **68.3×** | **42.7×** | **2.9×** | **24.8×** | **23.9×** | 1.4× ↓ | **7.2×** | **1.2×** |
| 50k | **30.2×** | **27.2×** | **1.2×** | **23.5×** | **21.1×** | 1.3× ↓ | **15.9×** | **1.7×** |
| 100k | **15.8×** | **22.4×** | **1.2×** | **14.0×** | **15.8×** | 1.5× ↓ | **10.8×** | **1.2×** |
| 500k | **7.7×** | **11.0×** | **1.5×** | **10.4×** | **9.1×** | 1.5× ↓ | **7.4×** | **1.2×** |
| 1m | **5.2×** | **9.8×** | **1.7×** | **10.0×** | **8.3×** | 1.4× ↓ | **6.7×** | **1.3×** |
| 5m | — | — | — | **6.3×** | **10.3×** | 1.8× ↓ | **8.4×** | 1.4× ↓ |
| 10m | — | — | — | **5.4×** | **6.0×** | 1.8× ↓ | **4.9×** | 1.2× ↓ |
| 50m | — | — | — | **4.2×** | **2.2×** | 2.0× ↓ | **2.2×** | 1.6× ↓ |

## Session Startup (excluded from benchmark timing)

Time consumed by engine initialisation before the first timed dataflow.  For Spark this includes SparkContext and first-query lazy init.

| Engine | Warmup (s) |
|--------|----------:|
| Polars | 0.03 |
| Spark  | 3.98 |

## Total Time per Engine × src→dest (all sizes combined)

| src → dest | Polars total | Spark total | Winner |
|------------|-------------|------------|--------|
| jsonl → parquet | 0.76s | 12.23s | ✅ Polars 16.0× faster |
| jsonl → delta | 1.44s | 22.56s | ✅ Polars 15.7× faster |
| jsonl → iceberg | 5.81s | 10.35s | ✅ Polars 1.8× faster |
| parquet → parquet | 9.51s | 45.02s | ✅ Polars 4.7× faster |
| parquet → delta | 37.41s | 2m 11.5s | ✅ Polars 3.5× faster |
| parquet → iceberg | 1m 44.0s | 53.71s | ✅ Spark 1.9× faster |
| delta → delta | 33.56s | 1m 53.6s | ✅ Polars 3.4× faster |
| iceberg → iceberg | 1m 20.8s | 56.12s | ✅ Spark 1.4× faster |

## Notes

- **Session startup excluded**: a warmup run (first 10k dataflow) is executed before
  timing begins.  Its duration is recorded in *Session Startup* above but not in any
  elapsed_s cell in this report.
- **Polars** uses lazy evaluation; `.collect()` happens once at the write boundary.
- **Spark** JVM is started in `_build_spark_driver`; SparkContext lazy-init is
  absorbed by the warmup run.
- All runs use `max_workers=1` (single-threaded) for a fair serial comparison.
- Iceberg stages use `merge_upsert`; overwrite stages use full replace.
- Delta stages depend on parquet→delta having run first (seed dependency).
- Iceberg stages require the local REST catalog (tabulario/iceberg-rest) + MinIO.
