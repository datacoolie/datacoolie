---
title: Operations — Running DataCoolie in Production
description: "Production guidance for DataCoolie: logging layout and file partitioning, Polars vs Spark benchmarks, troubleshooting common failures, and testing strategy."
---

# Operations

Practical guidance for running DataCoolie pipelines reliably in production
environments, from understanding log output to diagnosing failures.

## What's in this section

- [Logging layout](logging-layout.md) — How the ETL logger writes debug JSONL
  and analyst Parquet files, what the `LogPurpose` values mean, and how output
  is partitioned under `<output_path>/<purpose>/<log_type>/`.
- [Benchmarks](benchmarks.md) — Polars vs Spark throughput and latency numbers
  from the reference `usecase-sim` testbed. Helps you choose the right engine
  for your row-count and latency targets.
- [Troubleshooting](troubleshooting.md) — Common failure patterns and how to
  diagnose them: watermark staleness, metadata provider errors, merge key
  mismatches, platform credential issues, and partition path conflicts.
- [Testing strategy](testing-strategy.md) — How the DataCoolie test suite is
  structured, coverage gates, mock engine patterns, and how to add tests for
  custom plugins.

## Quick checklist

Before running a pipeline in a new environment:

1. Verify the platform (`LocalPlatform`, `AWSPlatform`, etc.) can reach its
   file paths and resolve secrets.
2. Check that the metadata provider is reachable and returns at least one active
   dataflow.
3. Confirm the engine has the required extras installed (`polars`, `spark`, etc.).
4. Review the logging output path and ensure the destination directory is
   writable with the expected partition structure.
