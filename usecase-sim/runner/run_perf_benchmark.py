"""Performance benchmark: Polars vs Spark for DataCoolie ETL.

Runs selected stages from perf_test.json one dataflow at a time and
records wall-clock duration per (stage, size) combination.  When results
for both engines are present a markdown report is rendered.

Usage (from the datacoolie/ directory):
    # Polars — all non-iceberg stages up to 1 m rows
    python usecase-sim/runner/run_perf_benchmark.py \\
        --engine polars --max-size 1m --no-iceberg --reset

    # Spark — same
    python usecase-sim/runner/run_perf_benchmark.py \\
        --engine spark --max-size 1m --no-iceberg

    # All stages including iceberg (requires REST catalog + MinIO)
    python usecase-sim/runner/run_perf_benchmark.py --engine polars --max-size 1m

    # Render markdown report (requires both result files to exist)
    python usecase-sim/runner/run_perf_benchmark.py --report-only

    # Reset output tables before benchmarking (recommended)
    python usecase-sim/runner/run_perf_benchmark.py --engine polars --reset

Options:
    --engine          polars | spark
    --metadata-path   path to perf_test.json (default: ./usecase-sim/metadata/file/perf_test.json)
    --stages          comma-separated stage names (default: all 8 stages in dependency order)
    --max-size        largest size to run: 10k|50k|100k|500k|1m|5m|10m|50m (default: 1m)
    --output-dir      where to store JSON result files and the report (default: ./benchmark_results)
    --reset           call reset_perf_data.py before benchmarking
    --report-only     skip benchmarking; only regenerate the markdown report
    --no-iceberg      skip iceberg stages (useful when docker/REST catalog is unavailable)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_RUNNER_DIR = Path(__file__).resolve().parent
_USECASE_SIM_DIR = _RUNNER_DIR.parent
_DATACOOLIE_DIR = _USECASE_SIM_DIR.parent
_DEFAULT_METADATA = str(_USECASE_SIM_DIR / "metadata" / "file" / "perf_test.json")
_DEFAULT_OUTPUT_DIR = str(_DATACOOLIE_DIR / "benchmark_results")
_RESET_SCRIPT = str(_USECASE_SIM_DIR / "scripts" / "reset_perf_data.py")

sys.path.insert(0, str(_RUNNER_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("perf_benchmark")

# ---------------------------------------------------------------------------
# Size helpers
# ---------------------------------------------------------------------------
_SIZE_ORDER = ["10k", "50k", "100k", "500k", "1m", "5m", "10m", "50m"]
_SIZE_ROWS: dict[str, int] = {
    "10k": 10_000,
    "50k": 50_000,
    "100k": 100_000,
    "500k": 500_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
    "10m": 10_000_000,
    "50m": 50_000_000,
}

# All stages in dependency order:
# seed stages first (jsonl→*, parquet→*), then derived stages (delta→delta, iceberg→iceberg)
DEFAULT_STAGES = (
    "perf_jsonl_parquet,perf_jsonl_delta,perf_jsonl_iceberg,"
    "perf_parquet_parquet,perf_parquet_delta,perf_parquet_iceberg,"
    "perf_delta_delta,perf_iceberg_iceberg"
)


def _parse_size_from_name(dataflow_name: str) -> str | None:
    """Extract size suffix from a dataflow name like ``perf_parquet_delta__500k``."""
    if "__" not in dataflow_name:
        return None
    suffix = dataflow_name.rsplit("__", 1)[-1]
    return suffix if suffix in _SIZE_ROWS else None


def _size_rank(size: str) -> int:
    try:
        return _SIZE_ORDER.index(size)
    except ValueError:
        return 999


def _fmt_rows(rows: int) -> str:
    if rows >= 1_000_000:
        return f"{rows // 1_000_000:,}M"
    if rows >= 1_000:
        return f"{rows // 1_000:,}K"
    return str(rows)


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    mins = int(seconds) // 60
    secs = seconds - mins * 60
    return f"{mins}m {secs:.1f}s"


def _throughput(rows: int, seconds: float) -> str:
    if seconds <= 0:
        return "—"
    return f"{rows / seconds:,.0f} rows/s"


# ---------------------------------------------------------------------------
# Engine builder
# ---------------------------------------------------------------------------

def _build_polars_driver(metadata_path: str, needs_iceberg: bool = True):
    """Return a configured PolarsEngine DataCoolieDriver."""
    from _runner_utils import MINIO_STORAGE_OPTIONS, build_iceberg_rest_catalog  # noqa: PLC0415

    from datacoolie.core import DataCoolieRunConfig  # noqa: PLC0415
    from datacoolie.engines import PolarsEngine  # noqa: PLC0415
    from datacoolie.metadata import FileProvider  # noqa: PLC0415
    from datacoolie.orchestration import DataCoolieDriver  # noqa: PLC0415
    from datacoolie.platforms import LocalPlatform  # noqa: PLC0415
    from datacoolie.watermark import WatermarkManager  # noqa: PLC0415

    storage_opts = dict(MINIO_STORAGE_OPTIONS)
    platform = LocalPlatform()
    iceberg_catalog = None
    if needs_iceberg:
        # Large local MinIO writes can fail under pyiceberg's default
        # threaded writer fan-out. Keep the benchmark stable unless the
        # caller already provided an explicit override.
        os.environ.setdefault("PYICEBERG_MAX_WORKERS", "1")
        iceberg_catalog = build_iceberg_rest_catalog(
            catalog_preset="local",
            iceberg_catalog_uri=None,
            uc_token="",
            uc_credential="",
            storage_opts=storage_opts,
        )
    engine = PolarsEngine(
        platform=platform,
        storage_options=storage_opts,
        iceberg_catalog=iceberg_catalog,
    )
    metadata = FileProvider(config_path=metadata_path, platform=platform)
    watermark = WatermarkManager(metadata_provider=metadata)
    config = DataCoolieRunConfig(max_workers=1)  # single-threaded for fair comparison
    return DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=config,
    )


def _build_spark_driver(metadata_path: str):
    """Return a configured SparkEngine DataCoolieDriver."""
    from _runner_utils import build_spark_session  # noqa: PLC0415

    from datacoolie.core import DataCoolieRunConfig  # noqa: PLC0415
    from datacoolie.engines import SparkEngine  # noqa: PLC0415
    from datacoolie.metadata import FileProvider  # noqa: PLC0415
    from datacoolie.orchestration import DataCoolieDriver  # noqa: PLC0415
    from datacoolie.platforms import LocalPlatform  # noqa: PLC0415
    from datacoolie.watermark import WatermarkManager  # noqa: PLC0415

    spark = build_spark_session(
        app_name="DataCoolie-PerfBenchmark",
        catalog_preset="local",
        iceberg_catalog_uri=None,
        uc_token="",
        uc_credential="",
        needs_s3=False,
        needs_iceberg=True,
        extra_config=None,
    )
    platform = LocalPlatform()
    engine = SparkEngine(spark_session=spark, platform=platform)
    metadata = FileProvider(config_path=metadata_path, platform=platform)
    watermark = WatermarkManager(metadata_provider=metadata)
    config = DataCoolieRunConfig(max_workers=1)
    return DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=config,
    ), spark


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def _warmup_engine(engine_name: str, driver) -> float:
    """Warm up the engine's lazy initialisation without touching any benchmark data.

    Runs a trivial in-process computation to trigger JVM start (Spark) or
    first-module imports (Polars) so that per-dataflow elapsed_s reflects
    pure processing cost, not session startup overhead.

    Does NOT call ``driver.run()`` — this keeps all watermarks clean so every
    timed run processes a full dataset.
    Returns wall-clock seconds consumed.
    """
    t0 = time.perf_counter()

    if engine_name == "polars":
        try:
            import polars as pl  # noqa: PLC0415
            # Trigger lazy-collect machinery (covers first-import path).
            _ = pl.DataFrame({"_warm": [1, 2, 3]}).lazy().filter(pl.col("_warm") > 0).collect()
        except Exception:  # noqa: BLE001
            pass

    else:  # spark
        try:
            # driver._engine._spark is the SparkSession; trigger the first
            # query/plan so the JVM finishes any remaining lazy bootstrap.
            spark = driver._engine._spark  # type: ignore[attr-defined]
            _ = spark.range(1).collect()
        except Exception:  # noqa: BLE001
            # Fallback: try to access the SparkContext directly.
            try:
                spark = driver._engine._spark  # type: ignore[attr-defined]
                spark.sparkContext.parallelize([1]).collect()
            except Exception:  # noqa: BLE001
                pass

    return round(time.perf_counter() - t0, 3)


def run_benchmark(
    engine_name: str,
    metadata_path: str,
    stages: list[str],
    max_size: str,
    output_dir: Path,
    no_iceberg: bool,
) -> dict[str, Any]:
    """Run per-dataflow benchmark for one engine.  Returns the result dict."""
    from datacoolie.core import ColumnCaseMode  # noqa: PLC0415

    # Ensure all relative paths in perf_test.json resolve from the datacoolie/ root,
    # regardless of the shell working directory when this script was invoked.
    os.chdir(str(_DATACOOLIE_DIR))

    max_rank = _size_rank(max_size)
    results: dict[str, Any] = {
        "engine": engine_name,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "max_size": max_size,
        "session_warmup_s": None,
        "stages": {},
    }

    spark = None
    if engine_name == "polars":
        driver = _build_polars_driver(metadata_path, needs_iceberg=not no_iceberg)
    else:
        driver, spark = _build_spark_driver(metadata_path)

    try:
        # ---------------------------------------------------------------
        # Warmup: absorb engine lazy-init (SparkContext first-query cost,
        # Polars first-import overhead).  Time is stored separately and
        # excluded from all per-dataflow elapsed_s measurements.
        # Warmup does NOT call driver.run() — watermarks stay clean.
        # ---------------------------------------------------------------
        logger.info("Warming up %s engine (session startup excluded from timing)...", engine_name)
        warmup_s = _warmup_engine(engine_name, driver)
        results["session_warmup_s"] = warmup_s
        logger.info("Warmup complete: %.2fs (not counted in benchmark results)", warmup_s)

        for stage in stages:
            if no_iceberg and "iceberg" in stage:
                logger.info("Skipping iceberg stage %s (--no-iceberg)", stage)
                continue

            logger.info("=== Stage: %s [%s] ===", stage, engine_name)
            dataflows = driver.load_dataflows(stage=stage)
            if not dataflows:
                logger.warning("No dataflows found for stage %s", stage)
                continue

            stage_results: list[dict[str, Any]] = []
            for df in dataflows:
                size = _parse_size_from_name(df.name)
                if size is None:
                    logger.warning("Cannot parse size from %s — skipping", df.name)
                    continue
                if _size_rank(size) > max_rank:
                    logger.info("  Skipping %s (exceeds --max-size %s)", df.name, max_size)
                    continue

                rows = _SIZE_ROWS[size]
                logger.info("  Running %-40s  (%s rows) ...", df.name, f"{rows:,}")
                t0 = time.perf_counter()
                try:
                    r = driver.run(
                        dataflows=[df],
                        column_name_mode=ColumnCaseMode.LOWER,
                    )
                    elapsed = time.perf_counter() - t0
                    status = "ok" if not r.has_failures else "failed"
                    error = list(r.errors.values())[0] if r.errors else None
                except Exception as exc:  # noqa: BLE001
                    elapsed = time.perf_counter() - t0
                    status = "error"
                    error = str(exc)

                stage_results.append({
                    "dataflow": df.name,
                    "size": size,
                    "rows": rows,
                    "elapsed_s": round(elapsed, 3),
                    "status": status,
                    "error": error,
                })
                logger.info(
                    "  %-40s  %s  [%s]",
                    df.name,
                    _fmt_duration(elapsed),
                    status,
                )

            results["stages"][stage] = stage_results

    finally:
        driver.close()
        if spark is not None:
            try:
                spark.stop()
            except Exception:  # noqa: BLE001
                pass

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{engine_name}_results.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    logger.info("Results saved to %s", out_file)
    return results


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

def _load_results(output_dir: Path, engine: str) -> dict[str, Any] | None:
    p = output_dir / f"{engine}_results.json"
    if not p.exists():
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _stage_label(stage: str) -> str:
    return stage.replace("perf_", "").replace("_", " → ")


def generate_report(output_dir: Path) -> str:
    """Generate and write perf_report.md.  Returns the markdown string.

    Report is pivoted by **size** (primary) → **src→dest** (rows) → **engine** (columns)
    so that all three dimensions are visible in a single glance at each table.
    Session startup / warmup time is tracked separately and excluded from all
    per-dataflow elapsed_s measurements.
    """
    polars = _load_results(output_dir, "polars")
    spark = _load_results(output_dir, "spark")

    lines: list[str] = []
    lines.append("# DataCoolie Performance Benchmark: Polars vs Spark")
    lines.append("")
    lines.append(f"_Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_")
    lines.append("")

    # --- Environment ---
    lines.append("## Environment")
    lines.append("")
    lines.append("| Parameter | Value |")
    lines.append("|-----------|-------|")

    import platform as _platform  # noqa: PLC0415

    lines.append(f"| OS | {_platform.system()} {_platform.release()} |")
    lines.append(f"| Python | {sys.version.split()[0]} |")
    try:
        import pyspark  # noqa: PLC0415
        lines.append(f"| PySpark | {pyspark.__version__} |")
    except ImportError:
        lines.append("| PySpark | not installed |")
    try:
        import polars as pl  # noqa: PLC0415
        lines.append(f"| Polars | {pl.__version__} |")
    except ImportError:
        lines.append("| Polars | not installed |")
    try:
        import psutil  # noqa: PLC0415
        cpu_count = psutil.cpu_count(logical=True)
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        lines.append(f"| CPU logical cores | {cpu_count} |")
        lines.append(f"| RAM | {ram_gb:.1f} GB |")
    except Exception:  # noqa: BLE001
        pass

    if polars:
        warmup_p = polars.get("session_warmup_s")
        warmup_str = f" — warmup: {warmup_p:.2f}s (excluded)" if warmup_p is not None else ""
        lines.append(
            f"| Polars run | {polars['timestamp'][:19].replace('T',' ')} UTC"
            f" — max size: {polars['max_size']}{warmup_str} |"
        )
    if spark:
        warmup_s = spark.get("session_warmup_s")
        warmup_str = f" — warmup: {warmup_s:.2f}s (excluded)" if warmup_s is not None else ""
        lines.append(
            f"| Spark run | {spark['timestamp'][:19].replace('T',' ')} UTC"
            f" — max size: {spark['max_size']}{warmup_str} |"
        )
    lines.append("")

    # Collect all stages and sizes present in results
    all_stages: list[str] = []
    _seen_stages: set[str] = set()
    for _src in (polars, spark):
        if _src:
            for _st in _src.get("stages", {}):
                if _st not in _seen_stages:
                    all_stages.append(_st)
                    _seen_stages.add(_st)
    # Sort stages in dependency order as defined in DEFAULT_STAGES
    _stage_order = [s.strip() for s in DEFAULT_STAGES.replace("\\", "").split(",") if s.strip()]
    all_stages.sort(key=lambda s: _stage_order.index(s) if s in _stage_order else 999)

    all_sizes: list[str] = []
    _seen_sizes: set[str] = set()
    for _src in (polars, spark):
        if _src:
            for _st_data in _src.get("stages", {}).values():
                for _r in _st_data:
                    sz = _r.get("size", "")
                    if sz and sz not in _seen_sizes:
                        all_sizes.append(sz)
                        _seen_sizes.add(sz)
    all_sizes.sort(key=_size_rank)

    # Build index: results[engine][stage][size] = row dict
    def _index(result: dict | None) -> dict[str, dict[str, dict]]:
        idx: dict[str, dict[str, dict]] = {}
        if result is None:
            return idx
        for stage, rows in result.get("stages", {}).items():
            idx[stage] = {r["size"]: r for r in rows if "size" in r}
        return idx

    p_idx = _index(polars)
    s_idx = _index(spark)

    # ------------------------------------------------------------------
    # Section 1: Results by Size
    # Primary grouping = size; rows = src→dest; columns = Polars | Spark
    # ------------------------------------------------------------------
    lines.append("## Results by Size")
    lines.append("")
    lines.append(
        "Each table shows one size bucket.  "
        "Rows = src→dest pipeline.  "
        "Columns = engine.  "
        "**Session startup / warmup time is excluded from all elapsed_s values.**"
    )
    lines.append("")

    for size in all_sizes:
        rows_count = _SIZE_ROWS.get(size, 0)
        lines.append(f"### {size.upper()} ({_fmt_rows(rows_count)} rows)")
        lines.append("")
        lines.append(
            "| src → dest | Polars (s) | Polars rows/s | Spark (s) | Spark rows/s | Speedup (Polars/Spark) |"
        )
        lines.append(
            "|------------|----------:|--------------|----------:|-------------|---------------------|"
        )
        for stage in all_stages:
            p_r = p_idx.get(stage, {}).get(size)
            s_r = s_idx.get(stage, {}).get(size)

            def _cell_es(r: dict | None) -> tuple[str, str]:
                if r is None:
                    return "—", "—"
                if r["status"] != "ok":
                    return f"ERR", "—"
                e = r["elapsed_s"]
                return f"{e:.2f}", _throughput(rows_count, e)

            p_t, p_tp = _cell_es(p_r)
            s_t, s_tp = _cell_es(s_r)

            speedup = "—"
            if (
                p_r and s_r
                and p_r["status"] == "ok"
                and s_r["status"] == "ok"
                and p_r["elapsed_s"] > 0
                and s_r["elapsed_s"] > 0
            ):
                ratio = s_r["elapsed_s"] / p_r["elapsed_s"]
                if ratio >= 1:
                    speedup = f"**{ratio:.1f}× faster**"
                else:
                    speedup = f"{1 / ratio:.1f}× slower"

            label = _stage_label(stage)
            lines.append(f"| {label} | {p_t} | {p_tp} | {s_t} | {s_tp} | {speedup} |")
        lines.append("")

    # ------------------------------------------------------------------
    # Section 2: Speedup heatmap — size × src-dest
    # ------------------------------------------------------------------
    lines.append("## Speedup Matrix (Polars vs Spark)")
    lines.append("")
    lines.append(
        "Speedup = Spark elapsed / Polars elapsed. "
        "**Bold** = Polars faster; plain = Spark faster; — = data missing."
    )
    lines.append("")

    # header: stage labels as columns
    col_labels = [_stage_label(st) for st in all_stages]
    header = "| Size | " + " | ".join(col_labels) + " |"
    sep = "|------|" + "|".join(["-------"] * len(all_stages)) + "|"
    lines.append(header)
    lines.append(sep)

    for size in all_sizes:
        cells = []
        for stage in all_stages:
            p_r = p_idx.get(stage, {}).get(size)
            s_r = s_idx.get(stage, {}).get(size)
            if (
                p_r and s_r
                and p_r["status"] == "ok"
                and s_r["status"] == "ok"
                and p_r["elapsed_s"] > 0
            ):
                ratio = s_r["elapsed_s"] / p_r["elapsed_s"]
                if ratio >= 1:
                    cells.append(f"**{ratio:.1f}×**")
                else:
                    cells.append(f"{1 / ratio:.1f}× ↓")
            elif p_r and p_r["status"] == "ok" and not s_r:
                cells.append("P only")
            elif s_r and s_r["status"] == "ok" and not p_r:
                cells.append("S only")
            else:
                cells.append("—")
        lines.append(f"| {size} | " + " | ".join(cells) + " |")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 3: Session startup summary
    # ------------------------------------------------------------------
    lines.append("## Session Startup (excluded from benchmark timing)")
    lines.append("")
    lines.append(
        "Time consumed by engine initialisation before the first timed dataflow.  "
        "For Spark this includes SparkContext and first-query lazy init."
    )
    lines.append("")
    lines.append("| Engine | Warmup (s) |")
    lines.append("|--------|----------:|")
    if polars:
        ws = polars.get("session_warmup_s")
        lines.append(f"| Polars | {f'{ws:.2f}' if ws is not None else '—'} |")
    if spark:
        ws = spark.get("session_warmup_s")
        lines.append(f"| Spark  | {f'{ws:.2f}' if ws is not None else '—'} |")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 4: Totals per engine per src-dest (all sizes)
    # ------------------------------------------------------------------
    lines.append("## Total Time per Engine × src→dest (all sizes combined)")
    lines.append("")
    lines.append("| src → dest | Polars total | Spark total | Winner |")
    lines.append("|------------|-------------|------------|--------|")
    for stage in all_stages:
        p_data = polars["stages"].get(stage, []) if polars else []
        s_data = spark["stages"].get(stage, []) if spark else []
        p_total = sum(r["elapsed_s"] for r in p_data if r["status"] == "ok")
        s_total = sum(r["elapsed_s"] for r in s_data if r["status"] == "ok")
        p_str = _fmt_duration(p_total) if p_total > 0 else "—"
        s_str = _fmt_duration(s_total) if s_total > 0 else "—"
        if p_total > 0 and s_total > 0:
            if p_total < s_total:
                winner = f"✅ Polars {s_total / p_total:.1f}× faster"
            elif p_total > s_total:
                winner = f"✅ Spark {p_total / s_total:.1f}× faster"
            else:
                winner = "≈ tie"
        elif p_total > 0:
            winner = "Polars (no Spark data)"
        elif s_total > 0:
            winner = "Spark (no Polars data)"
        else:
            winner = "—"
        lines.append(f"| {_stage_label(stage)} | {p_str} | {s_str} | {winner} |")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 5: Notes
    # ------------------------------------------------------------------
    lines.append("## Notes")
    lines.append("")
    lines.append("- **Session startup excluded**: a warmup run (first 10k dataflow) is executed before")
    lines.append("  timing begins.  Its duration is recorded in *Session Startup* above but not in any")
    lines.append("  elapsed_s cell in this report.")
    lines.append("- **Polars** uses lazy evaluation; `.collect()` happens once at the write boundary.")
    lines.append("- **Spark** JVM is started in `_build_spark_driver`; SparkContext lazy-init is")
    lines.append("  absorbed by the warmup run.")
    lines.append("- All runs use `max_workers=1` (single-threaded) for a fair serial comparison.")
    lines.append("- Iceberg stages use `merge_upsert`; overwrite stages use full replace.")
    lines.append("- Delta stages depend on parquet→delta having run first (seed dependency).")
    lines.append("- Iceberg stages require the local REST catalog (tabulario/iceberg-rest) + MinIO.")
    lines.append("")

    report = "\n".join(lines)
    report_path = output_dir / "perf_report.md"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info("Report written to %s", report_path)
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DataCoolie Polars vs Spark performance benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--engine", choices=["polars", "spark"], help="Engine to benchmark")
    parser.add_argument("--metadata-path", default=_DEFAULT_METADATA)
    parser.add_argument("--stages", default=DEFAULT_STAGES, help="Comma-separated stage names")
    parser.add_argument(
        "--max-size",
        default="1m",
        choices=_SIZE_ORDER,
        help="Largest dataflow size to run (default: 1m)",
    )
    parser.add_argument("--output-dir", default=_DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reset", action="store_true", help="Reset output data before benchmarking")
    parser.add_argument("--report-only", action="store_true", help="Only regenerate the markdown report")
    parser.add_argument("--no-iceberg", action="store_true", help="Skip iceberg stages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    if args.report_only:
        report = generate_report(output_dir)
        print("\n" + "=" * 60)
        print(f"Report: {output_dir / 'perf_report.md'}")
        sys.exit(0)

    if not args.engine:
        print("error: --engine is required (or use --report-only)", file=sys.stderr)
        sys.exit(1)

    if args.reset:
        logger.info("Resetting output data …")
        subprocess.run(
            [sys.executable, _RESET_SCRIPT],
            check=False,
            cwd=str(_DATACOOLIE_DIR),
        )

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]

    run_benchmark(
        engine_name=args.engine,
        metadata_path=args.metadata_path,
        stages=stages,
        max_size=args.max_size,
        output_dir=output_dir,
        no_iceberg=args.no_iceberg,
    )

    # Re-generate report whenever a new result is saved.
    polars_ok = (output_dir / "polars_results.json").exists()
    spark_ok = (output_dir / "spark_results.json").exists()
    if polars_ok or spark_ok:
        generate_report(output_dir)
        if not (polars_ok and spark_ok):
            missing = "spark" if polars_ok else "polars"
            logger.info(
                "Report generated with partial data — run --engine %s to complete the comparison.",
                missing,
            )


if __name__ == "__main__":
    main()
