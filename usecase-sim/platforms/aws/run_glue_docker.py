#!/usr/bin/env python3
"""
run_glue_docker.py — Launch sample_aws_glue_docker.py inside the Glue 5.0 container.

Usage:
  python run_glue_docker.py --stage read_file
  python run_glue_docker.py --stage "read_file,load_delta"
  python run_glue_docker.py                                                     # all stages
  python run_glue_docker.py --install datacoolie                                # pip package
  python run_glue_docker.py --install datacoolie==1.0.0                         # pinned
  python run_glue_docker.py --install dist/datacoolie-1.0.0-py3-none-any.whl   # wheel file
  python run_glue_docker.py --workers 4 --driver-memory 8g              # resource limits

How this mirrors cloud AWS Glue setup:
  --py-files  functions.zip                mirrors --extra-py-files  in cloud Glue job
  --install   <pkg or .whl>                mirrors --additional-python-modules in cloud Glue
  src/ via sys.path (default, Docker only) dev-friendly alternative — no install needed
  getResolvedOptions in script             identical to cloud Glue script arg parsing
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import zipfile
from pathlib import Path

from rich.pretty import pprint

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).parent.resolve()
# workspace = datacoolie/ root (3 levels up from this script)
_WORKSPACE_DEFAULT = (_THIS_DIR / "../../..").resolve()

_SCRIPT_IN_CONTAINER  = "/home/hadoop/workspace/usecase-sim/platforms/aws/sample_aws_glue_docker.py"
_FUNCZIP_IN_CONTAINER = "/home/hadoop/workspace/usecase-sim/platforms/aws/functions.zip"

# delta-spark Python bindings are NOT pre-installed in the Glue 5.0 image.
# Version must match the Delta JARs in the image: delta-spark_2.12-3.3.0-amzn-0.jar
_DELTA_SPARK_PKG = "delta-spark==3.3.0"


def _log(msg: str) -> None:
    """Print launcher-level message to stderr — separates from job/container output on stdout."""
    print(f"[glue-docker] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _pack_functions(functions_dir: Path, zip_path: Path) -> None:
    """Zip functions/ so spark-submit --py-files can distribute it (mirrors --extra-py-files)."""
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(functions_dir.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(functions_dir.parent))
    _log(f"packed {zip_path.name}  ({zip_path.stat().st_size // 1024} KB)")


def _is_wheel(install: str) -> bool:
    """True when install spec is a wheel file path (not a PyPI package name)."""
    return install.endswith(".whl")


def _container_path(host_path: str, workspace: Path) -> str:
    """Resolve a host wheel path to its container-absolute equivalent."""
    if host_path.startswith("/"):
        return host_path  # already container-absolute
    p = Path(host_path)
    if p.is_absolute():
        rel = p.relative_to(workspace)
    else:
        rel = Path(host_path)
    return f"/home/hadoop/workspace/{rel.as_posix()}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch sample_aws_glue_docker.py inside the AWS Glue 5.0 container.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--profile",    default=os.environ.get("AWS_PROFILE", "default"),
                        help="AWS named profile (default: $AWS_PROFILE or 'default')")
    parser.add_argument("--region",     default="ap-southeast-1")
    parser.add_argument("--bucket",     default="de-dev-0007")
    parser.add_argument("--stage",      default="", help="Comma-separated stages, or '' for all")
    parser.add_argument("--job-name",   default="local_docker_test")
    parser.add_argument("--workers",       default="*",
                        help="Spark local master thread count: '*' (all cores) or an integer. "
                             "Maps to --master local[N] in spark-submit. Default: '*'")
    parser.add_argument("--driver-memory", default="32g",
                        help="Spark driver memory (e.g. '4g', '8g', '32g'). "
                             "In local mode the driver is also the executor. Default: '32g'")
    parser.add_argument("--install", default="", metavar="PKG_OR_WHEEL",
                        help=(
                            "PyPI package spec (e.g. 'datacoolie' or 'datacoolie==1.0.0') "
                            "OR path to a wheel file relative to workspace root / absolute. "
                            "Mirrors --additional-python-modules in cloud Glue. "
                            "Omit to use volume-mounted src/ via sys.path (Docker dev mode)."
                        ))
    parser.add_argument("--workspace",  default="",
                        help="Absolute path to the datacoolie/ root (auto-detected).")
    a = parser.parse_args()

    workspace = Path(a.workspace).resolve() if a.workspace else _WORKSPACE_DEFAULT

    # -----------------------------------------------------------------------
    # Pack functions/ → functions.zip  (mirrors --extra-py-files)
    # -----------------------------------------------------------------------
    _pack_functions(_THIS_DIR / "functions", _THIS_DIR / "functions.zip")

    # -----------------------------------------------------------------------
    # Resolve install target (package name OR wheel → pip install spec)
    # -----------------------------------------------------------------------
    install = a.install.strip()
    if install and _is_wheel(install):
        pip_spec = _container_path(install, workspace)  # resolve wheel to container path
        _log(f"install mode: wheel  → {pip_spec}")
    elif install:
        pip_spec = install                              # raw package spec, pip resolves it
        _log(f"install mode: package → {pip_spec}")
    else:
        pip_spec = ""                                   # src mode — sys.path in script
        _log("install mode: src (sys.path)")

    stage_label = a.stage or "<all>"
    _log(f"profile={a.profile}  region={a.region}  bucket={a.bucket}  stage={stage_label}")
    _log(f"workers=local[{a.workers}]  driver-memory={a.driver_memory}")
    _log(f"always installing: {_DELTA_SPARK_PKG}  (Python bindings for Delta JARs in image)")

    # -----------------------------------------------------------------------
    # Spark conf
    # -----------------------------------------------------------------------
    spark_conf = [
        "--master", f"local[{a.workers}]",
        "--driver-memory", a.driver_memory,
        "--py-files", _FUNCZIP_IN_CONTAINER,
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        "--conf", "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
        "--conf", f"spark.sql.catalog.glue_catalog.warehouse=s3://{a.bucket}/output/iceberg/",
        "--conf", "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf", "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    ]

    script_args = [
        "--JOB_NAME", a.job_name,
        "--REGION",   a.region,
        "--BUCKET",   a.bucket,
        "--STAGE",    a.stage,
    ]

    # -----------------------------------------------------------------------
    # docker run base args
    # -----------------------------------------------------------------------
    aws_dir  = (Path.home() / ".aws").as_posix()
    wksp_dir = workspace.as_posix()

    # Persist pip download cache across --rm runs so delta-spark (and any
    # user --install package) is only fetched from PyPI once.
    pip_cache = workspace / ".pip-cache"
    pip_cache.mkdir(parents=True, exist_ok=True)

    docker_base = [
        "docker", "run", "--rm", "-it",
        "-v", f"{aws_dir}:/home/hadoop/.aws:ro",
        "-v", f"{wksp_dir}:/home/hadoop/workspace",
        "-v", f"{pip_cache.as_posix()}:/home/hadoop/.cache/pip",
        "-e", f"AWS_PROFILE={a.profile}",
        "-p", "4050:4040",
        "--name", "glue5_datacoolie",
        "public.ecr.aws/glue/aws-glue-libs:5",
    ]

    # Always install delta-spark Python bindings — the JARs are in the image but the
    # Python package is not. This mirrors --additional-python-modules delta-spark in cloud Glue.
    pkgs = [_DELTA_SPARK_PKG]
    if pip_spec:
        pkgs.append(pip_spec)

    all_args  = spark_conf + [_SCRIPT_IN_CONTAINER] + script_args
    spark_cmd = "spark-submit " + " ".join(f'"{ arg }"' for arg in all_args)
    pip_cmd   = "pip install --quiet " + " ".join(f'"{ p }"' for p in pkgs)
    bash_cmd  = f"{pip_cmd} && {spark_cmd}"

    # Entrypoint is "bash -l", so append "-c CMD" directly (not "bash -c CMD"),
    # which produces: bash -l -c "pip install ... && spark-submit ..."
    extra_env = ["-e", "DATACOOLIE_PIP_MODE=1"] if pip_spec else []
    cmd = docker_base + extra_env + ["-c", bash_cmd]
    pprint(cmd)
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == "__main__":
    main()
