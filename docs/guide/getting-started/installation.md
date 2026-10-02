---
title: Install DataCoolie for a first local run
description: Create an isolated Python environment, install the engine and table-format extras, and verify the interpreter you will use.
---

# Installation

Use one virtual environment per project. The commands below bind `pip` to the
same interpreter that will run the example, which avoids installing the extra
into a different Python installation.

## Pick an engine

| First goal | Install |
|---|---|
| Fastest local Delta run | `datacoolie[polars-delta]` |
| Local Spark + Delta run | `datacoolie[spark-delta]` |
| Validate/build a project archive | add `cli` |

For the canonical project, install one of these profiles:

```bash
python -m pip install "datacoolie[cli,polars-delta]==0.2.0"
# or
python -m pip install "datacoolie[cli,spark-delta]==0.2.0"
```

The examples target the current checkout/release line `0.2.0`. Check the
installed package before following a recipe:

```bash
python -c "import datacoolie, sys; print(sys.executable); print(datacoolie.__version__); print(datacoolie.__file__)"
python -m pip show datacoolie
```

The file path should be the interpreter's installed package, not an unrelated
checkout when you are testing a downloaded project. A source checkout can be
used deliberately during development; record that boundary in the run log.

If `0.2.0` is not published yet, do not silently substitute an older release.
From the repository directory that contains `pyproject.toml`, build and install
the matching wheel from the same source revision instead:

```bash
python -m pip install --upgrade build
python -m build --wheel
python -m pip install --force-reinstall \
  "dist/datacoolie-0.2.0-py3-none-any.whl[cli,polars-delta]"
# For the Spark route, use this wheel target instead:
# "dist/datacoolie-0.2.0-py3-none-any.whl[cli,spark-delta]"
```

The isolated build installs its declared build backend, and the wheel target
includes the CLI plus the selected engine/table-format extras. The wheel
command is the preview/source handoff used by this guide. It keeps the package
identity at `0.2.0`; the extracted example project remains outside the source
checkout and must still report `datacoolie.__file__` before a run.
An editable install (`python -m pip install -e .`) is for framework
development only and is not the packaged-project qualification path.

## Create the environment

Linux or macOS:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "datacoolie[cli,polars-delta]==0.2.0"
```

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install "datacoolie[cli,polars-delta]==0.2.0"
```

If PowerShell blocks script activation, run the interpreter directly instead:

```powershell
.\.venv\Scripts\python.exe -m pip install "datacoolie[cli,polars-delta]==0.2.0"
```

## Spark prerequisites

The Spark route additionally needs a Java runtime supported by your installed
PySpark version. Check both pieces before starting a Spark session:

```bash
java -version
python -c "import pyspark, delta; print(pyspark.__version__); print(delta.__file__)"
```

The reviewed local candidate is Python 3.11.9, PySpark 4.1.1,
`delta-spark` 4.2.0 and Java 17.0.12. This is a bounded local receipt, not a
claim that every version allowed by the package range is qualified. Delta's
Spark helper may download JVM artifacts on first start, so the first Spark run
needs network access or a populated dependency cache.

## Validate the downloaded project

After extracting `getting-started.zip`, run these commands from the directory
that contains `getting-started/`:

```bash
dc --project-dir getting-started validate --env local --format json
dc --project-dir getting-started inspect metadata --env local --section dataflows --full --format json
dc --project-dir getting-started build --format json
```

Validation checks the project metadata and runner layout. Build creates a
reusable environment artifact; it does not execute a dataflow or upload the
CSV input. For the first direct-Python run, use the source project directory
and the runner under `runners/local/`.

## Verify capability, not only registration

The plugin registry is an inventory. A listed engine or format can still be
missing an optional import. The quickstarts verify the selected engine by
performing a real CSV-to-Delta run and an independent Delta readback.

If import or startup fails, check the interpreter path, the selected extra,
Java/PySpark for Spark, and the table-format dependency before changing the
metadata. Continue to [Quickstart · Polars](quickstart-polars.md) or
[Quickstart · Spark](quickstart-spark.md) once the selected runtime is
available.
