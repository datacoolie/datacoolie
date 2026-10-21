"""Render installation requirements from the package manifest."""

from __future__ import annotations

from pathlib import Path
import tomllib


PROFILE_NOTES = {
    "spark": "Spark engine; provide a compatible Java runtime separately.",
    "polars": "Polars engine without lakehouse-format dependencies.",
    "polars-sql": "Polars SQL with qualified relation rewriting.",
    "polars-hash": "Optional Polars hashing dependency.",
    "spark-delta": "Spark engine and Delta format support; Java/JVM artifacts are separate.",
    "polars-delta": "Polars engine and path-based Delta reads/writes.",
    "polars-iceberg": "Polars engine and catalog-backed Iceberg support; configure a catalog.",
    "aws": "AWS platform SDK; configure credentials and permissions separately.",
    "fabric-external": "Fabric platform outside its native runtime using Azure SDK backends.",
    "databricks-external": "Databricks platform outside its native runtime using the SDK.",
    "source-api": "REST API source transport; combine with an engine profile.",
    "source-excel-polars": "Excel data reads with Polars; separate from Excel metadata.",
    "source-db-polars": "Polars ConnectorX database reads; PostgreSQL uses this by default.",
    "source-db-native-polars": "Polars native MySQL/MSSQL password reads. Native PostgreSQL requires a separately installed psycopg2 driver.",
    "source-db-oracle-polars": "Polars native Oracle reads with oracledb.",
    "source-db-mssql-odbc-polars": "Polars MSSQL token/identity reads. Install system ODBC Driver 18 separately.",
    "metadata-yaml": "YAML file metadata parsing; JSON metadata needs no extra.",
    "metadata-excel": "Excel metadata parsing; does not add Excel data-source support.",
    "metadata-db": "Database metadata provider via SQLAlchemy; install the selected DB driver separately.",
    "cli": "Project validation, inspection and build CLI; choose an engine separately to execute dataflows.",
    "all": "Union of declared Python profiles; system drivers, credentials and backend configuration are still required.",
}


def render_dependency_reference(manifest: dict | None = None) -> str:
    """Include every declared extra and fail when usage notes become stale."""
    if manifest is None:
        path = Path(__file__).resolve().parents[2] / "pyproject.toml"
        manifest = tomllib.loads(path.read_text(encoding="utf-8"))
    project = manifest["project"]
    extras = project["optional-dependencies"]
    if set(extras) != set(PROFILE_NOTES):
        raise ValueError("Dependency profile notes disagree with pyproject.toml")
    lines = [
        "---",
        "title: Dependency profiles | DataCoolie",
        "description: Complete Python dependency and optional profile inventory generated from the package manifest.",
        "---", "", "# Dependency profiles", "",
        f"Requirements below come from `pyproject.toml` for version `{project['version']}`. "
        "They are allowed package ranges, not a runtime qualification matrix.", "",
        f"Python requirement: `{project['requires-python']}`.", "",
        "## Core installation", "",
        "`python -m pip install datacoolie` installs only the core requirements:", "",
        *[f"- `{requirement}`" for requirement in project["dependencies"]], "",
        "Choose an engine and the formats/transports you use. Extras may be combined:", "",
        '```bash\npython -m pip install "datacoolie[cli,polars-delta,source-api]"\n```', "",
        "For the matching-release wheel route, interpreter setup and first local run, "
        "follow [Installation](../guide/getting-started/installation.md).", "",
        "## Optional profiles", "",
        "| Extra | Python requirements | Use / additional prerequisites |",
        "|---|---|---|",
    ]
    for name, requirements in extras.items():
        rendered = "<br>".join("`" + item.replace("|", "\\|") + "`" for item in requirements)
        lines.append(f"| `{name}` | {rendered} | {PROFILE_NOTES[name]} |")
    lines.extend([
        "", "## Select the transport", "",
        "Source dependencies and metadata-provider dependencies serve different operations. "
        "For engine/database URL syntax and native, ConnectorX, JDBC or ODBC selection, "
        "see [Database and SQL sources](../guide/metadata/source-database.md). "
        "A registry entry alone does not verify that its optional dependencies are installed.", "",
    ])
    return "\n".join(lines)
