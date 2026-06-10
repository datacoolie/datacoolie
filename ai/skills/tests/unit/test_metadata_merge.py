import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[2] / "datacoolie-metadata" / "scripts" / "merge.py"


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run_merge(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )


def write_connections(base: Path) -> None:
    write_json(
        base / "connections.json",
        [
            {"name": "src", "connection_type": "file"},
            {"name": "dst", "connection_type": "file"},
        ],
    )


def test_modular_stage_file_infers_parent_stage(tmp_path: Path) -> None:
    workspace = tmp_path / "acme_dcws"
    base = workspace / "metadata"
    write_connections(base)
    write_json(
        base / "dataflows" / "source2bronze.json",
        [{"name": "load_orders", "source": {"connection_name": "src"}, "destination": {"connection_name": "dst"}}],
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "dev")

    assert result.returncode == 0, result.stderr
    output = json.loads((workspace / "generated" / "dev" / "metadata.json").read_text())
    assert output["dataflows"][0]["stage"] == "source2bronze"


def test_nested_modular_file_requires_exact_stage(tmp_path: Path) -> None:
    base = tmp_path / "acme_dcws" / "metadata"
    write_connections(base)
    write_json(
        base / "dataflows" / "source2bronze" / "sap.json",
        [
            {
                "name": "load_sap",
                "stage": "source2bronze_crm",
                "source": {"connection_name": "src"},
                "destination": {"connection_name": "dst"},
            }
        ],
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "dev")

    assert result.returncode == 2
    assert "nested path requires 'source2bronze_sap'" in result.stderr


def test_split_output_and_stage_manifest(tmp_path: Path) -> None:
    workspace = tmp_path / "acme_dcws"
    base = workspace / "metadata"
    write_connections(base)
    write_json(
        base / "schema_hints.json",
        [
            {
                "connection_name": "src",
                "table_name": "sap_orders",
                "hints": [{"column_name": "id", "data_type": "INTEGER"}],
            }
        ],
    )
    write_json(
        base / "dataflows" / "source2bronze" / "sap.json",
        [{"name": "load_sap", "source": {"connection_name": "src"}, "destination": {"connection_name": "dst"}}],
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "dev", "--output-layout", "split", "--emit-stage-manifest")

    assert result.returncode == 0, result.stderr
    output_dir = workspace / "generated" / "dev"
    dataflows = json.loads((output_dir / "dataflows.json").read_text())
    manifest = json.loads((output_dir / "stage_manifest.json").read_text())
    assert dataflows[0]["stage"] == "source2bronze_sap"
    assert manifest == {"stages": [{"stage": "source2bronze_sap", "dataflows": ["load_sap"]}]}
    assert (output_dir / "schema_hints.json").exists()


def test_schema_hints_overlay_merges_by_group_and_column(tmp_path: Path) -> None:
    workspace = tmp_path / "acme_dcws"
    base = workspace / "metadata"
    write_connections(base)
    write_json(
        base / "dataflows.json",
        [{"name": "load_orders", "source": {"connection_name": "src"}, "destination": {"connection_name": "dst"}}],
    )
    write_json(
        base / "schema_hints.json",
        [
            {
                "connection_name": "src",
                "table_name": "orders",
                "hints": [
                    {"column_name": "id", "data_type": "INTEGER"},
                    {"column_name": "amount", "data_type": "DECIMAL", "precision": 10, "scale": 2},
                ],
            }
        ],
    )
    write_text(
        base / "environments" / "prod.yaml",
        """
schema_hints:
  - connection_name: src
    schema_name: null
    table_name: orders
    hints:
      - column_name: amount
        precision: 18
      - column_name: order_date
        data_type: DATE
  - connection_name: src
    schema_name: dbo
    table_name: customers
    hints:
      - column_name: customer_id
        data_type: STRING
""".lstrip(),
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "prod")

    assert result.returncode == 0, result.stderr
    output = json.loads((workspace / "generated" / "prod" / "metadata.json").read_text())
    groups = output["schema_hints"]
    orders = next(group for group in groups if group["table_name"] == "orders")
    order_hints = {hint["column_name"]: hint for hint in orders["hints"]}
    assert len(groups) == 2
    assert order_hints["id"]["data_type"] == "INTEGER"
    assert order_hints["amount"] == {
        "column_name": "amount",
        "data_type": "DECIMAL",
        "precision": 18,
        "scale": 2,
    }
    assert order_hints["order_date"]["data_type"] == "DATE"


def test_schema_hints_overlay_rejects_duplicate_column_keys(tmp_path: Path) -> None:
    base = tmp_path / "acme_dcws" / "metadata"
    write_connections(base)
    write_json(
        base / "dataflows.json",
        [{"name": "load_orders", "source": {"connection_name": "src"}, "destination": {"connection_name": "dst"}}],
    )
    write_text(
        base / "environments" / "prod.yaml",
        """
schema_hints:
  - connection_name: src
    table_name: orders
    hints:
      - column_name: amount
        data_type: DECIMAL
      - column_name: amount
        data_type: DOUBLE
""".lstrip(),
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "prod")

    assert result.returncode == 2
    assert "Duplicate schema_hints column_name 'amount'" in result.stderr


def test_schema_hints_overlay_rejects_missing_group_key(tmp_path: Path) -> None:
    base = tmp_path / "acme_dcws" / "metadata"
    write_connections(base)
    write_json(
        base / "dataflows.json",
        [{"name": "load_orders", "source": {"connection_name": "src"}, "destination": {"connection_name": "dst"}}],
    )
    write_text(
        base / "environments" / "prod.yaml",
        """
schema_hints:
  - connection_name: src
    hints:
      - column_name: amount
        data_type: DECIMAL
""".lstrip(),
    )

    result = run_merge(tmp_path, "--base", str(base), "--env", "prod")

    assert result.returncode == 2
    assert "must include connection_name or connection_id, plus table_name" in result.stderr
