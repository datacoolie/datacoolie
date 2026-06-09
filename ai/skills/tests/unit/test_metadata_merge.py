import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[2] / "datacoolie-metadata" / "scripts" / "merge.py"


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


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
    write_json(base / "schema_hints.json", [{"dataflow_name": "load_sap", "column_name": "id", "data_type": "INTEGER"}])
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
