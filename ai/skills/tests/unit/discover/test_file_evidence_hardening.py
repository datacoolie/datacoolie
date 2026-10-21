"""File metadata facts and bounded dataset coverage are separate evidence."""

import json

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import introspect_files
from _observation_contract import read_observations
from _probe_status import file_sha256


def parquet(path, *, extra=False):
    fields = [pa.field("id", pa.int64(), nullable=False)]
    values = {"id": [1]}
    if extra:
        fields.append(pa.field("extra", pa.string(), nullable=True))
        values["extra"] = [None]
    pq.write_table(pa.Table.from_pydict(values, schema=pa.schema(fields)), path)


def test_direct_parquet_declared_nullability_and_bound_status(tmp_path):
    path, output, status = (
        tmp_path / "input.parquet",
        tmp_path / "out.csv",
        tmp_path / "status.json",
    )
    parquet(path, extra=True)
    introspect_files.main(
        [
            "schema",
            "--path",
            str(path),
            "--source",
            "files",
            "--table",
            "orders",
            "--output",
            str(output),
            "--status-output",
            str(status),
        ]
    )
    assert [row["nullable"] for row in read_observations(output)] == ["false", "true"]
    evidence = json.loads(status.read_text())
    assert evidence["status"] == "complete"
    assert evidence["output_sha256"] == file_sha256(output)
    assert evidence["scope"]["kind"] == "objects"
    assert evidence["details"]["schema_scope"] == "file"


@pytest.mark.parametrize("max_files,conflict", [(1, False), (2, True)])
def test_directory_sampling_and_conflicts_are_partial(tmp_path, max_files, conflict):
    directory = tmp_path / "data"
    directory.mkdir()
    parquet(directory / "a.parquet")
    parquet(directory / "b.parquet", extra=conflict)
    output = tmp_path / "out.csv"
    issues, details = [], {}
    assert (
        introspect_files.cmd_schema(
            str(directory),
            "parquet",
            "files",
            "orders",
            str(output),
            None,
            max_files=max_files,
            issues=issues,
            details=details,
        )
        == 1
    )
    assert len(details["selected_files"]) == max_files
    assert not details["schema_complete"]
    assert any(
        ("conflicting" if conflict else "Unexamined") in issue for issue in issues
    )


def test_schema_traversal_bounds_reads_without_glob(tmp_path, monkeypatch):
    fs = fsspec.filesystem("memory")
    root = "/bounded-schema-fixture"
    fs.makedirs(root + "/deep", exist_ok=True)
    for name in ("a.parquet", "b.parquet", "deep/c.parquet"):
        with fs.open(root + "/" + name, "wb") as handle:
            pq.write_table(pa.table({"id": [1]}), handle)
    opened, listed = [], []
    original_open, original_ls = fs.open, fs.ls

    def tracked_open(path, *args, **kwargs):
        opened.append(path)
        return original_open(path, *args, **kwargs)

    def tracked_ls(path, *args, **kwargs):
        listed.append(path)
        return original_ls(path, *args, **kwargs)

    monkeypatch.setattr(fs, "open", tracked_open)
    monkeypatch.setattr(fs, "ls", tracked_ls)
    monkeypatch.setattr(
        fs, "glob", lambda *args, **kwargs: pytest.fail("unbounded glob")
    )
    monkeypatch.setattr(fsspec.core, "url_to_fs", lambda *args, **kwargs: (fs, root))
    issues, details = [], {}
    introspect_files.cmd_schema(
        "memory://fixture",
        "parquet",
        "files",
        "orders",
        str(tmp_path / "out.csv"),
        None,
        max_entries=2,
        max_depth=0,
        max_files=1,
        issues=issues,
        details=details,
    )
    assert opened == [root + "/a.parquet"]
    assert listed == [root]
    assert issues and not details["schema_complete"]


def test_inferred_tsv_has_unknown_nullability(tmp_path):
    path, output = tmp_path / "input.tsv", tmp_path / "out.csv"
    path.write_text("id\tvalue\n1\thello\n", encoding="utf-8")
    introspect_files.cmd_schema(str(path), "auto", "files", "orders", str(output), None)
    rows = read_observations(output)
    assert [row["column"] for row in rows] == ["id", "value"]
    assert all(row["nullable"] == "" and "inferred" in row["notes"] for row in rows)


def test_avro_and_delta_preserve_declared_nullability(tmp_path):
    import fastavro
    from deltalake import write_deltalake

    avro = tmp_path / "input.avro"
    with avro.open("wb") as handle:
        fastavro.writer(
            handle,
            {
                "type": "record",
                "name": "Fixture",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "extra", "type": ["null", "string"]},
                ],
            },
            [{"id": 1, "extra": None}],
        )
    delta = tmp_path / "delta"
    write_deltalake(
        str(delta),
        pa.Table.from_pydict(
            {"id": [1], "extra": [None]},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("extra", pa.string(), nullable=True),
                ]
            ),
        ),
    )
    for path, fmt in ((avro, "avro"), (delta, "delta")):
        output = tmp_path / f"{fmt}.csv"
        introspect_files.cmd_schema(
            str(path), fmt, "files", "orders", str(output), None
        )
        assert [row["nullable"] for row in read_observations(output)] == [
            "false",
            "true",
        ]


def test_unknown_extension_is_explicit_partial_exclusion(tmp_path):
    directory = tmp_path / "data"
    directory.mkdir()
    parquet(directory / "a.parquet")
    parquet(directory / "b.dat", extra=True)
    issues, details = [], {}
    introspect_files.cmd_schema(
        str(directory),
        "auto",
        "files",
        "orders",
        str(tmp_path / "out.csv"),
        None,
        issues=issues,
        details=details,
    )
    assert details["excluded_files"][0].endswith("b.dat")
    assert not details["schema_complete"]
    assert any("Unrecognized" in issue for issue in issues)


def test_output_aliases_cannot_mutate_source(tmp_path):
    source = tmp_path / "input.parquet"
    parquet(source)
    original = source.read_bytes()
    with pytest.raises(ValueError, match="outside source"):
        introspect_files.cmd_schema(
            str(source), "parquet", "files", "orders", str(source), None
        )
    for args in (["--output", str(source)], ["--status-output", str(source)]):
        with pytest.raises(SystemExit):
            introspect_files.main(
                [
                    "schema",
                    "--path",
                    str(source),
                    "--source",
                    "files",
                    "--table",
                    "orders",
                    *args,
                ]
            )
        assert source.read_bytes() == original


def test_directory_unselected_output_and_file_uri_status_preserve_bytes(tmp_path):
    directory = tmp_path / "data"
    directory.mkdir()
    first, second = directory / "a.parquet", directory / "b.parquet"
    parquet(first)
    parquet(second, extra=True)
    original = second.read_bytes()
    with pytest.raises(ValueError, match="outside source"):
        introspect_files.cmd_schema(
            str(directory), "parquet", "files", "orders", str(second), None
        )
    assert second.read_bytes() == original
    with pytest.raises(SystemExit):
        introspect_files.main(
            [
                "schema",
                "--path",
                second.as_uri(),
                "--source",
                "files",
                "--table",
                "orders",
                "--status-output",
                str(second),
            ]
        )
    assert second.read_bytes() == original
