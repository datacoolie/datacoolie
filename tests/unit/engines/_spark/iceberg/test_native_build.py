from __future__ import annotations

from pathlib import Path
import hashlib
from typing import Any

import pytest

from datacoolie.engines._spark.iceberg import native_build as native


def _runtime_manifest_values(source_fingerprint: str, helper: Path) -> dict[str, Any]:
    helper_hash = hashlib.sha256(helper.read_bytes()).hexdigest()
    return {
        "manifest_version": 1,
        "helper_abi": native.HELPER_ABI,
        "helper_class": native.HELPER_CLASS,
        "helper_classes": list(native.HELPER_CLASSES),
        "source_fingerprint": source_fingerprint,
        "spark_version": native.SPARK_VERSION,
        "iceberg_version": native.ICEBERG_VERSION,
        "java_release": native.JAVA_RELEASE,
        "runtime_jar": {
            "name": native.RUNTIME_JAR_NAME,
            "sha256": native.RUNTIME_JAR_SHA256,
        },
        "helper_jar": {"name": helper.name, "sha256": helper_hash},
        "runtime_classes": list(native.RUNTIME_CLASSES),
    }


def test_build_rejects_wrong_runtime_before_compiler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = tmp_path / native.RUNTIME_JAR_NAME
    runtime.write_bytes(b"wrong runtime")
    (tmp_path / "spark-core_2.12-3.5.9.jar").write_bytes(b"spark core")
    (tmp_path / "spark-sql_2.12-3.5.9.jar").write_bytes(b"spark sql")
    called: list[tuple[str, ...]] = []
    monkeypatch.setattr(native.subprocess, "run", lambda command, **_: called.append(tuple(command)))

    with pytest.raises(native.NativeBuildError, match="digest mismatch"):
        native.build_guard(
            tmp_path / "output",
            runtime_jar=runtime,
            spark_jars_dir=tmp_path,
            javac="javac",
            jar="jar",
        )
    assert called == []


def test_source_fingerprint_covers_all_canonical_java_sources() -> None:
    sources = native.canonical_sources()
    expected = hashlib.sha256(b"".join(path.read_bytes() for path in sources)).hexdigest()
    assert [path.name for path in sources] == [
        "GuardedTableOperations.java",
        "NativeMaintenance.java",
    ]
    assert native.source_fingerprint() == expected


def test_runtime_coverage_includes_operative_catalog_and_spark_action_classes() -> None:
    required = {
        "org.apache.iceberg.spark.SparkCatalog",
        "org.apache.iceberg.rest.RESTCatalog",
        "org.apache.iceberg.rest.RESTTableOperations",
        "org.apache.iceberg.spark.actions.DeleteOrphanFilesSparkAction",
        "org.apache.iceberg.spark.actions.ExpireSnapshotsSparkAction",
        "org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction",
        "org.apache.iceberg.spark.actions.RewriteManifestsSparkAction",
        "org.apache.iceberg.spark.actions.RewritePositionDeleteFilesSparkAction",
    }
    assert len(native.RUNTIME_CLASSES) == 24
    assert required.issubset(native.RUNTIME_CLASSES)


def test_build_rejects_wrong_spark_scala_binary_version(tmp_path: Path) -> None:
    (tmp_path / "spark-core_2.13-3.5.9.jar").write_bytes(b"spark core")
    (tmp_path / "spark-sql_2.13-3.5.9.jar").write_bytes(b"spark sql")

    with pytest.raises(native.NativeBuildError, match="not pinned"):
        native._resolve_spark_jars(tmp_path)


def test_build_emits_manifest_and_pinned_javac_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = tmp_path / native.RUNTIME_JAR_NAME
    runtime.write_bytes(b"pinned runtime placeholder")
    (tmp_path / "spark-core_2.12-3.5.9.jar").write_bytes(b"spark core")
    (tmp_path / "spark-sql_2.12-3.5.9.jar").write_bytes(b"spark sql")
    commands: list[tuple[str, ...]] = []

    def fake_hash(path: Path) -> str:
        if path == runtime:
            return native.RUNTIME_JAR_SHA256
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def fake_run(command: list[str], *, label: str) -> None:
        commands.append(tuple(command))
        if label == "javac":
            classes = Path(command[command.index("-d") + 1])
            target = classes / "org/datacoolie/iceberg/GuardedTableOperations.class"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"class bytes")
        else:
            jar_path = Path(command[command.index("--file") + 1])
            jar_path.write_bytes(b"jar bytes")

    monkeypatch.setattr(native, "sha256_file", fake_hash)
    monkeypatch.setattr(native, "_run_tool", fake_run)
    manifest = native.build_guard(
        tmp_path / "output",
        runtime_jar=runtime,
        spark_jars_dir=tmp_path,
        javac="javac",
        jar="jar",
    )

    assert commands[0][1:5] == ("--release", "11", "-proc:none", "-cp")
    assert commands[0][-2:] == tuple(str(path) for path in native.canonical_sources())
    assert commands[1][0:4] == ("jar", "--create", "--file", str(manifest.jar_path))
    assert manifest.jar_path == tmp_path / "output" / native.JAR_NAME
    assert manifest.manifest_path == tmp_path / "output" / native.MANIFEST_NAME
    written = native.load_manifest(manifest.manifest_path)
    assert written.source_fingerprint == native.source_fingerprint()
    assert written.helper_classes == native.HELPER_CLASSES
    assert written.runtime_jar_sha256 == native.RUNTIME_JAR_SHA256


class _Resources:
    def __init__(self, values: list[str]) -> None:
        self._values = iter(values)
        self._remaining = len(values)

    def hasMoreElements(self) -> bool:
        return self._remaining > 0

    def nextElement(self) -> str:
        self._remaining -= 1
        return next(self._values)


class _ContextLoader:
    def __init__(self, values: list[str]) -> None:
        self.values = values

    def getResources(self, _: str) -> _Resources:
        return _Resources(list(self.values))


class _ContextThread:
    def __init__(self, loader: _ContextLoader) -> None:
        self.loader = loader

    def currentThread(self) -> "_ContextThread":
        return self

    def getContextClassLoader(self) -> _ContextLoader:
        return self.loader


class _Field:
    def __init__(self, value: str) -> None:
        self.value = value

    def get(self, _: Any) -> str:
        return self.value


class _JavaClass:
    def __init__(self, source: Path, fields: dict[str, str], resources: list[str]) -> None:
        self.source = source
        self.fields = fields
        self.resources = resources

    def getProtectionDomain(self) -> Any:
        return self

    def getCodeSource(self) -> Any:
        return self

    def getLocation(self) -> str:
        return self.source.as_uri()

    def getClassLoader(self) -> Any:
        return self

    def getResources(self, _: str) -> _Resources:
        return _Resources(self.resources)

    def getField(self, name: str) -> _Field:
        return _Field(self.fields[name])


class _JavaLang:
    def __init__(self, classes: dict[str, _JavaClass]) -> None:
        self.Class = self
        self.classes = classes

    def forName(self, name: str) -> _JavaClass:
        return self.classes[name]


class _Jvm:
    def __init__(self, classes: dict[str, _JavaClass]) -> None:
        self.java = type("Java", (), {"lang": _JavaLang(classes)})()


class _BorrowedSpark:
    def __init__(self, jvm: _Jvm) -> None:
        self.version = native.SPARK_VERSION
        self._jvm = jvm
        self.stop_called = False

    def stop(self) -> None:
        self.stop_called = True


def _fake_runtime(
    tmp_path: Path, *, helper_resources: list[str] | None = None
) -> tuple[_BorrowedSpark, dict[str, Any], Path, Path]:
    helper = tmp_path / "guard.jar"
    helper.write_bytes(b"helper jar")
    runtime = tmp_path / native.RUNTIME_JAR_NAME
    runtime.write_bytes(b"runtime jar")
    source_fingerprint = native.source_fingerprint()
    helper_fields = {
        "HELPER_ABI": native.HELPER_ABI,
        "SOURCE_FINGERPRINT": source_fingerprint,
        "RUNTIME_JAR_SHA256": native.RUNTIME_JAR_SHA256,
    }
    classes = {
        native.HELPER_CLASS: _JavaClass(
            helper,
            helper_fields,
            helper_resources or [helper.joinpath("org/datacoolie/iceberg/GuardedTableOperations.class").as_uri()],
        )
    }
    classes[native.ACTION_HELPER_CLASS] = _JavaClass(
        helper,
        helper_fields,
        [helper.joinpath("org/datacoolie/iceberg/NativeMaintenance.class").as_uri()],
    )
    for name in native.RUNTIME_CLASSES:
        classes[name] = _JavaClass(
            runtime,
            {},
            [runtime.as_uri()],
        )
    return (
        _BorrowedSpark(_Jvm(classes)),
        _runtime_manifest_values(source_fingerprint, helper),
        helper,
        runtime,
    )


def test_stale_manifest_is_rejected_before_borrowed_session_access(tmp_path: Path) -> None:
    helper = tmp_path / "guard.jar"
    helper.write_bytes(b"helper jar")
    manifest = _runtime_manifest_values("0" * 64, helper)

    class Session:
        version = native.SPARK_VERSION

        @property
        def _jvm(self) -> Any:
            raise AssertionError("stale manifest must fail before JVM access")

    with pytest.raises(native.NativeProvenanceError, match="source fingerprint is stale"):
        native.verify_runtime_provenance(Session(), manifest, helper_jar=helper)


def test_stale_runtime_class_coverage_is_rejected_before_borrowed_session_access(
    tmp_path: Path,
) -> None:
    helper = tmp_path / "guard.jar"
    helper.write_bytes(b"helper jar")
    manifest = _runtime_manifest_values(native.source_fingerprint(), helper)
    manifest["runtime_classes"] = manifest["runtime_classes"][:-8]

    class Session:
        version = native.SPARK_VERSION

        @property
        def _jvm(self) -> Any:
            raise AssertionError("stale runtime coverage must fail before JVM access")

    with pytest.raises(native.NativeProvenanceError, match="runtime class coverage is stale"):
        native.verify_runtime_provenance(Session(), manifest, helper_jar=helper)


def test_provenance_accepts_borrowed_session_without_mutating_or_closing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spark, manifest, helper, runtime = _fake_runtime(tmp_path)
    real_hash = native.sha256_file
    monkeypatch.setattr(
        native,
        "sha256_file",
        lambda path: native.RUNTIME_JAR_SHA256 if path == runtime else real_hash(path),
    )
    native.verify_runtime_provenance(spark, manifest, helper_jar=helper)
    assert spark.stop_called is False


def test_duplicate_helper_resource_is_rejected_before_runtime_use(tmp_path: Path) -> None:
    duplicate = ["file:/one/guard.jar", "file:/two/guard.jar"]
    spark, manifest, helper, _ = _fake_runtime(tmp_path, helper_resources=duplicate)

    with pytest.raises(native.NativeProvenanceError, match="Duplicate or missing native guard"):
        native.verify_runtime_provenance(spark, manifest, helper_jar=helper)


def test_duplicate_context_loader_resource_is_rejected(tmp_path: Path) -> None:
    spark, manifest, helper, _ = _fake_runtime(tmp_path)
    spark._jvm.java.lang.Thread = _ContextThread(
        _ContextLoader(["file:/one/guard.jar", "file:/two/guard.jar"])
    )

    with pytest.raises(native.NativeProvenanceError, match="Duplicate or missing native guard"):
        native.verify_runtime_provenance(spark, manifest, helper_jar=helper)


def test_split_helper_code_sources_are_rejected(tmp_path: Path) -> None:
    spark, manifest, helper, _ = _fake_runtime(tmp_path)
    secondary = tmp_path / "secondary" / helper.name
    secondary.parent.mkdir()
    secondary.write_bytes(helper.read_bytes())
    spark._jvm.java.lang.classes[native.ACTION_HELPER_CLASS].source = secondary

    with pytest.raises(native.NativeProvenanceError, match="multiple code sources"):
        native.verify_runtime_provenance(spark, manifest, helper_jar=helper)
