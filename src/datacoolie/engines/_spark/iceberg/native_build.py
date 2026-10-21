"""Build and verify the bounded native Iceberg Spark guard.

The guard is an optional runtime resource.  This module deliberately has no import-time Spark
dependency and never downloads dependencies, compiles code, changes a Spark session, or closes a
borrowed resource during verification.  Build it explicitly before constructing a session and
pass the resulting JAR to that session's class path.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field, replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlparse


SPARK_VERSION = "3.5.9"
SPARK_SCALA_BINARY_VERSION = "2.12"
ICEBERG_VERSION = "1.10.1"
JAVA_RELEASE = "11"
HELPER_ABI = "datacoolie-iceberg-guard-v1"
HELPER_CLASS = "org.datacoolie.iceberg.GuardedTableOperations"
ACTION_HELPER_CLASS = "org.datacoolie.iceberg.NativeMaintenance"
HELPER_CLASSES = (HELPER_CLASS, ACTION_HELPER_CLASS)
RUNTIME_JAR_NAME = "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
RUNTIME_JAR_SHA256 = (
    "39ea09e6c03550a300b9d9ab498949836d2d434e441da4c12a943a086e396940"
)
MANIFEST_NAME = "manifest.json"
JAR_NAME = "guard.jar"
BUILD_RESOURCE = "META-INF/datacoolie-iceberg-guard.properties"
SOURCE_DIR = Path(__file__).with_name("native")
SOURCE_PATH = SOURCE_DIR / "GuardedTableOperations.java"
ACTION_SOURCE_PATH = SOURCE_DIR / "NativeMaintenance.java"
RUNTIME_CLASSES = (
    "org.apache.iceberg.BaseTable",
    "org.apache.iceberg.Table",
    "org.apache.iceberg.TableMetadata",
    "org.apache.iceberg.TableOperations",
    "org.apache.iceberg.spark.SparkCatalog",
    "org.apache.iceberg.rest.RESTCatalog",
    "org.apache.iceberg.rest.RESTTableOperations",
    "org.apache.iceberg.spark.actions.SparkActions",
    "org.apache.iceberg.spark.actions.DeleteOrphanFilesSparkAction",
    "org.apache.iceberg.spark.actions.ExpireSnapshotsSparkAction",
    "org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction",
    "org.apache.iceberg.spark.actions.RewriteManifestsSparkAction",
    "org.apache.iceberg.spark.actions.RewritePositionDeleteFilesSparkAction",
    "org.apache.iceberg.io.FileIO",
    "org.apache.iceberg.io.LocationProvider",
    "org.apache.iceberg.encryption.EncryptionManager",
    "org.apache.iceberg.HistoryEntry",
    "org.apache.iceberg.Snapshot",
    "org.apache.iceberg.actions.Action",
    "org.apache.iceberg.actions.DeleteOrphanFiles",
    "org.apache.iceberg.actions.ExpireSnapshots",
    "org.apache.iceberg.actions.RewriteDataFiles",
    "org.apache.iceberg.actions.RewriteManifests",
    "org.apache.iceberg.actions.RewritePositionDeleteFiles",
)


class NativeBuildError(RuntimeError):
    """The explicit native helper build could not produce a qualified artifact."""


class NativeProvenanceError(RuntimeError):
    """The borrowed Spark runtime does not match a qualified helper manifest."""


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of a file without loading it all into memory."""
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise NativeBuildError(f"Could not read native runtime artifact: {path}") from error
    return digest.hexdigest()


def canonical_sources() -> tuple[Path, ...]:
    """Return every canonical Java helper source in stable build order."""
    sources = tuple(sorted(SOURCE_DIR.glob("*.java"), key=lambda path: path.name))
    if not sources:
        raise NativeBuildError(f"Canonical native helper sources are missing: {SOURCE_DIR}")
    missing = [path for path in sources if not path.is_file()]
    if missing:
        raise NativeBuildError(f"Canonical native helper source is missing: {missing[0]}")
    return sources


def source_fingerprint(source: Path | None = None) -> str:
    """Return the SHA-256 of canonical Java sources in stable filename order."""
    sources = (source,) if source is not None else canonical_sources()
    digest = hashlib.sha256()
    for path in sources:
        if not path.is_file():
            raise NativeBuildError(f"Canonical native helper source is missing: {path}")
        try:
            digest.update(path.read_bytes())
        except OSError as error:
            raise NativeBuildError(f"Could not read canonical native helper source: {path}") from error
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class GuardBuildManifest:
    """Immutable, serializable evidence for one compiled guard JAR."""

    manifest_version: int
    helper_abi: str
    helper_class: str
    helper_classes: tuple[str, ...]
    source_fingerprint: str
    spark_version: str
    iceberg_version: str
    java_release: str
    runtime_jar_name: str
    runtime_jar_sha256: str
    helper_jar_name: str
    helper_jar_sha256: str
    runtime_classes: tuple[str, ...] = field(default_factory=lambda: RUNTIME_CLASSES)
    jar_path: Path | None = field(default=None, repr=False, compare=False)
    manifest_path: Path | None = field(default=None, repr=False, compare=False)

    def as_dict(self) -> dict[str, Any]:
        """Return the stable manifest shape written to disk."""
        runtime = {"name": self.runtime_jar_name, "sha256": self.runtime_jar_sha256}
        helper = {"name": self.helper_jar_name, "sha256": self.helper_jar_sha256}
        return {
            "manifest_version": self.manifest_version,
            "helper_abi": self.helper_abi,
            "abi": self.helper_abi,
            "helper_class": self.helper_class,
            "helper_classes": list(self.helper_classes),
            "source_fingerprint": self.source_fingerprint,
            "spark_version": self.spark_version,
            "iceberg_version": self.iceberg_version,
            "java_release": self.java_release,
            "runtime_jar": runtime,
            "runtime_jar_name": self.runtime_jar_name,
            "runtime_jar_sha256": self.runtime_jar_sha256,
            "helper_jar": helper,
            "helper_jar_name": self.helper_jar_name,
            "helper_jar_sha256": self.helper_jar_sha256,
            "runtime_classes": list(self.runtime_classes),
            "helper_resource": BUILD_RESOURCE,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GuardBuildManifest":
        """Parse and validate a generated manifest before any runtime access."""
        if not isinstance(value, Mapping):
            raise NativeProvenanceError("Native guard manifest must be an object")

        def required_text(key: str, fallback: str | None = None) -> str:
            item = value.get(key, fallback)
            if not isinstance(item, str) or not item.strip():
                raise NativeProvenanceError(f"Native guard manifest field {key!r} is missing")
            return item

        def nested_text(key: str, nested_key: str) -> str:
            item = value.get(key)
            if isinstance(item, Mapping) and nested_key in item:
                return required_text(nested_key, str(item[nested_key]))
            return required_text(f"{key}_{nested_key}")

        version = value.get("manifest_version", 1)
        if type(version) is not int or version != 1:
            raise NativeProvenanceError("Unsupported native guard manifest version")
        abi = required_text("helper_abi", value.get("abi"))
        classes = value.get("runtime_classes", list(RUNTIME_CLASSES))
        if not isinstance(classes, (list, tuple)) or not classes or any(
            not isinstance(item, str) or not item for item in classes
        ):
            raise NativeProvenanceError("Native guard runtime class list is invalid")
        helper_classes = value.get("helper_classes", [HELPER_CLASS, ACTION_HELPER_CLASS])
        if not isinstance(helper_classes, (list, tuple)) or not helper_classes or any(
            not isinstance(item, str) or not item for item in helper_classes
        ):
            raise NativeProvenanceError("Native guard helper class list is invalid")
        return cls(
            manifest_version=version,
            helper_abi=abi,
            helper_class=required_text("helper_class", HELPER_CLASS),
            helper_classes=tuple(helper_classes),
            source_fingerprint=required_text("source_fingerprint"),
            spark_version=required_text("spark_version"),
            iceberg_version=required_text("iceberg_version"),
            java_release=required_text("java_release"),
            runtime_jar_name=nested_text("runtime_jar", "name"),
            runtime_jar_sha256=nested_text("runtime_jar", "sha256"),
            helper_jar_name=nested_text("helper_jar", "name"),
            helper_jar_sha256=nested_text("helper_jar", "sha256"),
            runtime_classes=tuple(classes),
        )


def load_manifest(path: str | Path) -> GuardBuildManifest:
    """Load a generated manifest from a file or caller-selected build directory."""
    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / MANIFEST_NAME
    try:
        raw = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise NativeProvenanceError(f"Could not read native guard manifest: {candidate}") from error
    manifest = GuardBuildManifest.from_mapping(raw)
    return replace(manifest, manifest_path=candidate)


def _resolve_runtime_jar(runtime_jar: str | Path | None, spark_jars_dir: Path | None) -> Path:
    if runtime_jar is not None:
        candidate = Path(runtime_jar)
    elif spark_jars_dir is not None:
        candidate = spark_jars_dir / RUNTIME_JAR_NAME
    else:
        raise NativeBuildError(
            "An explicit Spark 3.5.9 runtime JAR is required; no dependency download is allowed"
        )
    if not candidate.is_file():
        raise NativeBuildError(f"Pinned Iceberg runtime JAR is missing: {candidate}")
    if candidate.name != RUNTIME_JAR_NAME:
        raise NativeBuildError(
            f"Expected pinned runtime JAR {RUNTIME_JAR_NAME}, got {candidate.name}"
        )
    digest = sha256_file(candidate)
    if digest != RUNTIME_JAR_SHA256:
        raise NativeBuildError(
            f"Pinned Iceberg runtime JAR digest mismatch: {candidate} ({digest})"
        )
    return candidate


def _resolve_spark_jars(
    spark_jars_dir: str | Path | None, spark_home: str | Path | None = None
) -> Path:
    if spark_jars_dir is not None and spark_home is not None:
        raise NativeBuildError("Provide one of spark_jars_dir or spark_home, not both")
    if spark_jars_dir is not None:
        candidate = Path(spark_jars_dir)
    elif spark_home is not None:
        candidate = Path(spark_home)
        if (candidate / "jars").is_dir():
            candidate = candidate / "jars"
    else:
        try:
            import pyspark  # type: ignore[import-not-found]
        except ImportError as error:
            raise NativeBuildError(
                "Spark JAR directory is required when pyspark is not installed"
            ) from error
        version = str(getattr(pyspark, "__version__", ""))
        if version != SPARK_VERSION:
            raise NativeBuildError(
                f"Native guard build requires pyspark {SPARK_VERSION}; found {version or 'unknown'}"
            )
        candidate = Path(pyspark.__file__).resolve().parent / "jars"
    if not candidate.is_dir():
        raise NativeBuildError(f"Spark JAR directory is missing: {candidate}")
    spark_sql = sorted(candidate.glob("spark-sql_*.jar"))
    spark_core = sorted(candidate.glob("spark-core_*.jar"))
    if not spark_sql or not spark_core:
        raise NativeBuildError(
            f"Spark JAR directory must contain Spark {SPARK_VERSION} core and SQL JARs: {candidate}"
        )
    spark_jars = sorted(candidate.glob("spark-*.jar"))
    expected_spark_suffix = f"_{SPARK_SCALA_BINARY_VERSION}-{SPARK_VERSION}.jar"
    if any(not path.name.endswith(expected_spark_suffix) for path in spark_jars):
        raise NativeBuildError(
            f"Spark JAR directory is not pinned to Spark {SPARK_VERSION}/{SPARK_SCALA_BINARY_VERSION}: "
            f"{candidate}"
        )
    return candidate


def _java_tool(name: str, explicit: str | Path | None, sibling_of: str | Path | None = None) -> str:
    if explicit is not None:
        candidate = str(explicit)
    else:
        candidate = shutil.which(name) or ""
        if not candidate and sibling_of is not None:
            sibling = Path(sibling_of).resolve().parent / (name + (".exe" if os.name == "nt" else ""))
            if sibling.is_file():
                candidate = str(sibling)
    if not candidate:
        raise NativeBuildError(f"Required Java tool is not available: {name}")
    return candidate


def _run_tool(command: Sequence[str], *, label: str) -> None:
    try:
        result = subprocess.run(
            list(command), check=False, capture_output=True, text=True
        )
    except OSError as error:
        raise NativeBuildError(f"Could not run {label}: {command[0]}") from error
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        raise NativeBuildError(f"{label} failed ({result.returncode}): {detail}")


def _write_text(path: Path, content: str) -> None:
    try:
        path.write_text(content, encoding="utf-8", newline="\n")
    except OSError as error:
        raise NativeBuildError(f"Could not write native guard artifact: {path}") from error


def build_guard(
    output_dir: str | Path,
    *,
    runtime_jar: str | Path | None = None,
    spark_jars_dir: str | Path | None = None,
    spark_home: str | Path | None = None,
    javac: str | Path | None = None,
    jar: str | Path | None = None,
) -> GuardBuildManifest:
    """Compile the canonical guard with pinned Spark/Iceberg dependencies.

    ``output_dir`` is caller-owned.  Only ``guard.jar`` and ``manifest.json`` are emitted there;
    temporary compiler classes are removed automatically.  The function never resolves or downloads
    a dependency and always invokes ``javac --release 11 -proc:none``.
    """
    sources = canonical_sources()
    fingerprint = source_fingerprint()
    jars_dir = _resolve_spark_jars(spark_jars_dir, spark_home)
    runtime = _resolve_runtime_jar(runtime_jar, jars_dir)
    javac_tool = _java_tool("javac", javac)
    jar_tool = _java_tool("jar", jar, sibling_of=javac_tool)

    destination = Path(output_dir)
    try:
        destination.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise NativeBuildError(f"Could not create native guard output directory: {destination}") from error

    classpath = os.pathsep.join(
        [str(runtime), *(str(path) for path in sorted(jars_dir.glob("*.jar")))]
    )
    jar_path = destination / JAR_NAME
    with tempfile.TemporaryDirectory(prefix=".classes-", dir=destination) as temporary:
        classes = Path(temporary) / "classes"
        classes.mkdir()
        _run_tool(
            [
                javac_tool,
                "--release",
                JAVA_RELEASE,
                "-proc:none",
                "-cp",
                classpath,
                "-d",
                str(classes),
                *(str(source) for source in sources),
            ],
            label="javac",
        )

        meta_inf = classes / "META-INF"
        meta_inf.mkdir(parents=True, exist_ok=True)
        resource_values = {
            "helper_abi": HELPER_ABI,
            "source_fingerprint": fingerprint,
            "runtime_jar_sha256": RUNTIME_JAR_SHA256,
            "spark_version": SPARK_VERSION,
            "iceberg_version": ICEBERG_VERSION,
            "java_release": JAVA_RELEASE,
        }
        _write_text(
            meta_inf / "datacoolie-iceberg-guard.properties",
            "".join(f"{key}={value}\n" for key, value in resource_values.items()),
        )
        _write_text(
            meta_inf / "datacoolie-iceberg-guard.json",
            json.dumps(resource_values, sort_keys=True, separators=(",", ":")) + "\n",
        )
        _run_tool(
            [jar_tool, "--create", "--file", str(jar_path), "-C", str(classes), "."],
            label="jar",
        )

    manifest = GuardBuildManifest(
        manifest_version=1,
        helper_abi=HELPER_ABI,
        helper_class=HELPER_CLASS,
        helper_classes=HELPER_CLASSES,
        source_fingerprint=fingerprint,
        spark_version=SPARK_VERSION,
        iceberg_version=ICEBERG_VERSION,
        java_release=JAVA_RELEASE,
        runtime_jar_name=runtime.name,
        runtime_jar_sha256=RUNTIME_JAR_SHA256,
        helper_jar_name=jar_path.name,
        helper_jar_sha256=sha256_file(jar_path),
        jar_path=jar_path,
        manifest_path=destination / MANIFEST_NAME,
    )
    _write_text(
        destination / MANIFEST_NAME,
        json.dumps(manifest.as_dict(), indent=2, sort_keys=True) + "\n",
    )
    return manifest


# Public aliases keep the explicit build entrypoint discoverable to callers that use native-helper
# terminology rather than the shorter guard name.
build_native_guard = build_guard
build_guard_jar = build_guard


def _file_uri_path(value: Any) -> Path:
    text = str(value)
    parsed = urlparse(text)
    if parsed.scheme and parsed.scheme != "file":
        raise NativeProvenanceError(f"Native guard code source is not a local file: {text}")
    raw = unquote(parsed.path if parsed.scheme else text)
    if os.name == "nt" and raw.startswith("/") and len(raw) > 2 and raw[2] == ":":
        raw = raw[1:]
    path = Path(raw)
    if not path.is_file():
        raise NativeProvenanceError(f"Native guard code source is not a JAR file: {path}")
    return path


def _class_code_source(jvm: Any, class_name: str) -> tuple[Any, Path]:
    try:
        try:
            java_class = jvm.java.lang.Class.forName(class_name)
        except Exception:
            # Spark adds ``spark.jars`` through its context class loader.  The system
            # loader used by Class.forName may not see those driver resources.
            loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
            java_class = loader.loadClass(class_name)
        location = java_class.getProtectionDomain().getCodeSource().getLocation()
        return java_class, _file_uri_path(location)
    except NativeProvenanceError:
        raise
    except Exception as error:  # pragma: no cover - exact exception is JVM/version-specific
        raise NativeProvenanceError(f"Could not inspect loaded class {class_name}") from error


def _resource_urls(java_class: Any, class_name: str, jvm: Any | None = None) -> list[str]:
    resource_name = class_name.replace(".", "/") + ".class"
    loaders: list[Any] = []
    try:
        defining_loader = java_class.getClassLoader()
        if defining_loader is not None:
            loaders.append(defining_loader)
        if jvm is not None:
            try:
                context_loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
            except Exception:
                context_loader = None
            if context_loader is not None:
                loaders.append(context_loader)
        urls: list[str] = []
        seen: set[str] = set()
        for loader in loaders:
            resources = loader.getResources(resource_name)
            while resources.hasMoreElements():
                url = str(resources.nextElement())
                if url not in seen:
                    seen.add(url)
                    urls.append(url)
        return urls
    except Exception as error:  # pragma: no cover - exact exception is JVM/version-specific
        raise NativeProvenanceError(f"Could not inspect class resources {class_name}") from error


def _static_field(java_class: Any, field_name: str) -> str:
    try:
        field = java_class.getField(field_name)
        value = field.get(None)
    except Exception as error:  # pragma: no cover - exact exception is JVM/version-specific
        raise NativeProvenanceError(f"Native guard ABI field is unavailable: {field_name}") from error
    if not isinstance(value, str) or not value:
        raise NativeProvenanceError(f"Native guard ABI field is invalid: {field_name}")
    return value


def _manifest_for_verify(value: GuardBuildManifest | Mapping[str, Any] | str | Path) -> GuardBuildManifest:
    if isinstance(value, GuardBuildManifest):
        return value
    if isinstance(value, (str, Path)):
        return load_manifest(value)
    return GuardBuildManifest.from_mapping(value)


def verify_runtime_provenance(
    spark: Any,
    manifest: GuardBuildManifest | Mapping[str, Any] | str | Path,
    *,
    helper_jar: str | Path | None = None,
) -> GuardBuildManifest:
    """Verify a borrowed Spark session and loaded JAR provenance before any mutation.

    The checks use only JVM class/resource metadata and ``spark.version``.  No Spark configuration,
    catalog, table, FileIO, or session lifecycle method is changed or closed.
    """
    checked = _manifest_for_verify(manifest)
    if checked.helper_abi != HELPER_ABI:
        raise NativeProvenanceError("Native guard helper ABI does not match this product")
    if checked.helper_class != HELPER_CLASS:
        raise NativeProvenanceError("Native guard helper class does not match this product")
    if checked.helper_classes != HELPER_CLASSES:
        raise NativeProvenanceError("Native guard manifest helper class coverage is stale")
    if checked.spark_version != SPARK_VERSION or checked.iceberg_version != ICEBERG_VERSION:
        raise NativeProvenanceError("Native guard manifest runtime versions are unsupported")
    if checked.java_release != JAVA_RELEASE:
        raise NativeProvenanceError("Native guard manifest Java release is unsupported")
    if checked.runtime_jar_name != RUNTIME_JAR_NAME or checked.runtime_jar_sha256 != RUNTIME_JAR_SHA256:
        raise NativeProvenanceError("Native guard manifest Iceberg runtime provenance is stale")
    if checked.runtime_classes != RUNTIME_CLASSES:
        raise NativeProvenanceError("Native guard manifest runtime class coverage is stale")
    if checked.source_fingerprint != source_fingerprint():
        raise NativeProvenanceError("Native guard source fingerprint is stale")

    version = getattr(spark, "version", None)
    if callable(version):
        version = version()
    if str(version) != SPARK_VERSION:
        raise NativeProvenanceError(
            f"Native guard requires Spark {SPARK_VERSION}; loaded {version!r}"
        )
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        raise NativeProvenanceError("Borrowed Spark session has no accessible JVM")

    helper_class, helper_source = _class_code_source(jvm, checked.helper_class)
    helper_digest = sha256_file(helper_source)
    for helper_name in checked.helper_classes:
        if helper_name == checked.helper_class:
            helper_java_class, helper_source_path = helper_class, helper_source
        else:
            helper_java_class, helper_source_path = _class_code_source(jvm, helper_name)
        helper_urls = _resource_urls(helper_java_class, helper_name, jvm)
        if len(helper_urls) != 1:
            raise NativeProvenanceError(
                f"Duplicate or missing native guard class resources for {helper_name}: "
                f"{len(helper_urls)}"
            )
        if helper_source_path.resolve() != helper_source.resolve():
            raise NativeProvenanceError(
                f"Native guard helper classes came from multiple code sources: {helper_name}"
            )
        if helper_source_path.name != checked.helper_jar_name:
            raise NativeProvenanceError(
                f"Loaded native guard helper class came from an unexpected artifact: {helper_name}"
            )
        if helper_digest != checked.helper_jar_sha256:
            raise NativeProvenanceError(
                f"Loaded native guard helper JAR digest mismatch for {helper_name}"
            )
        if _static_field(helper_java_class, "HELPER_ABI") != checked.helper_abi:
            raise NativeProvenanceError(f"Loaded native guard ABI differs for {helper_name}")
        if _static_field(helper_java_class, "SOURCE_FINGERPRINT") != checked.source_fingerprint:
            raise NativeProvenanceError(
                f"Loaded native guard source fingerprint differs for {helper_name}"
            )
        if _static_field(helper_java_class, "RUNTIME_JAR_SHA256") != checked.runtime_jar_sha256:
            raise NativeProvenanceError(
                f"Loaded native guard runtime provenance differs for {helper_name}"
            )
    helper_path = Path(helper_jar) if helper_jar is not None else helper_source
    if not helper_path.is_file() or helper_path.name != checked.helper_jar_name:
        raise NativeProvenanceError("Loaded native guard JAR is not the manifest artifact")
    if sha256_file(helper_path) != checked.helper_jar_sha256:
        raise NativeProvenanceError("Provided native guard JAR digest does not match manifest")
    if helper_digest != checked.helper_jar_sha256:
        raise NativeProvenanceError("Loaded native guard JAR digest does not match manifest")

    runtime_source: Path | None = None
    runtime_digest: str | None = None
    for class_name in checked.runtime_classes:
        java_class, source = _class_code_source(jvm, class_name)
        urls = _resource_urls(java_class, class_name, jvm)
        if len(urls) != 1:
            raise NativeProvenanceError(
                f"Duplicate or missing Iceberg class resources for {class_name}: {len(urls)}"
            )
        if runtime_source is None:
            runtime_source = source
            runtime_digest = sha256_file(source)
        elif source.resolve() != runtime_source.resolve():
            raise NativeProvenanceError("Iceberg runtime classes came from multiple code sources")
        if source.name != checked.runtime_jar_name or runtime_digest != checked.runtime_jar_sha256:
            raise NativeProvenanceError(
                f"Loaded Iceberg runtime provenance mismatch for {class_name}"
            )
    if runtime_source is None:  # defensive; manifest parser rejects an empty list
        raise NativeProvenanceError("Native guard manifest has no runtime classes")
    return checked


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--runtime-jar", type=Path)
    parser.add_argument("--spark-jars-dir", type=Path)
    parser.add_argument("--spark-home", type=Path)
    parser.add_argument("--javac")
    parser.add_argument("--jar")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest = build_guard(
        args.output_dir,
        runtime_jar=args.runtime_jar,
        spark_jars_dir=args.spark_jars_dir,
        spark_home=args.spark_home,
        javac=args.javac,
        jar=args.jar,
    )
    print(json.dumps(manifest.as_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the explicit CLI
    raise SystemExit(main())
