"""Prepare the simulator's pinned native guard before any fixture or Spark job."""
from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile

SIM = Path(__file__).resolve().parents[1]
if str(SIM) not in sys.path:
    sys.path.insert(0, str(SIM))

from runner.runtime_paths import DATACOOLIE_ROOT, DEPENDENCIES_DIR, spark_dependencies_dir  # noqa: E402
from datacoolie.engines._spark.iceberg import native_build as native  # noqa: E402


MANIFEST_TOKEN = "{native_guard_manifest}"


def current_manifest_path():
    """Address a build by the source bytes and covered runtime classes."""
    return (DEPENDENCIES_DIR / "native-maintenance" /
            f"{native.source_fingerprint()}-{len(native.RUNTIME_CLASSES)}classes" /
            native.MANIFEST_NAME)


def resolve_scenario(scenario):
    """Expand the opt-in token once; explicit artifact pins remain unchanged.

    Resolve only authored simulator coordinates, never an existing artifact.
    The executing helper independently verifies the resulting coordinate before
    build/seed, so a host/container source mismatch still fails closed.
    """
    if scenario.get("maintenance_guard_manifest") != MANIFEST_TOKEN:
        return scenario
    coordinate = current_manifest_path().relative_to(DATACOOLIE_ROOT).as_posix()

    def replace(value):
        if isinstance(value, str):
            return coordinate if value == MANIFEST_TOKEN else value
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        return value

    return replace(scenario)


def _manifest_path(value):
    path = Path(value)
    if not path.is_absolute():
        path = DATACOOLIE_ROOT / path
    # Reject symlink ancestors before resolving the caller's artifact coordinate.
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError("Native guard path must not contain symlinks")
    path = path.resolve()
    expected = current_manifest_path().resolve()
    if path != expected:
        raise ValueError(f"Native guard manifest must use the canonical source coordinate: {expected}")
    return path


def runtime_inputs():
    """Locate existing pinned dependencies; never start Spark or fetch JARs."""
    import pyspark

    if pyspark.__version__ != native.SPARK_VERSION:
        raise native.NativeBuildError(f"Native guard requires PySpark {native.SPARK_VERSION}")
    spark_jars = Path(pyspark.__file__).resolve().parent / "jars"
    directories = [
        spark_dependencies_dir("spark35") / "jars",
        Path("/tmp/datacoolie-usecase-sim/dependencies/spark35/jars"),
        Path.home() / ".ivy2/jars",
        Path.home() / ".ivy2.5.2/jars",
        spark_jars,
    ]
    for directory in directories:
        candidate = directory / native.RUNTIME_JAR_NAME
        if candidate.is_file():
            if native.sha256_file(candidate) != native.RUNTIME_JAR_SHA256:
                raise native.NativeProvenanceError(f"Pinned Iceberg runtime digest mismatch: {candidate}")
            return candidate, spark_jars
    raise native.NativeBuildError(
        "Pinned Iceberg runtime JAR is absent from the simulator/Ivy cache; "
        "prepare Spark 3.5.9 / Iceberg 1.10.1 dependencies before fixture setup"
    )


def verify_artifact(manifest_path):
    """Check source/ABI/versions, class coverage and actual helper bytes."""
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    required = {"manifest_version", "source_fingerprint", "helper_abi", "helper_class",
                "helper_classes", "runtime_classes", "spark_version", "iceberg_version",
                "java_release", "runtime_jar", "helper_jar"}
    if not isinstance(raw, dict) or not required <= raw.keys():
        raise native.NativeProvenanceError("Native guard manifest is incomplete")
    manifest = native.load_manifest(manifest_path)
    expected = {
        "source_fingerprint": native.source_fingerprint(),
        "helper_abi": native.HELPER_ABI,
        "helper_class": native.HELPER_CLASS,
        "helper_classes": native.HELPER_CLASSES,
        "runtime_classes": native.RUNTIME_CLASSES,
        "spark_version": native.SPARK_VERSION,
        "iceberg_version": native.ICEBERG_VERSION,
        "java_release": native.JAVA_RELEASE,
        "runtime_jar_name": native.RUNTIME_JAR_NAME,
        "runtime_jar_sha256": native.RUNTIME_JAR_SHA256,
        "helper_jar_name": native.JAR_NAME,
    }
    for key, value in expected.items():
        if getattr(manifest, key) != value:
            raise native.NativeProvenanceError(f"Native guard manifest {key} is stale or incompatible")
    jar = manifest_path.with_name(native.JAR_NAME)
    if jar.is_symlink() or not jar.is_file() or native.sha256_file(jar) != manifest.helper_jar_sha256:
        raise native.NativeProvenanceError("Native guard JAR is missing or its digest differs")
    with zipfile.ZipFile(jar) as archive:
        for name in native.HELPER_CLASSES:
            if archive.namelist().count(name.replace(".", "/") + ".class") != 1:
                raise native.NativeProvenanceError(f"Native guard helper class is missing or duplicated: {name}")
        properties = archive.read(native.BUILD_RESOURCE).decode("utf-8")
    recorded = dict(line.split("=", 1) for line in properties.splitlines() if "=" in line)
    for key in ("source_fingerprint", "helper_abi", "runtime_jar_sha256",
                "spark_version", "iceberg_version", "java_release"):
        if recorded.get(key) != expected[key]:
            raise native.NativeProvenanceError(f"Native guard embedded {key} differs")
    return manifest


def prepare(value):
    manifest_path = _manifest_path(value)
    runtime_jar, spark_jars = runtime_inputs()
    if manifest_path.parent.exists():
        manifest = verify_artifact(manifest_path)
        action = "reused"
        publication = "existing"
    else:
        manifest_path.parent.parent.mkdir(parents=True, exist_ok=True)
        # Validate both files before publication. Existing builds are never
        # overwritten, and only a complete verified pair admits a scenario.
        with tempfile.TemporaryDirectory(prefix=".guard-building-", dir=manifest_path.parent.parent) as temporary:
            directory = Path(temporary)
            native.build_guard(directory, runtime_jar=runtime_jar, spark_jars_dir=spark_jars)
            verify_artifact(directory / native.MANIFEST_NAME)
            action = "built"
            try:
                publication = _publish(directory, manifest_path.parent)
            except FileExistsError:
                if not manifest_path.parent.exists():
                    raise
                # A concurrent owner may have published; it must pass the same checks.
                action = "reused"
                publication = "existing"
            manifest_path = _manifest_path(value)
            manifest = verify_artifact(manifest_path)
    return {"action": action, "publication": publication, "manifest": str(manifest_path),
            "source_fingerprint": manifest.source_fingerprint,
            "helper_jar_sha256": manifest.helper_jar_sha256,
            "runtime_jar_sha256": manifest.runtime_jar_sha256,
            "runtime_jar": str(runtime_jar), "spark_version": manifest.spark_version}


def _publish(source, destination):
    """Publish without replacing even an empty competing directory."""
    if os.name == "nt":
        os.rename(source, destination)  # Windows refuses an existing destination.
    elif sys.platform == "linux":
        libc = ctypes.CDLL(None, use_errno=True)
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1):
            code = ctypes.get_errno()
            if code in {errno.EINVAL, errno.ENOSYS, errno.ENOTSUP}:
                # Docker Desktop Windows bind mounts may reject renameat2 flags.
                # Reserve exclusively, then publish the manifest last. A partial
                # directory fails verification and is never automatically repaired.
                _publish_reserved(source, destination)
                return "exclusive-reservation"
            raise OSError(code, os.strerror(code), str(destination))
    else:
        raise OSError(errno.ENOTSUP, "Atomic guard publication requires Windows or Linux")
    return "atomic-rename"


def _publish_reserved(source, destination):
    destination.mkdir(exist_ok=False)
    try:
        for name in (native.JAR_NAME, native.MANIFEST_NAME):
            with (source / name).open("rb") as reader, (destination / name).open("xb") as writer:
                shutil.copyfileobj(reader, writer)
                writer.flush()
                os.fsync(writer.fileno())
    except OSError as exc:
        raise native.NativeBuildError(f"Native guard publication is incomplete: {destination}: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    print("SIM_NATIVE_GUARD=" + json.dumps(prepare(args.manifest)), flush=True)


if __name__ == "__main__":
    main()
