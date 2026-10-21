"""Opt-in simulator execution profiles and conservative capability discovery."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path

from runner.runtime_timing import phase

MODE_ENV = "DATACOOLIE_SIM_EXECUTION_MODE"


def optimized():
    return os.environ.get(MODE_ENV, "sequential") == "optimized"


def functional_spark_config():
    return {"spark.master": "local[4]", "spark.driver.memory": "4g",
            "spark.driver.maxResultSize": "1g", "spark.sql.shuffle.partitions": "8",
            "spark.sql.sources.parallelPartitionDiscovery.parallelism": "8",
            "spark.databricks.delta.snapshotPartitions": "8"}


def ivy_cache(original, profile):
    """Move dependency I/O off the Windows mount; never move fixture outputs."""
    if not optimized() or not Path("/.dockerenv").is_file():
        return original
    root = Path("/tmp/datacoolie-usecase-sim") / "dependencies" / profile
    if not root.exists():
        with phase("cache_preparation_s"):
            root.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{profile}.preparing-", dir=root.parent))
            try:
                if original.exists():
                    shutil.copytree(original, temporary, dirs_exist_ok=True)
                try:
                    temporary.rename(root)
                except OSError:
                    if not root.is_dir():
                        raise
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)
    return root


def _raw_metadata_capabilities(path, *, stage=None, connection=None, maintenance=False,
                               skip_api_sources=False):
    """Inspect one embedded JSON document without constructing provider models.

    This compatibility path intentionally remains permissive about incomplete
    raw documents.  It is retained for existing scalar callers and fixtures;
    direct list/shared inputs use the provider-backed route below.
    """
    if not path or str(path).startswith(("s3:", "s3a:", "abfs:", "dbfs:")):
        return None
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(document, dict) or not isinstance(document.get("dataflows"), list):
        return None
    if not isinstance(document.get("connections"), list):
        return None
    connections = {c.get("name"): c for c in document["connections"] if isinstance(c, dict)}
    stages = {s.strip() for s in stage.split(",")} if stage else None
    targets = {s.strip() for s in connection.split(",")} if connection else None
    formats, types, paths = set(), set(), []
    selected = 0
    for flow in document["dataflows"]:
        if not isinstance(flow, dict):
            return None
        if not flow.get("is_active", True) or stages and flow.get("stage") not in stages:
            continue
        source, destination = flow.get("source", {}), flow.get("destination", {})
        if not isinstance(source, dict) or not isinstance(destination, dict):
            return None
        if not maintenance and (source.get("sql") or source.get("query") or source.get("function")):
            return None
        src = connections.get(source.get("connection_name"))
        dst = connections.get(destination.get("connection_name"))
        if dst is None or src is None:
            return None
        if skip_api_sources and src.get("connection_type") == "api":
            continue
        if maintenance:
            if targets and not targets.intersection({dst.get("name"), dst.get("connection_id")}):
                continue
            if dst.get("format") not in {"delta", "iceberg"}:
                continue
            used = [dst]
        else:
            used = [src, dst]
        selected += 1
        for value in used:
            if value.get("connection_type") not in {"file", "lakehouse", "database", "api"}:
                return None
            formats.add(value.get("format", ""))
            types.add(value.get("connection_type", ""))
            if not isinstance(value.get("configure", {}), dict):
                return None
            paths.append(str(value.get("configure", {}).get("base_path", "")))
    if not selected:
        return None
    # Package selection is coarser than individual dialect driver selection.
    return {"needs_iceberg": "iceberg" in formats,
            "needs_s3": any(p.startswith(("s3:", "s3a:")) for p in paths),
            "needs_jdbc": bool(types & {"database", "sql", "function"})}


_REMOTE_METADATA_PREFIXES = ("s3:", "s3a:", "abfs:", "abfss:", "dbfs:")


def _as_path_values(value):
    """Return provider path values as strings, preserving explicit order."""
    if isinstance(value, (str, os.PathLike)):
        return [os.fspath(value)]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [os.fspath(item) if isinstance(item, os.PathLike) else item for item in value]
    return None


def _is_exact_local_json_path(value) -> bool:
    """Return whether *value* names an existing local JSON file exactly."""
    if isinstance(value, os.PathLike):
        value = os.fspath(value)
    if not isinstance(value, str):
        return False
    value = value.strip()
    if not value or value.lower().startswith(_REMOTE_METADATA_PREFIXES) or "://" in value:
        return False
    candidate = Path(value).expanduser()
    return candidate.suffix.lower() == ".json" and candidate.is_file()


def _provider_input_paths(path, connections_path, schema_hints_path):
    """Validate and normalize the local JSON inputs for provider inspection."""
    try:
        paths = _as_path_values(path)
        if not paths or any(not _is_exact_local_json_path(item) for item in paths):
            return None
        for item in (connections_path, schema_hints_path):
            if item is not None and not _is_exact_local_json_path(item):
                return None
        return paths
    except (OSError, TypeError, ValueError):
        return None


def _connection_value(connection, field):
    """Read an enum-or-string connection field as a normalized string."""
    value = getattr(connection, field, None)
    value = getattr(value, "value", value)
    return value.strip().lower() if isinstance(value, str) else ""


def _model_endpoint_capabilities(dataflows, *, skip_api_sources=False, maintenance=False):
    """Derive dependency flags from selected provider dataflow models."""
    formats, types, paths = set(), set(), []
    selected = 0
    for dataflow in dataflows:
        source = getattr(dataflow, "source", None)
        destination = getattr(dataflow, "destination", None)
        source_connection = getattr(source, "connection", None)
        destination_connection = getattr(destination, "connection", None)
        if source_connection is None or destination_connection is None:
            return None

        source_type = _connection_value(source_connection, "connection_type")
        if skip_api_sources and source_type == "api":
            continue

        destination_format = _connection_value(destination_connection, "format")
        if maintenance and destination_format not in {"delta", "iceberg"}:
            continue

        # Query/function sources are intentionally opaque for ordinary reads;
        # maintenance depends only on its selected destination endpoint.
        if not maintenance and (
            getattr(source, "query", None) or getattr(source, "python_function", None)
        ):
            return None

        used = [destination_connection] if maintenance else [
            source_connection,
            destination_connection,
        ]
        for connection in used:
            connection_type = _connection_value(connection, "connection_type")
            if connection_type not in {"file", "lakehouse", "database", "api"}:
                return None
            formats.add(_connection_value(connection, "format"))
            types.add(connection_type)
            base_path = getattr(connection, "base_path", None)
            if base_path is None:
                configure = getattr(connection, "configure", {})
                if isinstance(configure, dict):
                    base_path = configure.get("base_path")
            if base_path is not None:
                paths.append(str(base_path))
        selected += 1

    if not selected:
        return None
    return {
        "needs_iceberg": "iceberg" in formats,
        "needs_s3": any(path.startswith(("s3:", "s3a:")) for path in paths),
        "needs_jdbc": bool(types & {"database", "sql"}),
    }


def provider_capabilities(provider, *, stage=None, connection=None, maintenance=False,
                          skip_api_sources=False):
    """Return capabilities for a provider's selected, validated models.

    The first public getter initializes the provider's complete metadata scope;
    the second getter applies the stage/maintenance selector to that snapshot.
    Any provider/model failure remains conservative so normal execution keeps
    ownership of its startup diagnostics.
    """
    try:
        # Force the complete connection/dataflow snapshot before applying a
        # selector.  FileProvider validates all models during this read.
        connections = provider.get_connections(active_only=False)
        if not isinstance(connections, list):
            return None
        if maintenance:
            connection_selector = connection if connection else None
            dataflows = provider.get_maintenance_dataflows(
                connection=connection_selector,
                active_only=True,
                attach_schema_hints=False,
            )
        else:
            dataflows = provider.get_dataflows(
                stage=stage,
                active_only=True,
                attach_schema_hints=False,
            )
        if not isinstance(dataflows, list):
            return None
        return _model_endpoint_capabilities(
            dataflows,
            skip_api_sources=skip_api_sources,
            maintenance=maintenance,
        )
    except Exception:
        return None


def _provider_metadata_capabilities(path, *, stage=None, connection=None,
                                    maintenance=False, skip_api_sources=False,
                                    connections_path=None, schema_hints_path=None,
                                    metadata_bindings=None, dataflow_names=None):
    """Inspect exact local JSON inputs through a temporary provider.

    Bound inputs use the simulator adapter so capability decisions see the
    same token/patch/delete projection as execution; ordinary inputs retain
    the product FileProvider class and its normal diagnostics.
    """
    paths = _provider_input_paths(path, connections_path, schema_hints_path)
    if paths is None:
        return None

    try:
        from runner.metadata_bindings import build_file_provider
        from datacoolie.platforms.local_platform import LocalPlatform
    except Exception:
        return None

    provider = None
    result = None
    try:
        kwargs = {"config_path": paths, "platform": LocalPlatform()}
        if connections_path is not None:
            kwargs["connections_path"] = os.fspath(connections_path)
        if schema_hints_path is not None:
            kwargs["schema_hints_path"] = os.fspath(schema_hints_path)
        if metadata_bindings is None and dataflow_names is None:
            # Resolve the class at call time so tests and host integrations
            # that replace the product FileProvider keep their normal hook.
            from datacoolie.metadata.file_provider import FileProvider
            provider = FileProvider(**kwargs)
        else:
            provider = build_file_provider(
                **kwargs,
                metadata_bindings=metadata_bindings,
                dataflows=dataflow_names,
            )
        result = provider_capabilities(
            provider,
            stage=stage,
            connection=connection,
            maintenance=maintenance,
            skip_api_sources=skip_api_sources,
        )
    except Exception:
        result = None
    finally:
        if provider is not None:
            try:
                provider.close()
            except Exception:
                # A capability hint must never hide execution startup or a
                # provider cleanup failure; keep the answer conservative.
                result = None
    return result


def metadata_capabilities(path, *, stage=None, connection=None, maintenance=False,
                          skip_api_sources=False, connections_path=None,
                          schema_hints_path=None, metadata_bindings=None,
                          dataflow_names=None):
    """Return selected capabilities, or None for opaque/unknown metadata.

    Scalar embedded JSON keeps its compatibility reader.  Exact local JSON
    lists and explicit shared section files are inspected with FileProvider so
    ordered sections and provider model validation remain authoritative.
    Bindings/selectors always use the exact-file route and the same projection
    adapter as the execution provider.
    """
    provider_route = (
        connections_path is not None
        or schema_hints_path is not None
        or metadata_bindings is not None
        or dataflow_names is not None
        or not isinstance(path, (str, os.PathLike))
    )
    if provider_route:
        return _provider_metadata_capabilities(
            path,
            stage=stage,
            connection=connection,
            maintenance=maintenance,
            skip_api_sources=skip_api_sources,
            connections_path=connections_path,
            schema_hints_path=schema_hints_path,
            metadata_bindings=metadata_bindings,
            dataflow_names=dataflow_names,
        )
    return _raw_metadata_capabilities(
        path,
        stage=stage,
        connection=connection,
        maintenance=maintenance,
        skip_api_sources=skip_api_sources,
    )
