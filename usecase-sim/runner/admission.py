"""Bounded admission: unknown cases are exclusive; declarations are opt-in."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from urllib.parse import urlsplit, unquote
from posixpath import normpath


def resource(value, root):
    if value.startswith("key:"):
        return value
    parsed = urlsplit(value)
    if parsed.scheme.lower() in {"s3", "s3a", "abfs", "abfss", "dbfs"}:
        scheme = {"s3a": "s3", "abfss": "abfs"}.get(parsed.scheme.lower(), parsed.scheme.lower())
        return f"{scheme}://{parsed.netloc.lower()}" + normpath(unquote(parsed.path)).rstrip("/")
    if parsed.scheme == "file":
        value = unquote(parsed.path)
    path = Path(value.replace("\\", "/"))
    if not path.is_absolute():
        path = Path(root) / path
    return path.resolve().as_posix().lower().rstrip("/")


def overlaps(a, b):
    return a == b or (not a.startswith("key:") and not b.startswith("key:") and
                     (a.startswith(b + "/") or b.startswith(a + "/")))


def declaration(scenario, root):
    declared = scenario.get("concurrency")
    if declared is None:
        return None  # Unknown setup/nested engines/mutations: run exclusively.
    if not isinstance(declared, dict) or set(declared) != {"resources", "engines", "isolation"}:
        raise ValueError("concurrency requires isolation, resources and engines")
    if declared["isolation"] not in {"declared", "fresh-local"}:
        raise ValueError("Unknown concurrency isolation")
    engines = declared["engines"]
    if not isinstance(engines, dict) or not engines or any(
            key not in {"spark", "polars"} or type(value) is not int or not 0 < value <= (1 if key == "spark" else 2)
            for key, value in engines.items()):
        raise ValueError("Invalid concurrency engine budget")
    if not engines.get(scenario.get("engine")):
        raise ValueError("concurrency must include the scenario's engine")
    resources = declared["resources"]
    if not isinstance(resources, list):
        raise ValueError("concurrency resources must be an array")
    parsed = []
    for item in resources:
        if not isinstance(item, dict) or set(item) != {"path", "mode"} or item["mode"] not in {"read", "write"} or not isinstance(item["path"], str) or not item["path"].strip():
            raise ValueError("Invalid concurrency resource")
        parsed.append((resource(item["path"], root), item["mode"]))
    # Cleanup declarations already own these paths. Derive their write locks
    # here instead of duplicating the same list in concurrency configuration.
    parsed.extend((resource(cleaned, root), "write") for cleaned in scenario.get("pre_clean_paths", []))
    if declared["isolation"] == "fresh-local" and (scenario.get("engine") != "polars" or
            scenario.get("platform", "local") != "local" or not scenario.get("skip_pipeline") or
            scenario.get("setup") or scenario.get("pre_clean_paths") or scenario.get("services") or
            "--root" in (scenario.get("validation") or {}).get("args", [])):
        raise ValueError("fresh-local requires a standalone Polars validator with generated fresh roots")
    if declared["isolation"] == "declared":
        _verify_file_case(scenario, parsed, root)
    return engines, parsed


def _verify_file_case(scenario, locks, root):
    """Reject hidden writes/engines in the initial file-only admission scope."""
    from runner.metadata_documents import load_metadata_files
    if scenario.get("metadata_type") != "file" or scenario.get("skip_pipeline") or scenario.get("setup") or scenario.get("engine_setup_function") or any(
            set(invocation) - {"label", "expected_exit_code", "timeout_seconds", "required_console_text"}
            for invocation in scenario.get("invocations", [])):
        raise ValueError("declared concurrency currently supports ordinary file-only cases")
    metadata = scenario.get("metadata_path")
    paths = [metadata] if isinstance(metadata, str) else metadata
    if not isinstance(paths, list) or not paths or any(
            not isinstance(path, str) or not path.strip() or "://" in path for path in paths):
        raise ValueError("declared concurrency requires local metadata")
    section_paths = {}
    for key in ("connections_path", "schema_hints_path"):
        value = scenario.get(key)
        if value is not None:
            if not isinstance(value, str) or not value.strip() or "://" in value:
                raise ValueError("declared concurrency requires local shared metadata")
            section_paths[key] = Path(root) / value
    document = load_metadata_files([Path(root) / path for path in paths],
        root=Path(root) / "usecase-sim", **section_paths)
    bindings = scenario.get("metadata_bindings")
    if bindings:
        from runner.metadata_bindings import bind_document
        document = bind_document(document, bindings)
    connections = {c["name"]: c for c in document.get("connections", [])}
    stages = {s.strip() for s in scenario.get("stage", "").split(",") if s.strip()}
    names = scenario.get("dataflows")
    if names is not None:
        if not isinstance(names, list) or not names or any(
            not isinstance(name, str) or not name for name in names
        ) or len(names) != len(set(names)):
            raise ValueError("dataflows must be a non-empty unique name list")
        available = {flow["name"] for flow in document["dataflows"]
                     if flow.get("is_active", True) and (not stages or flow.get("stage") in stages)}
        if not set(names) <= available:
            raise ValueError("dataflow selector is unknown or incompatible with stage")
    selected = 0
    for flow in document.get("dataflows", []):
        if not flow.get("is_active", True) or stages and flow.get("stage") not in stages:
            continue
        if names is not None and flow["name"] not in names:
            continue
        selected += 1
        if flow.get("processing_mode", "batch") != "batch":
            raise ValueError("Incremental flows require separate admission qualification")
        for side, mode in (("source", "read"), ("destination", "write")):
            endpoint = flow.get(side, {})
            connection = connections.get(endpoint.get("connection_name"), {})
            if connection.get("connection_type") != "file" or connection.get("format") != "parquet" or any(endpoint.get(key) for key in ("sql", "query", "function")):
                raise ValueError("Opaque or non-Parquet endpoint requires exclusive admission")
            table = endpoint.get("table")
            base = connection.get("configure", {}).get("base_path")
            if not table or not base:
                raise ValueError("Missing file endpoint path")
            target = resource(str(base).rstrip("/") + "/" + str(table), root)
            if not any((mode == "read" or owned_mode == "write") and
                       (target == path or target.startswith(path + "/")) for path, owned_mode in locks):
                raise ValueError(f"Undeclared {mode} endpoint: {target}")
    if not selected:
        raise ValueError("No selected flows for declared concurrency")


def compatible(candidate, running):
    if candidate is None or any(item is None for item in running):
        return not running
    engines, resources = candidate
    for engine, budget in (("spark", 1), ("polars", 2)):
        if engines.get(engine, 0) + sum(item[0].get(engine, 0) for item in running) > budget:
            return False
    return not any(overlaps(path, other) and (mode == "write" or other_mode == "write")
                   for path, mode in resources for _, occupied in running for other, other_mode in occupied)


def execute(names, declarations, dependencies, run, blocked, *, parallel, cancel=None):
    """Hold admission until run includes validation, receipt and child teardown."""
    pending, active, results = list(names), {}, {}
    selected = set(names)
    with ThreadPoolExecutor(max_workers=3 if parallel else 1) as pool:
        try:
            while pending or active:
                for name in list(pending):
                    prerequisites = dependencies.get(name, set()) & selected
                    if not prerequisites <= results.keys():
                        continue
                    if any(results[p] != 0 for p in prerequisites):
                        results[name] = blocked(name, "failed prerequisite")
                        pending.remove(name)
                        continue
                    occupied = [declarations[n] for n in active.values()]
                    if (active and not parallel) or not compatible(declarations[name], occupied):
                        continue
                    active[pool.submit(run, name)] = name
                    pending.remove(name)
                    if not parallel:
                        break
                if not active:
                    if pending:
                        raise ValueError(f"No ready scenario (dependency cycle): {pending}")
                    break
                completed, _ = wait(active, return_when=FIRST_COMPLETED)
                for future in completed:
                    name = active.pop(future)
                    # A lifecycle uncertainty is fatal: do not admit another case.
                    results[name] = future.result()
        except BaseException:
            if cancel:
                cancel()
            raise
    return {name: results[name] for name in names}
