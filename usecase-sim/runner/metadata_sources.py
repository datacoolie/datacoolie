"""Canonical metadata selections and in-memory focused-case projections.

Recipes contain names and step bindings. Flow definitions remain in the flat
group files; every read returns a separate projection for its invocation.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from runner.metadata_documents import load_metadata_files
from runner.runtime_paths import USECASE_SIM_DIR


def substitute_tokens(document, tokens):
    """Replace explicit markers in parsed string values without editing keys."""
    if not isinstance(tokens, dict) or any(
        not isinstance(key, str) or not key or not isinstance(value, str)
        for key, value in tokens.items()
    ):
        raise ValueError("tokens must map non-empty markers to string values")
    if isinstance(document, str):
        for marker, value in tokens.items():
            document = document.replace(marker, value)
        return document
    if isinstance(document, list):
        return [substitute_tokens(value, tokens) for value in document]
    if isinstance(document, dict):
        return {key: substitute_tokens(value, tokens) for key, value in document.items()}
    return copy.deepcopy(document)


def shared_paths(root=USECASE_SIM_DIR):
    return {
        "connections_path": str(Path(root) / "metadata/file/connections.json"),
        "schema_hints_path": str(Path(root) / "metadata/file/schema_hints.json"),
    }


def group_document(group, *, root=USECASE_SIM_DIR):
    """Read one complete flat group with the complete shared sections."""
    if not isinstance(group, str) or Path(group).name != group or not group:
        raise ValueError("group must be one metadata group name")
    return load_metadata_files(
        Path(root) / "metadata/file" / f"{group}.json", root=root,
        **shared_paths(root),
    )


def recipe_document(group, recipe_name, *, tokens=None, root=USECASE_SIM_DIR):
    """Return the exact authored flow/connection projection for a recipe."""
    from runner.metadata_bindings import bind_document

    key = f"{group}/{Path(recipe_name).stem}"
    recipes = json.loads((Path(root) / "scenarios/metadata_recipes.json").read_text(encoding="utf-8"))
    if key not in recipes:
        raise ValueError(f"unknown metadata recipe: {key}")
    recipe = recipes[key]
    document = group_document(group, root=root)
    document = bind_document(document, recipe.get("metadata_bindings", {}))
    by_name = {flow["name"]: flow for flow in document["dataflows"]}
    connections = {row["name"]: row for row in document["connections"]}
    result = {
        "connections": [connections[name] for name in recipe["connections"]],
        "dataflows": [by_name[name] for name in recipe["dataflows"]],
        "schema_hints": [hint for hint in document["schema_hints"]
                         if hint.get("connection_name") in recipe["connections"]
                         or hint.get("connection_id") in {
                             connections[name].get("connection_id")
                             for name in recipe["connections"]
                             if connections[name].get("connection_id")
                         }],
    }
    return substitute_tokens(result, tokens or {})


def case_bindings(case):
    """Describe complete projected rows against their canonical named owners."""
    document = copy.deepcopy(case.document)
    return {
        section: {row["name"]: {"replace": row} for row in document.get(section, [])}
        for section in ("connections", "dataflows")
        if document.get(section)
    }


def make_case_provider(case, *, stage=None, maintenance=False, **kwargs):
    """Keep authored cases on canonical reads and generated cases in memory."""
    from runner.metadata_bindings import BoundFileProvider, MemoryFileProvider

    if case.sources:
        stages = {name.strip() for name in stage.split(",") if name.strip()} if isinstance(stage, str) else set(stage or [])
        names = [row["name"] for row in case.document["dataflows"]
                 if not stages or row.get("stage") in stages]
        if maintenance:
            connections = {row["name"]: row for row in case.document["connections"]}
            names = [row["name"] for row in case.document["dataflows"]
                     if row["name"] in names and connections[row["destination"]["connection_name"]].get("format") in {"delta", "iceberg"}]
        if not names:
            raise ValueError(f"case has no selected dataflow for stage: {stage}")
        return BoundFileProvider(
            config_path=list(case.sources), **shared_paths(),
            bindings=case_bindings(case),
            dataflow_names=names,
            **kwargs,
        )
    return MemoryFileProvider(document=case.document, **kwargs)


def child_metadata_args(case):
    """Transport a case projection to a child without a JSON intermediate."""
    if not case.sources:
        return ["--metadata-document", json.dumps(case.document)]
    result = []
    for source in case.sources:
        result.extend(["--metadata-path", source])
    for key, path in shared_paths().items():
        result.extend(["--" + key.replace("_", "-"), path])
    for flow in case.document["dataflows"]:
        result.extend(["--dataflow", flow["name"]])
    result.extend(["--metadata-bindings", json.dumps(case_bindings(case))])
    return result


def metadata_provenance(document, *, sources=(), bindings=None, root=USECASE_SIM_DIR):
    """Record immutable input hashes and binding fields without secret values."""
    from datacoolie.utils.identity import name_to_uuid

    files = []
    for source in dict.fromkeys(sources):
        path = Path(source)
        if not path.is_absolute():
            path = Path(root).parent / path
        path = path.resolve()
        try:
            label = path.relative_to(Path(root).resolve()).as_posix()
        except ValueError:
            label = path.name
        try:
            fingerprint = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            fingerprint = None
        files.append({"path": label, "sha256": fingerprint})

    def fields(value, prefix=""):
        if isinstance(value, dict):
            return [field for key, child in value.items()
                    for field in fields(child, f"{prefix}.{key}" if prefix else key)]
        if isinstance(value, list):
            return [field for index, child in enumerate(value)
                    for field in fields(child, f"{prefix}[{index}]" )]
        return [prefix]

    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return {
        "source_kind": "file" if files else "memory",
        "sources": files,
        "dataflows": [{"name": row["name"], "stage": row.get("stage"),
                       "dataflow_id": row.get("dataflow_id") or name_to_uuid(row["name"])}
                      for row in document.get("dataflows", [])],
        "binding_fields": sorted(fields(bindings or {})),
        "effective_snapshot_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def case_metadata_provenance(case, *, stage=None, maintenance=False):
    sources = list(case.sources)
    if sources:
        sources.extend(shared_paths().values())
        sources.append(str(USECASE_SIM_DIR / "scenarios/metadata_recipes.json"))
    document = copy.deepcopy(case.document)
    if stage:
        stages = {value.strip() for value in stage.split(",")} if isinstance(stage, str) else set(stage)
        document["dataflows"] = [row for row in document.get("dataflows", []) if row.get("stage") in stages]
    if maintenance:
        connections = {row["name"]: row for row in document.get("connections", [])}
        document["dataflows"] = [row for row in document.get("dataflows", [])
                                if connections[row["destination"]["connection_name"]].get("format") in {"delta", "iceberg"}]
    return metadata_provenance(document, sources=sources, bindings=case_bindings(case))


def scenario_metadata_provenance(scenario):
    """Hash exact file inputs and each effective invocation selection."""
    from runner.metadata_bindings import bind_document

    selections = []
    for invocation in scenario.get("invocations", [{}]):
        effective = {**scenario, **invocation}
        paths = effective.get("metadata_path", [])
        paths = [paths] if isinstance(paths, str) else paths
        if not paths:
            continue
        sections = {key: effective[key] for key in ("connections_path", "schema_hints_path") if effective.get(key)}
        sources = [*paths, *sections.values()]
        try:
            document = bind_document(load_metadata_files(paths, **sections), effective.get("metadata_bindings", {}))
            selected = effective.get("dataflows")
            if selected:
                by_name = {row["name"]: row for row in document["dataflows"]}
                document["dataflows"] = [by_name[name] for name in selected]
            stage = effective.get("stage")
            if stage:
                stages = {value.strip() for value in stage.split(",")}
                document["dataflows"] = [row for row in document["dataflows"] if row.get("stage") in stages]
            selections.append(metadata_provenance(document, sources=sources, bindings=effective.get("metadata_bindings")))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            # Startup-invalid probes still need the exact source hash. No raw
            # exception or metadata values belong in a provenance receipt.
            snapshot = metadata_provenance({}, sources=sources)
            snapshot["selection_error"] = type(exc).__name__
            selections.append(snapshot)
    return selections


__all__ = ["case_bindings", "case_metadata_provenance", "metadata_provenance",
           "scenario_metadata_provenance", "child_metadata_args", "group_document", "make_case_provider",
           "recipe_document", "shared_paths", "substitute_tokens"]
