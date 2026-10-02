"""Render runtime fields without evaluating dataclass default factories."""

from __future__ import annotations

from dataclasses import MISSING, fields

from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.logging.configuration.config import LogConfig


RUN_NOTES = {
    "job_id": "Caller correlation identity; must be non-empty. Logging also supplies session and operation identities.",
    "job_num": "Total shard count; at least 1.",
    "job_index": "Zero-based shard index: 0 <= job_index < job_num.",
    "max_workers": "Maximum concurrent dataflow workers; at least 1.",
    "stop_on_error": "Stop admitting new dataflows after a failure; already admitted work may finish.",
    "retry_count": "Additional attempts after a failed execution; non-negative.",
    "retry_delay": "Seconds between retries; non-negative.",
    "dry_run": "Use the Driver dry-run validation path. This still prepares runtime dependencies; it is not an offline schema-only check.",
    "retention_hours": "Retention passed to destination maintenance; non-negative. Backend support determines the effect.",
    "allowed_function_prefixes": "Import-path prefixes for Python function sources. An empty list applies no prefix restriction. A non-empty list requires the function path to start with one of these strings; use trusted module prefixes such as my_project.sources. to constrain imports.",
    "run_attributes": "Optional caller correlation object, copied on construction. Keys must be strings; nested values must be JSON-compatible with finite numbers and no cycles. Raw JSON strings and arbitrary objects are rejected.",
}
REPLAY_NOTES = {
    "start": "Required inclusive lower bound, not None.",
    "end": "Required exclusive upper bound, not None.",
    "chunk_interval": "None selects one bounded read. Time chunks use years/months/weeks/days/hours/minutes; integer chunks use {\"step\": N}. Constructor checks the mapping shape; execution validates the interval and range.",
    "save_watermark": "Persist source-observed watermarks after successful chunks. Does not create a replay checkpoint or skip chunks on a later run.",
    "chunk_column": "Non-empty override, or the first source watermark column. The reader must support bounded reads for the selected column.",
}
LOG_NOTES = {
    "log_level": "Console threshold: DEBUG, INFO, WARNING, ERROR, CRITICAL. Names normalize to uppercase.",
    "file_level": "Persisted Python-record threshold; same supported levels as log_level.",
    "storage_mode": "Local capture: memory or file; names normalize to lowercase.",
    "output_path": "Standalone logger component root, or None for no remote writer. Must be a non-empty path when supplied. Driver log_base_path instead creates category roots.",
    "partition_by_date": "Boolean controlling UTC date partitioning.",
    "partition_pattern": "Non-empty template with simple placeholders in ordered prefix: {year}, {month}, {day}, {hour}. Each path segment needs a placeholder; literals cannot contain digits, %, or braces.",
    "persistence_mode": "snapshot or batch; names normalize to lowercase. See the logging guide for write layout and backend requirements.",
    "flush_interval_seconds": "Finite non-negative seconds; 0 disables time-triggered flushing. Booleans are rejected.",
    "flush_batch_bytes": "Positive integer byte threshold; booleans are rejected.",
    "buffer_memory_bytes": "Positive integer memory buffer budget; booleans are rejected.",
    "spool_max_bytes": "Positive integer spool budget, at least buffer_memory_bytes; booleans are rejected.",
    "spool_directory": "Optional non-empty local spool path; normalized when supplied.",
    "close_timeout_seconds": "Positive finite close timeout; booleans are rejected.",
    "console_color": "auto, always, or never; names normalize to lowercase. Auto respects NO_COLOR and TERM=dumb before terminal detection.",
}


def _default_label(field) -> str:
    if field.default is not MISSING:
        return f"`{field.default!r}`"
    if field.default_factory is list:
        return "new `[]` per instance"
    if field.default_factory is not MISSING:
        return f"generated per instance (`{field.default_factory.__name__}`)"
    return "required"


def render_field_table(model: type, notes: dict[str, str]) -> str:
    """Fail on undocumented fields rather than silently dropping new options."""
    declared = fields(model)
    if {field.name for field in declared} != set(notes):
        raise ValueError(f"Runtime field notes disagree with {model.__name__}")
    rows = ["| Field | Python type | Default | Meaning / constraints |", "|---|---|---|---|"]
    for field in declared:
        type_label = str(field.type).replace("|", "\\|")
        meaning = notes[field.name].replace("|", "\\|")
        rows.append(f"| `{field.name}` | `{type_label}` | {_default_label(field)} | {meaning} |")
    return "\n".join(rows)


def _api(model: str, *, hide_signature: bool = False) -> str:
    return (
        f"::: {model}\n    options:\n      show_bases: false\n"
        "      members_order: source\n      show_source: false\n"
        + ("      show_signature: false\n" if hide_signature else "")
    )


def render_runtime_reference() -> str:
    """Preserve the reference route and existing section headings."""
    return "\n\n".join([
        "---\ntitle: Runtime configuration reference | DataCoolie\ndescription: Python runtime fields, defaults and constraints for Driver sessions, replay and logging.\n---",
        "# Runtime configuration",
        "Runtime configuration is distinct from authored metadata. A runner supplies these values when creating a Driver session; the Driver does not read a project manifest at runtime. Types and defaults below come from the installed source models. Semantic constraints describe current runtime behavior.",
        "## Run configuration",
        "`DataCoolieRunConfig` accepts declared fields as keyword arguments through its model constructor. Its dataclass declaration disables the generated initializer, so an empty generated signature would be misleading. These fields are not keys in authored metadata JSON.",
        render_field_table(DataCoolieRunConfig, RUN_NOTES),
        '```python\nfrom datacoolie.core import DataCoolieRunConfig\n\nconfig = DataCoolieRunConfig(\n    job_id="nightly-orders", max_workers=2, retry_count=1,\n    run_attributes={"scheduler": "local", "attempt": 1},\n)\n```',
        "Supply this object as `config=` to `DataCoolieDriver`. The [create_driver factory](api/orchestration.md#datacoolie.orchestration.factory.create_driver) instead accepts run fields such as `job_id`, `max_workers` and `retry_count` directly; it builds the configuration object internally. See [runtime setup](../guide/operations/runtime-configuration.md) for a full runner.",
        _api("datacoolie.core.models.run_config.DataCoolieRunConfig", hide_signature=True),
        "## Replay configuration",
        render_field_table(ReplayConfig, REPLAY_NOTES),
        "Replay uses `[start, end)`. Construction does not prove reader capabilities or validate every range/interval combination; execution performs those checks. See [replay and backfill](../guide/operations/replay-and-backfill.md).",
        _api("datacoolie.core.models.run_config.ReplayConfig"),
        "## Logging configuration",
        render_field_table(LogConfig, LOG_NOTES),
        "Logging capture is bounded. Buffer or spool exhaustion can lose records and is reported through logging statistics; logging persistence is not a transactional guarantee for business execution. For activation, flush, close and storage behavior, see [logging](../guide/operations/logging.md) and the [logging API](api/logging.md).",
        _api("datacoolie.logging.configuration.config.LogConfig"),
        "",
    ])

