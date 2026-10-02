"""Transform model and declarative transformation rules."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, TypeVar

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.converters import convert_to_bool
from datacoolie.utils.collections import ensure_list

from datacoolie.core.models.base import CompatModel, _parse_json_object


_ModelT = TypeVar("_ModelT", bound=CompatModel)


_MAX_PORTABLE_REGEX_LENGTH = 4096
_REGEX_ESCAPED_LITERALS = frozenset(r".^$*+?{}[]\|()-")
_REGEX_CONTROL_ESCAPES = frozenset("nrtf")
_REGEX_UNSUPPORTED_ESCAPES = frozenset("dDsSwWbBAZGpPkK")


@dataclass(init=False)
class SchemaHint(CompatModel):
    """Column-level type hint applied by the transform schema converter."""

    column_name: str
    data_type: str
    format: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default_value: Optional[str] = None
    ordinal_position: Optional[int] = 0
    is_active: bool = True

    @classmethod
    def _must_be_non_empty(cls, v: Any, field_name: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError(f"{field_name} must be a non-empty string")
        return v

    @staticmethod
    def _optional_int(value: Any, field_name: str) -> Optional[int]:
        """Normalize JSON/Excel numeric fields without truncating decimals."""

        if value is None or value == "":
            return None
        if isinstance(value, bool):
            raise ConfigurationError(f"{field_name} must be an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text and (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
                return int(text)
        raise ConfigurationError(f"{field_name} must be an integer")

    def __post_init__(self) -> None:
        self.column_name = self._must_be_non_empty(self.column_name, "column_name")
        self.data_type = self._must_be_non_empty(self.data_type, "data_type")
        self.precision = self._optional_int(self.precision, "precision")
        self.scale = self._optional_int(self.scale, "scale")
        self.ordinal_position = self._optional_int(
            self.ordinal_position, "ordinal_position"
        )
        try:
            self.is_active = convert_to_bool(self.is_active)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError("is_active must be a boolean") from exc


@dataclass(init=False)
class AdditionalColumn(CompatModel):
    """Computed column added during the transform phase."""

    column: str
    expression: str

    @classmethod
    def _must_be_non_empty(cls, v: Any, field_name: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ConfigurationError(f"{field_name} must be a non-empty string")
        return v

    def __post_init__(self) -> None:
        self.column = self._must_be_non_empty(self.column, "column")
        self.expression = self._must_be_non_empty(self.expression, "expression")


def _validate_portable_regex(
    pattern: str,
    *,
    field_path: str = "value_rules.pattern",
) -> str:
    """Validate the portable-regex invariant owned by ``ValueRule``."""
    if not isinstance(pattern, str):
        raise ConfigurationError(
            "regex_replace rule requires a string pattern",
            details={"field": field_path},
        )
    if len(pattern) > _MAX_PORTABLE_REGEX_LENGTH:
        raise ConfigurationError(
            f"Portable regex patterns must not exceed {_MAX_PORTABLE_REGEX_LENGTH} characters",
            details={"field": field_path, "length": len(pattern)},
        )

    group_stack: list[dict[str, bool]] = []
    in_class = False
    escaped = False
    index = 0

    while index < len(pattern):
        char = pattern[index]

        if escaped:
            if char.isdigit() or char in _REGEX_UNSUPPORTED_ESCAPES:
                _portable_regex_error(
                    field_path, index - 1, f"unsupported escape \\{char}"
                )
            if (
                char not in _REGEX_ESCAPED_LITERALS
                and char not in _REGEX_CONTROL_ESCAPES
            ):
                _portable_regex_error(
                    field_path, index - 1, f"unsupported escape \\{char}"
                )
            escaped = False
            index += 1
            continue

        if char == "\\":
            escaped = True
            index += 1
            continue

        if in_class:
            if char == "]":
                in_class = False
            elif pattern[index : index + 2] in {"&&", "--", "~~"}:
                _portable_regex_error(
                    field_path,
                    index,
                    "character-class set operations are unsupported",
                )
            index += 1
            continue

        if char == "[":
            in_class = True
            index += 1
            continue

        if char == "(":
            if group_stack:
                group_stack[-1]["nested"] = True
            if pattern[index : index + 3] == "(?:":
                index += 3
            elif pattern[index : index + 2] == "(?":
                _portable_regex_error(
                    field_path,
                    index,
                    "lookaround, named groups, and inline flags are unsupported",
                )
            else:
                index += 1
            group_stack.append(
                {"nested": False, "quantified": False, "alternation": False}
            )
            continue

        if char == ")":
            if not group_stack:
                _portable_regex_error(field_path, index, "unbalanced closing group")
            group = group_stack.pop()
            next_index = index + 1
            is_quantified = next_index < len(pattern) and pattern[next_index] in "*+?{"
            if is_quantified and any(group.values()):
                _portable_regex_error(
                    field_path,
                    next_index,
                    "quantified groups containing nesting, quantifiers, or alternation are unsupported",
                )
            index += 1
            continue

        if group_stack and char in "*+?{":
            group_stack[-1]["quantified"] = True
        elif group_stack and char == "|":
            group_stack[-1]["alternation"] = True

        if char in "*+?" and index + 1 < len(pattern) and pattern[index + 1] == "+":
            _portable_regex_error(
                field_path, index, "possessive quantifiers are unsupported"
            )

        index += 1

    if escaped:
        _portable_regex_error(field_path, len(pattern) - 1, "trailing escape")
    if in_class:
        _portable_regex_error(field_path, len(pattern) - 1, "unclosed character class")
    if group_stack:
        _portable_regex_error(field_path, len(pattern) - 1, "unclosed group")

    try:
        re.compile(pattern)
    except re.error as exc:
        raise ConfigurationError(
            "Invalid portable regex pattern",
            details={"field": field_path, "reason": str(exc)},
        ) from exc
    return pattern


def _portable_regex_error(field_path: str, index: int, reason: str) -> None:
    raise ConfigurationError(
        "Unsupported portable regex pattern",
        details={"field": field_path, "index": max(index, 0), "reason": reason},
    )


@dataclass(init=False)
class ValueRule(CompatModel):
    """Typed, engine-portable value normalization rule."""

    forbid_unknown_fields: ClassVar[bool] = True

    operation: str
    columns: List[str] = field(default_factory=list)
    order: int = 100
    mode: Optional[str] = None
    pattern: Optional[str] = None
    replacement: str = ""
    value: Any = None
    mapping: Dict[str, str] = field(default_factory=dict)
    on_unmapped: str = "keep"

    def __post_init__(self) -> None:
        self.operation = str(self.operation).strip().lower()
        self.columns = ensure_list(self.columns)
        self.mode = (
            self.mode.strip().lower() if isinstance(self.mode, str) else self.mode
        )
        self.on_unmapped = str(self.on_unmapped).strip().lower()
        supported = {
            "trim",
            "case",
            "regex_replace",
            "empty_to_null",
            "fill_null",
            "map",
        }
        if self.operation not in supported:
            raise ConfigurationError(
                f"Unsupported value rule operation: {self.operation!r}",
                details={"supported": sorted(supported)},
            )
        _validate_column_list(self.columns, "value_rules.columns")
        if (
            not isinstance(self.order, int)
            or isinstance(self.order, bool)
            or self.order < 0
        ):
            raise ConfigurationError("value_rules.order must be a non-negative integer")
        if self.operation == "case" and self.mode not in {"lower", "upper"}:
            raise ConfigurationError("case rule requires mode 'lower' or 'upper'")
        if self.operation == "regex_replace" and not isinstance(self.pattern, str):
            raise ConfigurationError("regex_replace rule requires a string pattern")
        if self.operation == "regex_replace":
            self.pattern = _validate_portable_regex(self.pattern or "")
        if not isinstance(self.replacement, str):
            raise ConfigurationError("value_rules.replacement must be a string")
        if self.operation == "fill_null" and (
            self.value is None or isinstance(self.value, (dict, list, tuple, set))
        ):
            raise ConfigurationError("fill_null.value must be a non-null JSON scalar")
        if self.operation == "map":
            if (
                not isinstance(self.mapping, dict)
                or not self.mapping
                or not all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in self.mapping.items()
                )
            ):
                raise ConfigurationError(
                    "map.mapping must be a non-empty string-to-string object"
                )
            if self.on_unmapped not in {"keep", "null"}:
                raise ConfigurationError(
                    "map.on_unmapped supports only 'keep' or 'null'"
                )


@dataclass(init=False)
class MaskingRule(CompatModel):
    """Typed, irreversible column masking rule."""

    forbid_unknown_fields: ClassVar[bool] = True

    method: str
    columns: List[str] = field(default_factory=list)
    value: Any = None
    keep_start: int = 0
    keep_end: int = 0
    mask_char: str = "*"
    bucket_size: Optional[float] = None
    unit: Optional[str] = None

    def __post_init__(self) -> None:
        self.method = str(self.method).strip().lower()
        self.columns = ensure_list(self.columns)
        self.unit = (
            self.unit.strip().lower() if isinstance(self.unit, str) else self.unit
        )
        supported = {"redact", "nullify", "partial", "numeric_bucket", "date_truncate"}
        if self.method not in supported:
            raise ConfigurationError(
                f"Unsupported masking method: {self.method!r}",
                details={"supported": sorted(supported)},
            )
        _validate_column_list(self.columns, "masking_rules.columns")
        if self.method == "redact" and (
            self.value is None or isinstance(self.value, (dict, list, tuple, set))
        ):
            raise ConfigurationError("redact.value must be a non-null JSON scalar")
        if self.method == "partial":
            if (
                not isinstance(self.keep_start, int)
                or isinstance(self.keep_start, bool)
                or not isinstance(self.keep_end, int)
                or isinstance(self.keep_end, bool)
                or self.keep_start < 0
                or self.keep_end < 0
            ):
                raise ConfigurationError(
                    "partial keep_start and keep_end must be non-negative"
                )
            if not isinstance(self.mask_char, str) or len(self.mask_char) != 1:
                raise ConfigurationError(
                    "partial.mask_char must contain exactly one character"
                )
        if self.method == "numeric_bucket" and (
            not isinstance(self.bucket_size, (int, float))
            or isinstance(self.bucket_size, bool)
            or self.bucket_size <= 0
        ):
            raise ConfigurationError(
                "numeric_bucket.bucket_size must be greater than zero"
            )
        if self.method == "date_truncate" and self.unit not in {
            "year",
            "month",
            "day",
            "hour",
        }:
            raise ConfigurationError(
                "date_truncate.unit must be year, month, day, or hour"
            )


@dataclass(init=False)
class HashColumn(CompatModel):
    """Stable hash column generated from an ordered list of scalar columns."""

    forbid_unknown_fields: ClassVar[bool] = True

    target_column: str
    columns: List[str] = field(default_factory=list)
    algorithm: str = "sha256"
    serialization: str = "dc_hash_v1"

    def __post_init__(self) -> None:
        if not isinstance(self.target_column, str) or not self.target_column.strip():
            raise ConfigurationError(
                "hash_columns.target_column must be a non-empty string"
            )
        self.columns = ensure_list(self.columns)
        _validate_column_list(self.columns, "hash_columns.columns")
        self.algorithm = str(self.algorithm).strip().lower()
        if self.algorithm not in {"sha256", "xxhash64"}:
            raise ConfigurationError(
                "hash_columns.algorithm currently supports only 'sha256' and 'xxhash64'"
            )
        self.serialization = str(self.serialization).strip().lower()
        if self.serialization != "dc_hash_v1":
            raise ConfigurationError(
                "hash_columns.serialization currently supports only 'dc_hash_v1'"
            )


def _validate_column_list(columns: List[str], field_name: str) -> None:
    if not columns or not all(
        isinstance(column, str) and column.strip() for column in columns
    ):
        raise ConfigurationError(f"{field_name} must contain non-empty strings")
    lowered = [column.lower() for column in columns]
    if len(lowered) != len(set(lowered)):
        raise ConfigurationError(f"{field_name} must not contain duplicate columns")


def _coerce_model_list(
    value: Any,
    model_type: type[_ModelT],
    field_path: str,
) -> List[_ModelT]:
    """Coerce a typed metadata collection with indexed error context."""
    if value is None or (isinstance(value, (list, tuple)) and not value):
        return []
    if isinstance(value, (Mapping, model_type)):
        items = [value]
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        raise ConfigurationError(
            f"{field_path} must be a list of objects",
            details={"field": field_path, "value_type": type(value).__name__},
        )

    result: List[_ModelT] = []
    for index, item in enumerate(items):
        item_path = f"{field_path}[{index}]"
        if isinstance(item, model_type):
            result.append(item)
            continue
        if not isinstance(item, Mapping):
            raise ConfigurationError(
                f"{item_path} must be an object",
                details={"field": item_path, "value_type": type(item).__name__},
            )
        try:
            result.append(model_type(**dict(item)))
        except ConfigurationError as exc:
            raise ConfigurationError(
                exc.message,
                details={**exc.details, "field": item_path},
            ) from exc
    return result


@dataclass(init=False)
class Transform(CompatModel):
    """Transformation rules applied between source read and destination write."""

    field_path_prefix: ClassVar[str] = "transform"

    deduplicate_columns: List[str] = field(default_factory=list)
    latest_data_columns: List[str] = field(default_factory=list)
    filter_expression: Optional[str] = None
    additional_columns: List[AdditionalColumn] = field(default_factory=list)
    schema_hints: List[SchemaHint] = field(default_factory=list)
    select_columns: List[str] = field(default_factory=list)
    drop_columns: List[str] = field(default_factory=list)
    rename_columns: Dict[str, str] = field(default_factory=dict)
    value_rules: List[ValueRule] = field(default_factory=list)
    hash_columns: List[HashColumn] = field(default_factory=list)
    masking_rules: List[MaskingRule] = field(default_factory=list)
    configure: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _coerce_list(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_dedup(cls, v: Any) -> List[str]:
        return ensure_list(v)

    @classmethod
    def _coerce_additional(cls, v: Any) -> List[AdditionalColumn]:
        return _coerce_model_list(v, AdditionalColumn, "transform.additional_columns")

    @classmethod
    def _coerce_hints(cls, v: Any) -> List[SchemaHint]:
        return _coerce_model_list(v, SchemaHint, "transform.schema_hints")

    @classmethod
    def _coerce_value_rules(cls, v: Any) -> List[ValueRule]:
        return _coerce_model_list(v, ValueRule, "transform.value_rules")

    @classmethod
    def _coerce_masking_rules(cls, v: Any) -> List[MaskingRule]:
        return _coerce_model_list(v, MaskingRule, "transform.masking_rules")

    @classmethod
    def _coerce_hash_columns(cls, v: Any) -> List[HashColumn]:
        return _coerce_model_list(v, HashColumn, "transform.hash_columns")

    @classmethod
    def _parse_configure(cls, v: Any) -> Dict[str, Any]:
        return _parse_json_object(v)

    def __post_init__(self) -> None:
        self.latest_data_columns = self._coerce_list(self.latest_data_columns)
        self.deduplicate_columns = self._coerce_dedup(self.deduplicate_columns)
        self.additional_columns = self._coerce_additional(self.additional_columns)
        self.schema_hints = self._coerce_hints(self.schema_hints)
        self.select_columns = self._coerce_list(self.select_columns)
        self.drop_columns = self._coerce_list(self.drop_columns)
        self.value_rules = self._coerce_value_rules(self.value_rules)
        self.hash_columns = self._coerce_hash_columns(self.hash_columns)
        self.masking_rules = self._coerce_masking_rules(self.masking_rules)
        self.configure = self._parse_configure(self.configure)
        _ = self.missing_column_policy
        _ = self.timestamp_timezone
        self._validate_projection()
        self._validate_hash_targets()
        self._validate_masking_targets()
        self._validate_schema_hints()

    def _validate_schema_hints(self) -> None:
        """Reject duplicate local hints before they become a mapping.

        A transform is the runtime model boundary for locally authored hints.
        Treating duplicate columns as invalid here prevents the convenience
        ``schema_hints_dict`` view from silently selecting the last entry.
        Provider-attached hints are validated by the metadata provider's
        grouped-hint contract before they reach this model.
        """

        seen: set[str] = set()
        duplicates: set[str] = set()
        for hint in self.schema_hints:
            key = hint.column_name.casefold()
            if key in seen:
                duplicates.add(hint.column_name)
            seen.add(key)
        if duplicates:
            raise ConfigurationError(
                "schema_hints must not contain duplicate columns",
                details={"columns": sorted(duplicates)},
            )

    def _validate_projection(self) -> None:
        if self.select_columns and self.drop_columns:
            raise ConfigurationError(
                "select_columns and drop_columns are mutually exclusive"
            )
        if self.select_columns:
            _validate_column_list(self.select_columns, "select_columns")
        if self.drop_columns:
            _validate_column_list(self.drop_columns, "drop_columns")
        if not isinstance(self.rename_columns, dict) or not all(
            isinstance(source, str)
            and source.strip()
            and isinstance(target, str)
            and target.strip()
            for source, target in self.rename_columns.items()
        ):
            raise ConfigurationError("rename_columns must be a string-to-string object")
        sources = {source.lower() for source in self.rename_columns}
        targets = [target.lower() for target in self.rename_columns.values()]
        if len(targets) != len(set(targets)):
            raise ConfigurationError(
                "rename_columns must not contain duplicate targets"
            )
        if sources.intersection(targets):
            raise ConfigurationError(
                "rename_columns chains, cycles, and no-op renames are not supported"
            )

    def _validate_masking_targets(self) -> None:
        seen: set[str] = set()
        for rule in self.masking_rules:
            overlap = seen.intersection(column.lower() for column in rule.columns)
            if overlap:
                raise ConfigurationError(
                    "A column may appear in only one masking rule",
                    details={"columns": sorted(overlap)},
                )
            seen.update(column.lower() for column in rule.columns)

    def _validate_hash_targets(self) -> None:
        targets = [definition.target_column.lower() for definition in self.hash_columns]
        if len(targets) != len(set(targets)):
            raise ConfigurationError(
                "hash_columns must not contain duplicate target columns"
            )

    @property
    def missing_column_policy(self) -> str:
        policy = (
            str(self.configure.get("missing_column_policy", "error")).strip().lower()
        )
        if policy not in {"error", "ignore"}:
            raise ConfigurationError(
                "missing_column_policy must be 'error' or 'ignore'"
            )
        return policy

    def deduplicate_column_names(
        self, merge_keys: List[str] | None = None
    ) -> List[str]:
        """Return dedup columns, falling back to *merge_keys*."""
        if self.deduplicate_columns:
            return self.deduplicate_columns
        return merge_keys or []

    @property
    def convert_timestamp_ntz(self) -> bool:
        """Whether to convert ``timestamp_ntz`` columns to ``timestamp``.

        Reads ``convert_timestamp_ntz`` from :attr:`configure`.
        Defaults to ``False`` so a naive wall-clock value is not silently
        interpreted as an instant.

        Example (YAML / metadata)::

            transform:
              configure:
                convert_timestamp_ntz: false
        """
        return convert_to_bool(self.configure.get("convert_timestamp_ntz", False))

    @property
    def timestamp_timezone(self) -> Optional[str]:
        """Timezone used when an explicit NTZ-to-instant conversion is requested.

        The value is passed to the engine adapter, which validates whether it
        can implement the conversion without mutating shared session state.
        """

        value = self.configure.get("timestamp_timezone")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ConfigurationError(
                "timestamp_timezone must be a non-empty string or null"
            )
        return value.strip()

    @property
    def deduplicate_by_rank(self) -> bool:
        """Whether to use RANK-based deduplication instead of ROW_NUMBER.

        Reads ``deduplicate_by_rank`` from :attr:`configure`.
        Defaults to ``False``.

        Example (YAML / metadata)::

            transform:
              configure:
                deduplicate_by_rank: true
        """
        return convert_to_bool(self.configure.get("deduplicate_by_rank", False))

    @property
    def schema_hints_dict(self) -> Dict[str, SchemaHint]:
        self._validate_schema_hints()
        return {h.column_name: h for h in self.schema_hints}
