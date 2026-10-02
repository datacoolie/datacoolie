"""Shared model foundation and compatibility behavior."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Mapping
from dataclasses import MISSING, dataclass, fields, is_dataclass
from types import UnionType
from typing import Any, ClassVar, Dict, List, Union, get_args, get_origin, get_type_hints

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.converters import json_default, parse_json


@dataclass(frozen=True)
class _CompatFieldInfo:
    """Small subset of field metadata used by tests and compatibility helpers."""

    default: Any = MISSING
    default_factory: Callable[[], Any] | None = None


class _ClassProperty:
    """Descriptor implementing a minimal read-only class property."""

    def __init__(self, func: Callable[[type], Any]) -> None:
        self._func = func

    def __get__(self, instance: object, owner: type | None = None) -> Any:
        if owner is None:
            owner = type(instance)
        return self._func(owner)


def _build_default(dc_field: Any) -> Any:
    """Return the declared default value for a dataclass field."""

    if dc_field.default_factory is not MISSING:
        return dc_field.default_factory()
    if dc_field.default is not MISSING:
        return copy.deepcopy(dc_field.default)
    raise ConfigurationError(f"Missing required field: {dc_field.name}")


def _to_field_info(dc_field: Any) -> _CompatFieldInfo:
    """Convert a dataclass field into the lightweight compatibility shape."""

    default_factory = None
    if dc_field.default_factory is not MISSING:
        default_factory = dc_field.default_factory
    return _CompatFieldInfo(
        default=None if dc_field.default is MISSING else dc_field.default,
        default_factory=default_factory,
    )


def _parse_json_object(value: Any) -> Dict[str, Any]:
    """Parse a dict-like JSON field and wrap parsing failures consistently."""

    try:
        return parse_json(value, raise_on_error=True)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


def _model_dump_value(value: Any) -> Any:
    """Recursively serialise model values to plain Python containers."""

    if isinstance(value, CompatModel):
        return value.model_dump()
    if is_dataclass(value) and not isinstance(value, type):
        return {
            dc_field.name: _model_dump_value(getattr(value, dc_field.name))
            for dc_field in fields(value)
        }
    if isinstance(value, list):
        return [_model_dump_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_model_dump_value(item) for item in value)
    if isinstance(value, dict):
        return {key: _model_dump_value(item) for key, item in value.items()}
    return value


def _coerce_annotation_value(
    annotation: Any,
    value: Any,
    field_path: str | None = None,
) -> Any:
    """Coerce nested model annotations from mappings into model instances."""

    if value is None:
        return None

    origin = get_origin(annotation)
    if origin in (list, List):
        args = get_args(annotation)
        if args and isinstance(value, list):
            inner = args[0]
            result = []
            for index, item in enumerate(value):
                item_path = f"{field_path}[{index}]" if field_path else None
                try:
                    result.append(_coerce_annotation_value(inner, item, item_path))
                except ConfigurationError as exc:
                    details = dict(exc.details)
                    if item_path:
                        details["field"] = item_path
                    raise ConfigurationError(exc.message, details=details) from exc
            return result
        return value

    if origin in (dict, Dict):
        return value

    if origin in (Union, UnionType):
        for arg in get_args(annotation):
            if arg is type(None):
                continue
            coerced = _coerce_annotation_value(arg, value, field_path)
            if coerced is not value:
                return coerced
        return value

    if (
        isinstance(annotation, type)
        and issubclass(annotation, CompatModel)
        and isinstance(value, Mapping)
    ):
        try:
            return annotation(**dict(value))
        except ConfigurationError as exc:
            details = dict(exc.details)
            if field_path:
                details["field"] = field_path
            raise ConfigurationError(exc.message, details=details) from exc

    return value


class CompatModel:
    """Small compatibility layer for the subset of BaseModel behavior we use."""

    forbid_unknown_fields: ClassVar[bool] = False
    field_path_prefix: ClassVar[str | None] = None
    model_fields_set: set[str]

    def __init__(self, **kwargs: Any) -> None:
        cls = type(self)
        dc_fields = fields(cls)
        declared_names = {dc_field.name for dc_field in dc_fields}
        unknown_fields = set(kwargs).difference(declared_names)
        if unknown_fields and cls.forbid_unknown_fields:
            raise ConfigurationError(
                f"Unknown field(s) for {cls.__name__}",
                details={"fields": sorted(unknown_fields)},
            )
        provided_fields = set(kwargs) & declared_names
        type_hints = get_type_hints(cls)

        for dc_field in dc_fields:
            if dc_field.name in kwargs:
                value = kwargs[dc_field.name]
            else:
                value = _build_default(dc_field)
            annotation = type_hints.get(dc_field.name, Any)
            prefix = cls.field_path_prefix
            field_path = f"{prefix}.{dc_field.name}" if prefix else dc_field.name
            setattr(
                self,
                dc_field.name,
                _coerce_annotation_value(annotation, value, field_path),
            )

        self.model_fields_set = provided_fields
        post_init = getattr(self, "__post_init__", None)
        if callable(post_init):
            post_init()

    @_ClassProperty
    def model_fields(cls: type["CompatModel"]) -> Dict[str, _CompatFieldInfo]:
        return {dc_field.name: _to_field_info(dc_field) for dc_field in fields(cls)}

    @classmethod
    def model_construct(cls, **values: Any) -> "CompatModel":
        obj = cls.__new__(cls)
        dc_fields = fields(cls)
        declared_names = {dc_field.name for dc_field in dc_fields}
        for dc_field in dc_fields:
            if dc_field.name in values:
                value = values[dc_field.name]
            else:
                value = _build_default(dc_field)
            setattr(obj, dc_field.name, value)
        obj.model_fields_set = set(values) & declared_names
        return obj

    def model_copy(self, *, deep: bool = False) -> "CompatModel":
        return copy.deepcopy(self) if deep else copy.copy(self)

    def model_dump(self) -> Dict[str, Any]:
        return {
            dc_field.name: _model_dump_value(getattr(self, dc_field.name))
            for dc_field in fields(self)
        }

    def model_dump_json(self) -> str:
        return json.dumps(self.model_dump(), default=json_default)
