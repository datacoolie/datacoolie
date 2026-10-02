"""Pure component-root normalization and prefix-qualified resolution."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.utils.path_utils import ensure_relative_path, join_path, normalize_path


class ComponentPathError(ConfigurationError):
    """Raised when a component root or prefix is ambiguous or unsafe."""


@dataclass(frozen=True, slots=True)
class ComponentPath:
    """One effective component root and its query/import prefix."""

    base_path: str
    prefix: str


def _leaf(path: str) -> str:
    value = normalize_path(path).rstrip("/")
    leaf = value.rsplit("/", 1)[-1]
    if not leaf or leaf in {".", ".."}:
        raise ComponentPathError(
            f"component root has no usable folder prefix: {path!r}"
        )
    return leaf


def _expand_root(
    value: str,
    *,
    artifact_base_path: str | None,
    allow_deferred_artifact: bool = False,
) -> str:
    raw = value.strip()
    if raw.startswith("artifact:"):
        target = raw[len("artifact:") :]
        if not target.startswith("/") or target.startswith("//"):
            raise ComponentPathError(
                f"artifact component root must use artifact:/<relative-path>: {value!r}"
            )
        if artifact_base_path is None:
            if not allow_deferred_artifact:
                raise ComponentPathError(
                    f"component root {value!r} requires artifact_base_path"
                )
            try:
                ensure_relative_path(target[1:])
            except ValueError as exc:
                raise ComponentPathError(
                    f"invalid artifact component root: {value!r}"
                ) from exc
            return normalize_path(raw)
        try:
            relative = ensure_relative_path(target[1:])
        except ValueError as exc:
            raise ComponentPathError(
                f"invalid artifact component root: {value!r}"
            ) from exc
        return join_path(artifact_base_path, relative)
    return normalize_path(raw)


def normalize_component_paths(
    values: str | Sequence[str] | None,
    *,
    name: str,
    artifact_base_path: str | None = None,
    allow_empty: bool = False,
    allow_deferred_artifact: bool = False,
) -> tuple[ComponentPath, ...] | None:
    """Normalize a public string/list root input without storage I/O.

    ``None`` means no value was supplied and is preserved for caller fallback.
    An empty sequence is explicit absence when ``allow_empty`` is true.
    """

    if values is None:
        return None
    if isinstance(values, str):
        raw_values: list[str] = [values]
    elif isinstance(values, Sequence):
        raw_values = list(values)
    else:
        raise ComponentPathError(
            f"{name} must be a path string or a sequence of path strings"
        )
    if not raw_values and not allow_empty:
        raise ComponentPathError(f"{name} must contain at least one path")
    result: list[ComponentPath] = []
    seen: dict[str, str] = {}
    for index, value in enumerate(raw_values):
        if not isinstance(value, str) or not value.strip():
            raise ComponentPathError(f"{name}[{index}] must be a non-empty path")
        base = _expand_root(
            value,
            artifact_base_path=artifact_base_path,
            allow_deferred_artifact=allow_deferred_artifact,
        )
        if not base:
            raise ComponentPathError(f"{name}[{index}] must be a non-empty path")
        prefix = _leaf(base)
        folded = prefix.casefold()
        if folded in seen:
            raise ComponentPathError(
                f"{name} roots have duplicate folder prefix {prefix!r}: "
                f"{seen[folded]!r} and {base!r}"
            )
        seen[folded] = base
        result.append(ComponentPath(base, prefix))
    return tuple(result)


def select_prefixed_root(
    roots: Sequence[ComponentPath],
    resource_path: str,
    *,
    name: str,
    allow_unprefixed_single: bool = False,
) -> tuple[ComponentPath, str]:
    """Select one root by prefix, with optional single-root convenience."""

    try:
        relative = ensure_relative_path(resource_path)
    except ValueError as exc:
        raise ComponentPathError(
            f"invalid {name} resource path: {resource_path!r}"
        ) from exc
    parts = relative.split("/")
    prefix = parts[0]
    matches = [root for root in roots if root.prefix == prefix]
    if not matches:
        if allow_unprefixed_single and len(roots) == 1:
            return roots[0], relative
        expected = ", ".join(root.prefix for root in roots) or "<none>"
        raise ComponentPathError(
            f"{name} reference {resource_path!r} must start with one of [{expected}]"
        )
    if len(matches) > 1:  # defensive; normalization already rejects this
        raise ComponentPathError(f"ambiguous {name} prefix {prefix!r}")
    if len(parts) == 1:
        raise ComponentPathError(
            f"{name} reference {resource_path!r} must include a file below {prefix!r}"
        )
    return matches[0], "/".join(parts[1:])


__all__ = [
    "ComponentPath",
    "ComponentPathError",
    "normalize_component_paths",
    "select_prefixed_root",
]
