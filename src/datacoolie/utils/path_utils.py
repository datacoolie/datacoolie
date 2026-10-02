"""Path manipulation utilities for the DataCoolie framework.

The framework passes paths to platform adapters rather than touching a local
filesystem directly.  ``build_path`` is retained for its historical,
permissive behaviour; the helpers below are deliberately stricter and are
used for runtime roots and artifact references.
"""

from __future__ import annotations

import re


_DRIVE_ROOT_RE = re.compile(r"^[A-Za-z]:/$")
_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:/")


def _split_root(path: str) -> tuple[str, list[str]]:
    """Return a path root and slash-separated segments.

    The root includes a URI scheme and authority (``s3://bucket``), a drive
    root (``C:/``), or the POSIX root (``/``).  Relative paths use an empty
    root.  This helper does not resolve ``.``/``..``; callers that need a
    containment decision must validate those segments explicitly.
    """

    normalised = path.replace("\\", "/")
    if "://" in normalised:
        marker = normalised.find("://")
        scheme = normalised[:marker]
        remainder = normalised[marker + 3 :]
        authority, separator, tail = remainder.partition("/")
        if not authority:
            # Preserve the third slash for URI forms such as ``file:///tmp``
            # where the authority is intentionally empty.
            root = f"{scheme}://" + ("/" if separator else "")
            return root, [part for part in tail.split("/") if part and part != "."]
        return (
            f"{scheme}://{authority}",
            [part for part in tail.split("/") if part and part != "."] if separator else [],
        )
    if _DRIVE_PATH_RE.match(normalised):
        drive = normalised[:3]
        return drive, [part for part in normalised[3:].split("/") if part and part != "."]
    if normalised.startswith("/"):
        return "/", [part for part in normalised.split("/") if part and part != "."]
    # Single-slash schemes such as dbfs:/ and file:/ retain their scheme root.
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*:/)(.*)$", normalised)
    if scheme_match:
        return scheme_match.group(1), [
            part for part in scheme_match.group(2).split("/") if part and part != "."
        ]
    return "", [part for part in normalised.split("/") if part and part != "."]


def _join_root(root: str, segments: list[str]) -> str:
    """Join a root and segments without dropping URI/drive roots."""

    if root.endswith(":///"):
        return root + "/".join(segments)
    if root.endswith("://"):
        return root + "/".join(segments)
    if root.endswith(":/"):
        return root + "/".join(segments)
    if root == "/":
        return "/" + "/".join(segments)
    if root:
        return root + ("/" + "/".join(segments) if segments else "")
    return "/".join(segments)


def join_path(base: str, relative: str) -> str:
    """Join a configured base with one relative resource path.

    Unlike :func:`build_path`, this function rejects absolute/URI-like second
    operands so a caller cannot accidentally escape the selected root.
    ``..`` validation belongs to :func:`ensure_relative_path`.
    """

    base_value = normalize_path(base)
    if not isinstance(relative, str):
        raise ValueError("relative path must be a string")
    # Validate before any trimming so an absolute second operand cannot be
    # converted into a seemingly relative path by stripping its root slash.
    relative_value = ensure_relative_path(relative)
    if not base_value:
        raise ValueError("base path must be non-empty")
    if not relative_value:
        raise ValueError("relative path must be non-empty")
    root, segments = _split_root(base_value)
    if not root:
        segments = [segment for segment in segments if segment != "."]
    return normalize_path(_join_root(root, segments + relative_value.split("/")))


def parent_path(path: str) -> str:
    """Return the parent of a local or platform path while preserving roots.

    A relative single segment has ``.`` as its parent.  URI authorities and
    drive/POSIX roots are never treated as ordinary path segments.
    """

    value = normalize_path(path)
    if not value:
        return "."
    root, segments = _split_root(value)
    if segments:
        return _join_root(root, segments[:-1]) or "."
    return root or "."


def ensure_relative_path(path: str) -> str:
    """Validate and normalize a relative platform resource path.

    The returned value uses forward slashes.  Traversal, control characters,
    query/fragment suffixes, and encoded separators are rejected before any
    platform I/O occurs.
    """

    if not isinstance(path, str):
        raise ValueError("resource path must be a string")
    value = path.replace("\\", "/")
    if not value or value.strip() != value:
        raise ValueError("resource path must be non-empty and have no surrounding whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("resource path contains a control character")
    lowered = value.lower()
    if (
        "?" in value
        or "#" in value
        or "%2f" in lowered
        or "%5c" in lowered
        or "%2e" in lowered
    ):
        raise ValueError("resource path contains unsupported query, fragment, or encoded separator")
    if value.startswith(("/", "~")) or _DRIVE_PATH_RE.match(value) or "://" in value:
        raise ValueError(f"resource path must be relative: {path!r}")
    segments = value.split("/")
    if any(segment == ".." for segment in segments):
        raise ValueError("resource path may not contain '..'")
    # Empty segments and explicit current-directory segments are harmless;
    # collapse them so the returned path has one canonical spelling.
    segments = [segment for segment in segments if segment not in ("", ".")]
    if not segments:
        raise ValueError("resource path must contain a file name")
    return "/".join(segments)


def is_path_within(base: str, candidate: str) -> bool:
    """Return whether *candidate* is lexically below *base*.

    Roots (including URI authorities) must match exactly.  This is a lexical
    guard only; local adapters add canonical/symlink checks before reading.
    """

    base_root, base_segments = _split_root(normalize_path(base))
    candidate_root, candidate_segments = _split_root(normalize_path(candidate))
    if base_root != candidate_root or len(candidate_segments) < len(base_segments):
        return False
    return candidate_segments[: len(base_segments)] == base_segments

def normalize_path(path: str | None) -> str:
    """Normalise a storage path by removing trailing slashes and double slashes.

    Forward slashes are used as the canonical separator.

    Args:
        path: Raw path string (or ``None``).

    Returns:
        Normalised path.
    """
    if not path:
        return ""

    # Replace backslash with forward slash
    normalised = path.replace("\\", "/")

    # Collapse multiple consecutive slashes (preserve protocol prefix like abfss://)
    if "://" in normalised:
        proto_end = normalised.index("://") + 3
        prefix = normalised[:proto_end]
        rest = normalised[proto_end:]
        while "//" in rest:
            rest = rest.replace("//", "/")
        normalised = prefix + rest
    else:
        while "//" in normalised:
            normalised = normalised.replace("//", "/")

    if (
        normalised == "/"
        or _DRIVE_ROOT_RE.match(normalised)
        or normalised.endswith("://")
        or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:///$", normalised)
        or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:/$", normalised)
    ):
        return normalised
    return normalised.rstrip("/")


def normalize_optional_base_path(value: str | None, *, name: str) -> str | None:
    """Normalize an optional configured root and reject supplied blanks."""

    if value is None:
        return None
    raw = str(value)
    if not raw.strip():
        raise ValueError(f"{name} must be a non-empty path when supplied")
    normalized = normalize_path(raw.strip())
    if not normalized:
        raise ValueError(f"{name} must be a non-empty path when supplied")
    return normalized


def build_path(*parts: str | None) -> str:
    """Join non-``None`` path segments with ``/``.

    Each segment is stripped of leading/trailing slashes before joining.
    The result is normalised via :func:`normalize_path`.

    Args:
        *parts: Path segments (``None`` entries are skipped).

    Returns:
        Joined, normalised path.
    """
    segments: list[str] = []
    for part in parts:
        if not part:
            continue
        # Only strip the trailing slash on the first segment so that absolute
        # paths (e.g. ``/Volumes/…``) keep their leading slash.
        stripped = part.rstrip("/") if not segments else part.strip("/")
        if stripped and stripped.strip():
            segments.append(stripped)

    return normalize_path("/".join(segments))
