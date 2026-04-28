"""Path manipulation utilities for the DataCoolie framework."""

from __future__ import annotations


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

    return normalised.rstrip("/")


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
