"""Schema directory resolution for datacoolie-metadata skill scripts.

Single source of truth: ``ai/skills/datacoolie-metadata/schemas/``.
The schema lives exclusively in the skill — not in the Python package.

Resolution order (first match wins):
    1. ``--schemas-dir <path>`` CLI arg.
    2. ``DATACOOLIE_SCHEMAS_DIR`` environment variable.
    3. Skill-bundled schemas: ``<skill_root>/schemas/`` (primary, offline-ready).
    4. User cache populated by ``--fetch-latest``: ``~/.datacoolie/schemas/``.

``--fetch-latest`` downloads the latest schema from GitHub and writes it into
the user cache (``~/.datacoolie/schemas/``) so it persists across skill updates.
The GitHub base URL can be overridden via ``DATACOOLIE_SCHEMA_GITHUB_BASE``.

Usage::

    from _schema_resolver import find_schemas_dir, load_schema, resolve_schema_version
    from _schema_resolver import fetch_latest_from_github, USER_CACHE_DIR

    schemas_dir = find_schemas_dir()
    schema = load_schema("0.1.0", schemas_dir)
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

# scripts/ → skill_root/
_SKILL_ROOT = Path(__file__).resolve().parent.parent
# Single source of truth: schemas bundled inside the skill directory
SKILL_SCHEMAS_DIR = _SKILL_ROOT / "schemas"
# User-level cache: populated by --fetch-latest, persists across skill updates
USER_CACHE_DIR = Path.home() / ".datacoolie" / "schemas"

# ---------------------------------------------------------------------------
# GitHub URL constants (override via env var for private / mirror repos)
# ---------------------------------------------------------------------------

_DEFAULT_GITHUB_BASE = (
    "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/skills/datacoolie-metadata/schemas"
)


def _github_base() -> str:
    return os.environ.get("DATACOOLIE_SCHEMA_GITHUB_BASE", _DEFAULT_GITHUB_BASE).rstrip("/")


# ---------------------------------------------------------------------------
# GitHub fetch (writes to user cache)
# ---------------------------------------------------------------------------


def fetch_latest_from_github(
    *,
    version: str | None = None,
    timeout: int = 5,
    verbose: bool = True,
) -> bool:
    """Download the latest schema from GitHub and cache it in :data:`USER_CACHE_DIR`.

    Downloads ``compatibility.json`` first to discover the latest schema version
    (or uses *version* when supplied), then downloads
    ``{version}/metadata.schema.json``.  Both files are written to
    ``~/.datacoolie/schemas/`` so subsequent offline runs use the cached copy.

    Args:
        version:  Specific version to fetch.  ``None`` resolves from GitHub
                  ``compatibility.json``.
        timeout:  Network timeout in seconds (default ``5``).
        verbose:  Print progress/status to stderr.

    Returns:
        ``True`` when the fetch and cache-write succeeded, ``False`` on failure.
    """
    base = _github_base()

    def _get(url: str) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "datacoolie-metadata-skill/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.read()

    # -- Step 1: fetch compatibility.json --
    compat_url = f"{base}/compatibility.json"
    try:
        compat_bytes = _get(compat_url)
    except (urllib.error.URLError, OSError) as exc:
        if verbose:
            print(f"  [warn] Could not reach GitHub ({exc}). Using local schema.", file=sys.stderr)
        return False

    compat_data: dict = json.loads(compat_bytes)
    target_version = version or compat_data.get("latest_schema", "0.1.0")

    # -- Step 2: fetch schema JSON --
    schema_url = f"{base}/{target_version}/metadata.schema.json"
    try:
        schema_bytes = _get(schema_url)
    except (urllib.error.URLError, OSError) as exc:
        if verbose:
            print(
                f"  [warn] Could not fetch schema v{target_version} from GitHub ({exc})."
                " Using local schema.",
                file=sys.stderr,
            )
        return False

    # Validate JSON before writing
    try:
        json.loads(schema_bytes)
    except json.JSONDecodeError as exc:
        if verbose:
            print(f"  [warn] GitHub returned invalid JSON ({exc}). Using local schema.", file=sys.stderr)
        return False

    # -- Step 3: write to user cache --
    try:
        USER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        (USER_CACHE_DIR / "compatibility.json").write_bytes(compat_bytes)
        version_dir = USER_CACHE_DIR / target_version
        version_dir.mkdir(parents=True, exist_ok=True)
        (version_dir / "metadata.schema.json").write_bytes(schema_bytes)
    except OSError as exc:
        if verbose:
            print(f"  [warn] Could not write to user cache ({exc}).", file=sys.stderr)
        return False

    if verbose:
        print(f"✓ Schema v{target_version} fetched from GitHub and cached in {USER_CACHE_DIR}")
    return True


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def find_schemas_dir(explicit: str | None = None) -> Path:
    """Return the first valid schemas directory from the resolution chain.

    Raises:
        SystemExit(2): When no valid directory is found.
    """
    candidates: list[tuple[str, Path]] = []

    if explicit:
        candidates.append(("--schemas-dir", Path(explicit).expanduser().resolve()))

    env_val = os.environ.get("DATACOOLIE_SCHEMAS_DIR")
    if env_val:
        candidates.append(("DATACOOLIE_SCHEMAS_DIR", Path(env_val).expanduser().resolve()))

    candidates.append(("skill-bundled", SKILL_SCHEMAS_DIR))
    candidates.append(("user-cache (~/.datacoolie/schemas)", USER_CACHE_DIR))

    for _source, path in candidates:
        if path.is_dir():
            return path

    checked = "\n  ".join(f"{s}: {p}" for s, p in candidates)
    print(
        f"ERROR: Could not locate a schemas directory.\n"
        f"Checked:\n  {checked}\n\n"
        f"Fix (choose one):\n"
        f"  • pip install datacoolie          (schema ships with the package)\n"
        f"  • python validate.py --fetch-latest   (downloads to ~/.datacoolie/schemas/)\n"
        f"  • set DATACOOLIE_SCHEMAS_DIR=<path>",
        file=sys.stderr,
    )
    sys.exit(2)


def load_compatibility(schemas_dir: Path) -> dict:
    """Load ``compatibility.json`` from *schemas_dir*."""
    compat_file = schemas_dir / "compatibility.json"
    if not compat_file.exists():
        return {"latest_schema": "0.1.0", "datacoolie_to_schema": {}}
    with open(compat_file, encoding="utf-8") as f:
        return json.load(f)


def resolve_schema_version(
    metadata: dict,
    schemas_dir: Path,
    explicit_version: str | None = None,
) -> str:
    """Resolve the schema version to use.

    Priority: *explicit_version* arg → ``$schema`` field → ``compatibility.json``.
    """
    if explicit_version:
        return explicit_version

    schema_ref = metadata.get("$schema", "")
    if schema_ref:
        parts = schema_ref.rstrip("/").split("/")
        for i, part in enumerate(parts):
            if part == "schemas" and i + 1 < len(parts):
                return parts[i + 1]

    compat = load_compatibility(schemas_dir)
    return compat.get("latest_schema", "0.1.0")


def load_schema(version: str, schemas_dir: Path) -> dict:
    """Load and return the JSON Schema dict for *version* from *schemas_dir*."""
    schema_path = schemas_dir / version / "metadata.schema.json"
    if not schema_path.exists():
        available = [p.name for p in schemas_dir.iterdir() if p.is_dir()]
        print(
            f"ERROR: Schema version '{version}' not found at {schema_path}\n"
            f"Available versions: {available}\n"
            f"Tip: run with --fetch-latest to download it from GitHub.",
            file=sys.stderr,
        )
        sys.exit(2)
    with open(schema_path, encoding="utf-8") as f:
        return json.load(f)

