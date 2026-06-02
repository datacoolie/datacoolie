"""conftest.py — Shared fixtures for unit tests.

Note: discover/provision/deploy scripts were removed during the knowledge-based
migration. Only datacoolie-metadata retains scripts; its path is added below.
The discover skill now has introspection scripts; its path is added too.
"""
import sys
from pathlib import Path

# Allow imports from datacoolie-metadata scripts (still script-based)
METADATA_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-metadata" / "scripts"
if str(METADATA_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(METADATA_SCRIPTS))

# Allow imports from datacoolie-discover scripts
DISCOVER_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-discover" / "scripts"
if str(DISCOVER_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DISCOVER_SCRIPTS))
