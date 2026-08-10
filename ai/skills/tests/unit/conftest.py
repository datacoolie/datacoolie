"""Shared import paths for DataCoolie AI skill unit tests."""
import sys
from pathlib import Path

# Build owns all deterministic workspace and metadata scripts.
BUILD_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-build" / "scripts"
if str(BUILD_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(BUILD_SCRIPTS))

# Allow imports from datacoolie-discover scripts
DISCOVER_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-discover" / "scripts"
if str(DISCOVER_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DISCOVER_SCRIPTS))

# Allow imports from datacoolie-design scripts
DESIGN_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-design" / "scripts"
if str(DESIGN_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DESIGN_SCRIPTS))

# Allow imports from the deterministic provision evidence validator.
PROVISION_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-provision" / "scripts"
if str(PROVISION_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(PROVISION_SCRIPTS))

# Allow imports from deterministic release evidence validation.
RELEASE_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-release" / "scripts"
if str(RELEASE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(RELEASE_SCRIPTS))
