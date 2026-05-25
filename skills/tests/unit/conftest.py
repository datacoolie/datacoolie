"""conftest.py — Add skill scripts directories to sys.path for unit tests."""
import sys
from pathlib import Path

# Allow `import _api_introspect`, `import _auth`, etc. without installing
SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-discover" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

# Allow `import provision`, `import providers.*`, `import terraform.*`
PROVISION_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-provision" / "scripts"
if str(PROVISION_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(PROVISION_SCRIPTS))

# Allow `import preflight`, `import promote`, `import apply`, etc.
DEPLOY_SCRIPTS = Path(__file__).parent.parent.parent / "datacoolie-deploy" / "scripts"
if str(DEPLOY_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DEPLOY_SCRIPTS))
