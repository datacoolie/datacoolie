"""Read-only CLI for the usecase-sim case catalog.

This wrapper intentionally imports only :mod:`cases.catalog`.  It never loads
the scenario runner, starts Docker services, runs setup scripts, or writes
runtime receipts.
"""

from __future__ import annotations

import sys
from pathlib import Path


SIMULATOR_ROOT = Path(__file__).resolve().parents[1]
if str(SIMULATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_ROOT))

from cases.catalog import _cli  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(_cli())
