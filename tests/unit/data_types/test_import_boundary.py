"""Ensure the pure datatype contract does not import optional runtimes."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_pure_resolver_import_is_optional_dependency_free() -> None:
    """Importing the shared resolver must not initialize Spark or Polars."""
    product_root = Path(__file__).parents[3]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(product_root / "src")
    script = (
        "import sys; "
        "from datacoolie.engines.data_types import resolve_schema_hint; "
        "resolve_schema_hint('int8', type_system='postgresql'); "
        "print('pyspark' in sys.modules, 'polars' in sys.modules)"
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=product_root,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "False False"
