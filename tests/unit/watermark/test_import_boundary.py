"""Verify watermark value imports and intentional public exports in fresh processes."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _run_cold_import(script: str) -> None:
    product_root = Path(__file__).parents[3]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(product_root / "src")
    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=product_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("module", [
    "datacoolie.watermark",
    "datacoolie.watermark.base",
    "datacoolie.watermark.contracts",
    "datacoolie.watermark.runtime",
])
def test_value_imports_do_not_load_storage_or_logging(module: str) -> None:
    _run_cold_import(f"""
import importlib
import sys
importlib.import_module({module!r})
for prefix in (
    'datacoolie.watermark.watermark_manager', 'datacoolie.metadata',
    'datacoolie.logging', 'datacoolie.orchestration', 'datacoolie.sources',
    'pyspark', 'polars',
):
    loaded = [name for name in sys.modules
              if name == prefix or name.startswith(prefix + '.')]
    assert not loaded, loaded
""")


def test_public_exports_keep_canonical_identity() -> None:
    _run_cold_import("""
import datacoolie.watermark as public
from datacoolie.watermark import WatermarkManager
from datacoolie.watermark import *
from datacoolie.watermark import base
from datacoolie.watermark.watermark_manager import WatermarkManager as concrete
assert WatermarkManager is public.WatermarkManager is concrete
for name in public.__all__:
    owner = concrete if name == 'WatermarkManager' else getattr(base, name)
    assert getattr(public, name) is owner
try:
    public.missing_export
except AttributeError:
    pass
else:
    raise AssertionError('Unknown public attributes must raise AttributeError')
""")
