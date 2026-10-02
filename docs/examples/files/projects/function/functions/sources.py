"""A small function source used by the public function-project example."""

from __future__ import annotations

from typing import Any

import polars as pl


def load_orders(*, engine: Any, source: Any, watermark_start: Any, watermark_end: Any):
    """Return a tiny engine-compatible fixture without external file I/O.

    Keeping the fixture in the function makes the generated ZIP self-contained:
    the project build packages configured function roots, not arbitrary project
    data directories.
    """
    del engine, source, watermark_start, watermark_end
    return pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "amount": [19.99, 29.00, 5.50],
            "category": ["hardware", "software", "hardware"],
        }
    ).lazy()
