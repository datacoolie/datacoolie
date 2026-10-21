"""Assert the two-page cap rejects the 30-row API without writes."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import (
    resolve_case_root,
    materialize_fixture,
    root_argument,
    receipt_payload,
    write_receipt,
)
from read.api_rejection_support import seed_checkpoint, run_rejection

ERROR = "API offset pagination reached max_pages=2 with a full page; the end of the result was not confirmed."


def validate(root=None, *, qualification_scope="host-polars-mock-api"):
    import httpx

    case = resolve_case_root("read", root)
    materialize_fixture(case, "api_max_pages_negative.json")
    document = case.document
    document["connections"][0]["configure"]["base_url"] = os.getenv(
        "DATACOOLIE_MOCK_API_URL", "http://localhost:8082"
    )
    flow = document["dataflows"][0]
    response = httpx.get(
        document["connections"][0]["configure"]["base_url"].rstrip("/")
        + "/api/orders/offset",
        params={"offset": 0, "limit": 1},
        timeout=5,
    )
    response.raise_for_status()
    if response.json().get("total") != 30:
        raise AssertionError("Max-pages recipe requires the exact 30-row API fixture")
    seed_checkpoint(case, flow["name"], {"sentinel": 17})
    run, detail = run_rejection(
        case, flow=flow["name"], stage=flow["stage"], error=ERROR
    )
    detail["api_total"] = 30
    payload = receipt_payload(case, runs=[run], details=detail, script=Path(__file__))
    payload["profile"].update(
        destination_format="parquet", qualification_scope=qualification_scope
    )
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    print(validate(parser.parse_args().root))
