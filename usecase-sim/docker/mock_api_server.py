"""Mock API server for DataCoolie API source use case simulation.

Serves 17 endpoints that cover all real-world API integration patterns
supported by APIReader.  Run this alongside the ETL runner to test all
``local_api_source`` stage dataflows.

Usage:
    python usecase-sim/docker/mock_api_server.py [--port 8082]

Auth environment variables (defaults shown; used by secure/* endpoints):
    MOCK_BEARER_TOKEN   test-bearer-token-123
    MOCK_API_KEY        test-api-key-456
    MOCK_BASIC_USER     datacoolie
    MOCK_BASIC_PASS     password123

Endpoint catalogue:
    0.  POST /oauth/token               OAuth2 client credentials token endpoint
    1.  GET  /api/orders/simple         No pagination, root-level list
    2.  GET  /api/orders/offset         Offset/limit pagination
    3.  GET  /api/orders/cursor         Cursor pagination
    4.  GET  /api/orders/next-link      Next-URL pagination
    5.  GET  /api/orders/nested         Deeply nested data_path
    6.  POST /api/orders/search         POST with JSON body filter
    7.  GET  /api/orders/with-params    Extra static query params
    8.  GET  /api/orders/incremental    Server-side date filter + watermark
                                         Accepts modified_since in any format:
                                         ISO-8601, Unix seconds (timestamp),
                                         Unix ms (timestamp_ms), datetime string
    9.  GET  /api/secure/bearer         Bearer token auth
    10. GET  /api/secure/apikey         API key in header auth
    11. GET  /api/secure/basic          HTTP Basic auth
    12. GET  /api/orders/rate-limited   429 on first call then 200 (tests retry)
    13. GET  /api/secure/oauth2         OAuth2 bearer (token auto-fetched at runtime)
    14. GET  /api/orders/ranged         Range-split: modified_since + modified_until filter
    15. GET  /api/orders/ranged-inclusive  Range-split with inclusive upper bound (tests watermark_range_to_exclusive_offset)
    16. GET  /api/secure/bearer-ranged     Bearer auth + date range filter (auth + range-split)
    17. POST /api/orders/wm-body           POST body watermark push-down (watermark_param_location="body")
"""

from __future__ import annotations

import argparse
import base64
import threading
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

try:
    from flask import Flask, jsonify, request
except ImportError:
    raise SystemExit("Flask is required: pip install flask")


# ---------------------------------------------------------------------------
# Embedded data (30 rows — same dataset as scripts/generate_data.py)
# ---------------------------------------------------------------------------

ROWS: List[Dict[str, Any]] = [
    {"order_id": 1001, "order_date": "2024-01-15", "amount": "199.99", "quantity": 2,  "region": "US-East", "modified_at": "2024-01-15T10:00:00", "customer_name": "Alice Johnson",       "status": "completed"},
    {"order_id": 1002, "order_date": "2024-01-15", "amount": "49.50",  "quantity": 1,  "region": "US-West", "modified_at": "2024-01-15T10:05:00", "customer_name": "Bob Smith",           "status": "completed"},
    {"order_id": 1003, "order_date": "2024-01-16", "amount": "325.00", "quantity": 5,  "region": "EU-West", "modified_at": "2024-01-16T08:30:00", "customer_name": "Clara Müller",        "status": "shipped"},
    {"order_id": 1004, "order_date": "2024-01-16", "amount": "15.75",  "quantity": 3,  "region": "US-East", "modified_at": "2024-01-16T09:00:00", "customer_name": "David Lee",           "status": "pending"},
    {"order_id": 1005, "order_date": "2024-01-17", "amount": "89.00",  "quantity": 1,  "region": "APAC",    "modified_at": "2024-01-17T12:00:00", "customer_name": "Emily Chen",          "status": "completed"},
    {"order_id": 1006, "order_date": "2024-01-17", "amount": "210.50", "quantity": 4,  "region": "US-West", "modified_at": "2024-01-17T12:30:00", "customer_name": "Frank Garcia",        "status": "cancelled"},
    {"order_id": 1007, "order_date": "2024-01-18", "amount": "750.00", "quantity": 10, "region": "EU-West", "modified_at": "2024-01-18T07:00:00", "customer_name": "Greta Berg",          "status": "completed"},
    {"order_id": 1008, "order_date": "2024-01-18", "amount": "34.99",  "quantity": 2,  "region": "US-East", "modified_at": "2024-01-18T08:15:00", "customer_name": "Henry Park",          "status": "shipped"},
    {"order_id": 1009, "order_date": "2024-01-19", "amount": "120.00", "quantity": 3,  "region": "APAC",    "modified_at": "2024-01-19T14:00:00", "customer_name": "Isha Patel",          "status": "completed"},
    {"order_id": 1010, "order_date": "2024-01-19", "amount": "55.25",  "quantity": 1,  "region": "US-West", "modified_at": "2024-01-19T14:30:00", "customer_name": "James Brown",         "status": "pending"},
    {"order_id": 1011, "order_date": "2024-01-20", "amount": "430.00", "quantity": 6,  "region": "EU-West", "modified_at": "2024-01-20T09:00:00", "customer_name": "Katrin Johansson",    "status": "completed"},
    {"order_id": 1012, "order_date": "2024-01-20", "amount": "22.50",  "quantity": 1,  "region": "US-East", "modified_at": "2024-01-20T09:30:00", "customer_name": "Liam Wilson",         "status": "shipped"},
    {"order_id": 1013, "order_date": "2024-01-21", "amount": "675.00", "quantity": 8,  "region": "APAC",    "modified_at": "2024-01-21T11:00:00", "customer_name": "Mei Tanaka",          "status": "completed"},
    {"order_id": 1014, "order_date": "2024-01-21", "amount": "95.00",  "quantity": 2,  "region": "US-West", "modified_at": "2024-01-21T11:30:00", "customer_name": "Noah Martinez",       "status": "completed"},
    {"order_id": 1015, "order_date": "2024-01-22", "amount": "180.00", "quantity": 3,  "region": "EU-West", "modified_at": "2024-01-22T06:45:00", "customer_name": "Olivia Fischer",      "status": "pending"},
    {"order_id": 1016, "order_date": "2024-01-22", "amount": "42.00",  "quantity": 1,  "region": "US-East", "modified_at": "2024-01-22T07:00:00", "customer_name": "Paul Kim",            "status": "completed"},
    {"order_id": 1017, "order_date": "2024-01-23", "amount": "560.00", "quantity": 7,  "region": "APAC",    "modified_at": "2024-01-23T13:00:00", "customer_name": "Qian Li",             "status": "shipped"},
    {"order_id": 1018, "order_date": "2024-01-23", "amount": "28.75",  "quantity": 1,  "region": "US-West", "modified_at": "2024-01-23T13:15:00", "customer_name": "Rosa Alvarez",        "status": "completed"},
    {"order_id": 1019, "order_date": "2024-01-24", "amount": "305.00", "quantity": 4,  "region": "EU-West", "modified_at": "2024-01-24T08:00:00", "customer_name": "Stefan Novak",        "status": "completed"},
    {"order_id": 1020, "order_date": "2024-01-24", "amount": "67.50",  "quantity": 2,  "region": "US-East", "modified_at": "2024-01-24T08:20:00", "customer_name": "Tara Singh",          "status": "pending"},
    {"order_id": 1021, "order_date": "2024-01-25", "amount": "410.00", "quantity": 5,  "region": "APAC",    "modified_at": "2024-01-25T15:00:00", "customer_name": "Umar Hassan",         "status": "completed"},
    {"order_id": 1022, "order_date": "2024-01-25", "amount": "19.99",  "quantity": 1,  "region": "US-West", "modified_at": "2024-01-25T15:10:00", "customer_name": "Violet Chang",        "status": "cancelled"},
    {"order_id": 1023, "order_date": "2024-01-26", "amount": "880.00", "quantity": 12, "region": "EU-West", "modified_at": "2024-01-26T07:30:00", "customer_name": "Wolfgang Braun",      "status": "completed"},
    {"order_id": 1024, "order_date": "2024-01-26", "amount": "130.00", "quantity": 3,  "region": "US-East", "modified_at": "2024-01-26T07:45:00", "customer_name": "Xena Petrov",         "status": "shipped"},
    {"order_id": 1025, "order_date": "2024-01-27", "amount": "245.50", "quantity": 4,  "region": "APAC",    "modified_at": "2024-01-27T10:00:00", "customer_name": "Yuto Nakamura",       "status": "completed"},
    # Duplicate order_ids (tests deduplication / latest_data logic)
    {"order_id": 1001, "order_date": "2024-01-15", "amount": "199.99", "quantity": 2,  "region": "US-East", "modified_at": "2024-01-28T10:00:00", "customer_name": "Alice Johnson",       "status": "completed"},
    {"order_id": 1003, "order_date": "2024-01-16", "amount": "340.00", "quantity": 5,  "region": "EU-West", "modified_at": "2024-01-28T08:30:00", "customer_name": "Clara Müller",        "status": "shipped"},
    {"order_id": 1010, "order_date": "2024-01-19", "amount": "55.25",  "quantity": 1,  "region": "US-West", "modified_at": "2024-01-28T14:30:00", "customer_name": "James Brown",         "status": "completed"},
    # Additional rows
    {"order_id": 1026, "order_date": "2024-02-01", "amount": "72.00",  "quantity": 2,  "region": "US-East", "modified_at": "2024-02-01T09:00:00", "customer_name": "Zara Okafor",         "status": "pending"},
    {"order_id": 1027, "order_date": "2024-02-01", "amount": "510.00", "quantity": 6,  "region": "EU-West", "modified_at": "2024-02-01T09:30:00", "customer_name": "Aarav Sharma",        "status": "completed"},
]


# ---------------------------------------------------------------------------
# App + auth config
# ---------------------------------------------------------------------------

app = Flask(__name__)

# The simulator keeps a short-lived, in-memory trace for the active run.  The
# setup/validation helpers reset and read it through the private simulator
# endpoints below.  Only method/path/query are recorded; request headers and
# bodies are deliberately excluded so auth and signed payloads cannot leak
# into receipts.
_TRACE_LOCK = threading.Lock()
_REQUEST_TRACE: list[dict[str, Any]] = []
_TRACE_RUN_ID = "uninitialised"


@app.before_request
def _capture_simulator_request() -> None:
    if request.path.startswith("/__sim/"):
        return
    with _TRACE_LOCK:
        _REQUEST_TRACE.append(
            {
                "ordinal": len(_REQUEST_TRACE) + 1,
                "method": request.method,
                "path": request.path,
                "query": request.query_string.decode("utf-8", errors="replace"),
            }
        )


@app.route("/__sim/health")
def simulator_health():
    return jsonify({"status": "ok", "service": "mock-api"})


@app.route("/__sim/trace/reset", methods=["POST"])
def reset_simulator_trace():
    global _TRACE_RUN_ID
    run_id = request.args.get("run_id", "surface-sync")
    with _TRACE_LOCK:
        _REQUEST_TRACE.clear()
        _TRACE_RUN_ID = run_id
    return jsonify({"run_id": _TRACE_RUN_ID, "requests": 0})


@app.route("/__sim/trace")
def simulator_trace():
    with _TRACE_LOCK:
        return jsonify({"run_id": _TRACE_RUN_ID, "requests": list(_REQUEST_TRACE)})

MOCK_BEARER_TOKEN = os.environ.get("MOCK_BEARER_TOKEN", "test-bearer-token-123")
MOCK_API_KEY      = os.environ.get("MOCK_API_KEY",      "test-api-key-456")
MOCK_BASIC_USER   = os.environ.get("MOCK_BASIC_USER",   "datacoolie")
MOCK_BASIC_PASS   = os.environ.get("MOCK_BASIC_PASS",   "password123")


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

def _page_slice(rows: List[Dict], offset: int, limit: int) -> List[Dict]:
    return rows[offset : offset + limit]


def _make_cursor(offset: int) -> str:
    """Encode an offset as a base64 cursor token (opaque to the client)."""
    return base64.b64encode(str(offset).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.b64decode(cursor.encode()).decode())
    except Exception:
        return 0


def _offset_params() -> tuple[int, int]:
    offset = int(request.args.get("offset", 0))
    limit  = int(request.args.get("limit",  10))
    return offset, limit


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _check_bearer() -> Optional[tuple]:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {MOCK_BEARER_TOKEN}":
        return jsonify({"error": "Unauthorized — invalid bearer token"}), 401
    return None


def _check_apikey() -> Optional[tuple]:
    key = request.headers.get("X-API-Key", "")
    if key != MOCK_API_KEY:
        return jsonify({"error": "Unauthorized — invalid API key"}), 401
    return None


def _check_basic() -> Optional[tuple]:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return jsonify({"error": "Unauthorized — Basic auth required"}), 401
    try:
        decoded = base64.b64decode(auth[6:]).decode()
        user, password = decoded.split(":", 1)
    except Exception:
        return jsonify({"error": "Unauthorized — malformed credentials"}), 401
    if user != MOCK_BASIC_USER or password != MOCK_BASIC_PASS:
        return jsonify({"error": "Unauthorized — wrong credentials"}), 401
    return None


# ---------------------------------------------------------------------------
# Datetime parsing helper — handles all watermark_param_format variants
# ---------------------------------------------------------------------------

def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Convert *value* to a naive UTC datetime in any format APIReader may send.

    Supported formats (mirrors ``watermark_param_format`` options):

    * ``"iso"``         — ``"2024-01-15T12:00:00"``
    * ``"date"``        — ``"2024-01-15"``
    * ``"datetime"``    — ``"2024-01-15 12:00:00"``
    * ``"datetime_ms"`` — ``"2024-01-15 12:00:00.123"``
    * ``"timestamp"``   — Unix seconds as float string ``"1705276800.0"``
    * ``"timestamp_ms"``— Unix milliseconds as int string ``"1705276800000"``
    """
    if not value:
        return None
    # ISO / date / datetime / datetime_ms — fromisoformat handles all of these
    try:
        return datetime.fromisoformat(value).replace(tzinfo=None)
    except ValueError:
        pass
    # Unix timestamp: seconds when < 1e10, milliseconds when >= 1e10
    try:
        ts = float(value)
        if ts >= 1e10:  # milliseconds (> year 5138 in seconds — unambiguous)
            ts /= 1000
        return datetime.utcfromtimestamp(ts)
    except (ValueError, OverflowError, OSError):
        pass
    return None


# ---------------------------------------------------------------------------
# Rate-limit state (resets on server restart)
# ---------------------------------------------------------------------------

_rate_limit_triggered = False


# ---------------------------------------------------------------------------
# 0. OAuth2 client credentials token endpoint
#    POST /oauth/token  body: grant_type, client_id, client_secret
#    Returns a short-lived access token (valid 3600s in the response, but
#    the server does not actually validate expiry — any token from this
#    endpoint is accepted by /api/secure/oauth2).
# ---------------------------------------------------------------------------

MOCK_OAUTH2_CLIENT_ID     = os.environ.get("MOCK_OAUTH2_CLIENT_ID",     "datacoolie-client")
MOCK_OAUTH2_CLIENT_SECRET = os.environ.get("MOCK_OAUTH2_CLIENT_SECRET", "supersecret")
MOCK_OAUTH2_ACCESS_TOKEN  = "mock-oauth2-access-token-xyz"

@app.route("/oauth/token", methods=["POST"])
def oauth_token():
    grant_type    = request.form.get("grant_type", "")
    client_id     = request.form.get("client_id",  "")
    client_secret = request.form.get("client_secret", "")

    # client_secret_basic: credentials arrive in Authorization: Basic header
    if not client_id:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                client_id, _, client_secret = decoded.partition(":")
            except Exception:
                pass

    if grant_type != "client_credentials":
        return jsonify({"error": "unsupported_grant_type"}), 400
    if client_id != MOCK_OAUTH2_CLIENT_ID or client_secret != MOCK_OAUTH2_CLIENT_SECRET:
        return jsonify({"error": "invalid_client"}), 401

    return jsonify({
        "access_token": MOCK_OAUTH2_ACCESS_TOKEN,
        "token_type": "Bearer",
        "expires_in": 3600,
    })


def _check_oauth2() -> Optional[tuple]:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {MOCK_OAUTH2_ACCESS_TOKEN}":
        return jsonify({"error": "Unauthorized — invalid or expired OAuth2 token"}), 401
    return None


# ---------------------------------------------------------------------------
# 1. No pagination — root-level list
#    APIReader: data_path not set → extracts top-level list directly
# ---------------------------------------------------------------------------

@app.route("/api/orders/simple")
def orders_simple():
    return jsonify(ROWS)


# ---------------------------------------------------------------------------
# 2. Offset/limit pagination
#    APIReader: pagination_type="offset", data_path="data"
# ---------------------------------------------------------------------------

@app.route("/api/orders/offset")
def orders_offset():
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 3. Cursor pagination — opaque base64-encoded offset token
#    APIReader: pagination_type="cursor", cursor_path="next_cursor", data_path="items"
# ---------------------------------------------------------------------------

@app.route("/api/orders/cursor")
def orders_cursor():
    cursor_str = request.args.get("cursor")
    limit       = int(request.args.get("limit", 10))
    offset      = _decode_cursor(cursor_str) if cursor_str else 0
    page        = _page_slice(ROWS, offset, limit)
    next_offset = offset + limit
    next_cursor = _make_cursor(next_offset) if next_offset < len(ROWS) else None
    return jsonify({"items": page, "next_cursor": next_cursor})


# ---------------------------------------------------------------------------
# 4. Next-URL (next_link) pagination — full URL embedded in response
#    APIReader: pagination_type="next_link", next_link_path="_links.next",
#               data_path="records"
# ---------------------------------------------------------------------------

@app.route("/api/orders/next-link")
def orders_next_link():
    page_num = int(request.args.get("page",  1))
    limit    = int(request.args.get("limit", 10))
    offset   = (page_num - 1) * limit
    rows = list(ROWS)
    if request.args.get("include_late") == "1":
        rows.append(
            {
                "order_id": 1028,
                "order_date": "2024-02-02",
                "amount": "77.70",
                "quantity": 2,
                "region": "US-East",
                "modified_at": "2024-02-02T09:30:00",
                "customer_name": "Late Arrival",
                "status": "completed",
            }
        )
    modified_since = request.args.get("modified_since")
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [
            row
            for row in rows
            if datetime.fromisoformat(row["modified_at"]) >= cutoff
        ]
    page     = _page_slice(rows, offset, limit)
    has_more = (offset + limit) < len(rows)
    next_url = None
    if has_more:
        next_params = {"page": page_num + 1, "limit": limit}
        if request.args.get("include_late") == "1":
            next_params["include_late"] = "1"
        if modified_since is not None:
            next_params["modified_since"] = modified_since
        next_url = (
            f"http://{request.host}/api/orders/next-link?"
            f"{urlencode(next_params)}"
        )
    return jsonify({"records": page, "_links": {"next": next_url}})


# ---------------------------------------------------------------------------
# Amendment 5 surface-sync fixtures
# ---------------------------------------------------------------------------

_SURFACE_REQUEST_END_ROWS = [
    {"order_id": 2001, "covered_at": "2024-01-15T12:00:00+00:00"},
    {"order_id": 2002, "covered_at": "2024-01-16T00:00:00+00:00"},
    {"order_id": 2003, "covered_at": "2024-01-18T12:00:00+00:00"},
]


@app.route("/api/orders/surface-request-end")
def orders_surface_request_end():
    """Return rows for the request-end inclusive-continuation fixture."""
    page = int(request.args.get("page", "1"))
    lower_name = "covered_from" if "covered_from" in request.args else "from_time"
    upper_name = "covered_to" if "covered_to" in request.args else "to_time"
    lower_text = request.args.get(lower_name)
    upper_text = request.args.get(upper_name)
    lower = _parse_datetime(lower_text)
    upper = _parse_datetime(upper_text)
    rows = list(_SURFACE_REQUEST_END_ROWS)
    if lower is not None:
        rows = [
            row
            for row in rows
            if datetime.fromisoformat(row["covered_at"].replace("+00:00", "")) >= lower
        ]
    if upper is not None:
        # The endpoint intentionally treats the upper bound as inclusive.  The
        # source contract's request-end mapping must therefore preserve the
        # exact boundary on the next invocation.
        rows = [
            row
            for row in rows
            if datetime.fromisoformat(row["covered_at"].replace("+00:00", "")) <= upper
        ]
    if page == 1:
        page_rows = rows[:1]
        next_link = (
            f"http://{request.host}/api/orders/surface-request-end?"
            + urlencode(
                {
                    "page": 2,
                    lower_name: lower_text or "",
                    upper_name: upper_text or "",
                }
            )
        ) if len(rows) > 1 else None
    else:
        page_rows = rows[1:]
        next_link = None
    return jsonify({"data": page_rows, "paging": {"next": next_link}})


_SURFACE_NEXT_LINK_ROWS = [
    {"order_id": 2101, "event_time": "2024-01-15T10:00:00+00:00"},
    {"order_id": 2102, "event_time": "2024-01-15T11:00:00+00:00"},
]


@app.route("/api/orders/surface-next-link")
def orders_surface_next_link():
    """Serve opaque and repeat-query continuation variants for C5."""
    page = int(request.args.get("page", "1"))
    mode = request.args.get("mode", "opaque")
    rows = list(_SURFACE_NEXT_LINK_ROWS)
    if page != 1:
        return jsonify({"data": rows[1:], "paging": {"next": None}})

    # Keep the continuation token visibly opaque.  The trace records the URL
    # spelling so validators can prove that opaque mode did not add bounds.
    if mode == "opaque":
        next_query = "page=2&mode=opaque&signed=token%2Bopaque"
    elif mode == "matching":
        from_value = request.args.get("from_time", "")
        to_value = request.args.get("to_time", "")
        next_query = urlencode(
            {
                "page": 2,
                "mode": "matching",
                "from_time": from_value,
                "to_time": to_value,
                "signed": "token+matching",
            }
        )
    elif mode == "missing":
        next_query = "page=2&mode=missing&signed=token%2Bmissing"
    elif mode == "conflict":
        next_query = "page=2&mode=conflict&from_time=conflicting&signed=token%2Bconflict"
    elif mode == "duplicate":
        next_query = (
            "page=2&mode=duplicate&from_time=duplicate-a&from_time=duplicate-b"
            "&signed=token%2Bduplicate"
        )
    else:
        return jsonify({"error": f"unknown surface mode {mode}"}), 400
    next_url = f"http://{request.host}/api/orders/surface-next-link?{next_query}"
    return jsonify({"data": rows[:1], "paging": {"next": next_url}})


# ---------------------------------------------------------------------------
# 5. Deeply nested data_path
#    APIReader: pagination_type="offset",
#               data_path="response.payload.orders"
# ---------------------------------------------------------------------------

@app.route("/api/orders/nested")
def orders_nested():
    offset, limit = _offset_params()
    return jsonify({
        "response": {
            "payload": {
                "orders": _page_slice(ROWS, offset, limit)
            }
        }
    })


# ---------------------------------------------------------------------------
# 6. POST with JSON body filter — single-page, no pagination
#    APIReader: method="POST", body={"status":"completed"}, data_path="data"
#    Filters rows server-side by the "status" field in the request body.
# ---------------------------------------------------------------------------

@app.route("/api/orders/search", methods=["POST"])
def orders_search():
    body   = request.get_json(silent=True) or {}
    status = body.get("status")
    rows   = [r for r in ROWS if status is None or r["status"] == status]
    return jsonify({"data": rows})


# ---------------------------------------------------------------------------
# 7. Extra static query params — server accepts but ignores unknown params
#    APIReader: params={"api_version":"v2"} merged with offset pagination
# ---------------------------------------------------------------------------

@app.route("/api/orders/with-params")
def orders_with_params():
    # api_version is accepted and ignored
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 8. Server-side incremental filter + client watermark tracking
#    APIReader: params={"modified_since":"2024-01-25T00:00:00"},
#               watermark_columns=["modified_at"], pagination_type="offset"
#    Server filters by modified_since; APIReader tracks the high-water mark.
# ---------------------------------------------------------------------------

@app.route("/api/orders/incremental")
def orders_incremental():
    modified_since = request.args.get("modified_since")
    offset, limit  = _offset_params()
    rows = ROWS
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) >= cutoff]
    return jsonify({"data": _page_slice(rows, offset, limit), "total": len(rows)})


# ---------------------------------------------------------------------------
# 9. Bearer token auth
#    APIReader connection: auth_type="bearer", auth_token resolved from env
# ---------------------------------------------------------------------------

@app.route("/api/secure/bearer")
def secure_bearer():
    err = _check_bearer()
    if err:
        return err
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 10. API key in header
#     APIReader connection: auth_type="api_key", api_key_header="X-API-Key"
# ---------------------------------------------------------------------------

@app.route("/api/secure/apikey")
def secure_apikey():
    err = _check_apikey()
    if err:
        return err
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 11. HTTP Basic auth
#     APIReader connection: auth_type="basic", username/password from env
# ---------------------------------------------------------------------------

@app.route("/api/secure/basic")
def secure_basic():
    err = _check_basic()
    if err:
        return err
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 13. OAuth2 client credentials — token fetched at runtime by APIReader
#     Connection: auth_type="oauth2_client_credentials",
#                 token_url="http://localhost:8082/oauth/token",
#                 client_id / client_secret resolved from secrets_ref
# ---------------------------------------------------------------------------

@app.route("/api/secure/oauth2")
def secure_oauth2():
    err = _check_oauth2()
    if err:
        return err
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 12. Rate limiting — returns 429 Retry-After on the very first request,
#     then 200 for all subsequent calls.
#     Tests the API transport helper's 429 → auto-retry path.
#     Note: the flag resets on server restart; restart to re-test.
# ---------------------------------------------------------------------------

@app.route("/api/orders/rate-limited")
def orders_rate_limited():
    global _rate_limit_triggered
    if not _rate_limit_triggered:
        _rate_limit_triggered = True
        resp = jsonify({"error": "Rate limit exceeded. Please retry."})
        resp.headers["Retry-After"] = "0"
        return resp, 429
    offset, limit = _offset_params()
    return jsonify({"data": _page_slice(ROWS, offset, limit), "total": len(ROWS)})


# ---------------------------------------------------------------------------
# 14. Range-split: date range filter with modified_since + modified_until
#     APIReader: watermark_range_interval_unit="day",
#                watermark_param_mapping={"modified_at": "modified_since"},
#                watermark_to_param="modified_until"
#     Server returns rows where modified_since <= modified_at < modified_until.
# ---------------------------------------------------------------------------

@app.route("/api/orders/ranged")
def orders_ranged():
    modified_since = request.args.get("modified_since")
    modified_until = request.args.get("modified_until")
    rows = ROWS
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) >= cutoff]
    ceiling = _parse_datetime(modified_until)
    if ceiling is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) < ceiling]
    return jsonify({"data": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# 15. Range-split with INCLUSIVE upper bound (modified_until is <= instead of <)
#     Used to test watermark_range_to_exclusive_offset: the client subtracts an
#     epsilon from the boundary sent to the API so rows exactly at the split
#     point are not fetched twice by adjacent ranges.
#     APIReader: watermark_range_to_exclusive_offset="1s"
# ---------------------------------------------------------------------------

@app.route("/api/orders/ranged-inclusive")
def orders_ranged_inclusive():
    modified_since = request.args.get("modified_since")
    modified_until = request.args.get("modified_until")
    rows = ROWS
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) >= cutoff]
    ceiling = _parse_datetime(modified_until)
    if ceiling is not None:
        # Inclusive upper bound: <= ceiling (BETWEEN A AND B semantics)
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) <= ceiling]
    return jsonify({"data": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# 16. Bearer auth + date range filter
#     Combines token validation with modified_since / modified_until filtering.
#     Tests that auth and watermark range-split work together.
#     APIReader: connection auth_type="bearer" + watermark_range_interval_unit
# ---------------------------------------------------------------------------

@app.route("/api/secure/bearer-ranged")
def secure_bearer_ranged():
    err = _check_bearer()
    if err:
        return err
    modified_since = request.args.get("modified_since")
    modified_until = request.args.get("modified_until")
    rows = ROWS
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) >= cutoff]
    ceiling = _parse_datetime(modified_until)
    if ceiling is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) < ceiling]
    return jsonify({"data": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# 17. POST body watermark push-down
#     modified_since / modified_until are in the JSON request body (not params).
#     Tests APIReader watermark_param_location="body" with wm_to_param.
# ---------------------------------------------------------------------------

@app.route("/api/orders/wm-body", methods=["POST"])
def orders_wm_body():
    body = request.get_json(silent=True) or {}
    modified_since = body.get("modified_since")
    modified_until = body.get("modified_until")
    rows = ROWS
    cutoff = _parse_datetime(modified_since)
    if cutoff is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) >= cutoff]
    ceiling = _parse_datetime(modified_until)
    if ceiling is not None:
        rows = [r for r in rows if datetime.fromisoformat(r["modified_at"]) < ceiling]
    return jsonify({"data": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataCoolie mock API server")
    parser.add_argument("--port", type=int, default=8082, help="Listening port (default 8082)")
    args = parser.parse_args()

    print(f"Mock API server starting on http://localhost:{args.port}")
    print(f"  MOCK_BEARER_TOKEN         = {MOCK_BEARER_TOKEN}")
    print(f"  MOCK_API_KEY              = {MOCK_API_KEY}")
    print(f"  MOCK_BASIC_USER           = {MOCK_BASIC_USER}")
    print(f"  MOCK_BASIC_PASS           = {MOCK_BASIC_PASS}")
    print(f"  MOCK_OAUTH2_CLIENT_ID     = {MOCK_OAUTH2_CLIENT_ID}")
    print(f"  MOCK_OAUTH2_CLIENT_SECRET = {MOCK_OAUTH2_CLIENT_SECRET}")
    print(f"  MOCK_OAUTH2_ACCESS_TOKEN  = {MOCK_OAUTH2_ACCESS_TOKEN}")
    print()

    app.run(host="0.0.0.0", port=args.port, debug=False)
