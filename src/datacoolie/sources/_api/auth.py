"""Authentication strategies and credential token helpers for API sources."""

from __future__ import annotations

import base64
import threading
import time
from typing import Any, Dict, Optional, Tuple

from datacoolie.core.exceptions import SourceError
from datacoolie.logging.runtime.manager import get_logger
from .transport import safe_url

logger = get_logger(__name__)

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

try:
    import botocore.auth
    import botocore.awsrequest
    import botocore.credentials
    import botocore.session as _boto_session
    _HAS_BOTOCORE = True
except ImportError:
    _HAS_BOTOCORE = False

_AUTH_BEARER = "bearer"
_AUTH_BASIC = "basic"
_AUTH_API_KEY = "api_key"
_AUTH_OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
_AUTH_AWS_SIGV4 = "aws_sigv4"

_OAUTH2_TOKEN_CACHE: Dict[Tuple[str, str], Tuple[str, float]] = {}
_OAUTH2_TOKEN_CACHE_LOCK = threading.Lock()

class _SigV4Auth(httpx.Auth if httpx else object):  # type: ignore[misc]
    """httpx Auth that signs every request with AWS Signature Version 4.

    Uses ``botocore`` so the standard credential chain (env vars,
    ``~/.aws/credentials``, EC2 instance profile, ECS task role, etc.)
    is respected automatically when ``aws_access_key_id`` is omitted.
    """

    def __init__(
        self,
        credentials: "botocore.credentials.Credentials",
        service: str,
        region: str,
    ) -> None:
        self._credentials = credentials
        self._service = service
        self._region = region

    def auth_flow(self, request: "httpx.Request"):  # type: ignore[override]
        aws_req = botocore.awsrequest.AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=dict(request.headers),
        )
        signer = botocore.auth.SigV4Auth(self._credentials, self._service, self._region)
        signer.add_auth(aws_req)
        for key, value in aws_req.headers.items():
            request.headers[key] = value
        yield request

def apply_auth(headers: Dict[str, str], conn_cfg: Dict[str, Any]) -> None:
    """Apply authentication to request headers."""
    auth_type = conn_cfg.get("auth_type", "").lower()

    if auth_type == _AUTH_BEARER:
        token = conn_cfg.get("auth_token", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"

    elif auth_type == _AUTH_BASIC:
        username = conn_cfg.get("username", "")
        password = conn_cfg.get("password", "")
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"

    elif auth_type == _AUTH_API_KEY:
        key_header = conn_cfg.get("api_key_header", "X-API-Key")
        key_value = conn_cfg.get("api_key_value", "")
        if key_value:
            headers[key_header] = key_value

    elif auth_type == _AUTH_OAUTH2_CLIENT_CREDENTIALS:
        token = fetch_oauth2_token(conn_cfg)
        if token:
            headers["Authorization"] = f"Bearer {token}"

def get_http_auth(conn_cfg: Dict[str, Any]) -> Optional[Any]:
    """Return an httpx Auth handler for auth types that need per-request signing.

    Currently only ``aws_sigv4`` returns a handler; all other auth types
    are handled via plain headers in ``_apply_auth`` and return ``None``.
    """
    auth_type = conn_cfg.get("auth_type", "").lower()
    if auth_type != _AUTH_AWS_SIGV4:
        return None

    if not _HAS_BOTOCORE:
        raise SourceError(
            "auth_type='aws_sigv4' requires botocore. Install it with: pip install botocore"
        )

    region = conn_cfg.get("aws_region", "us-east-1")
    service = conn_cfg.get("aws_service", "execute-api")

    # Build a botocore session so the standard credential chain is honoured.
    # Explicit key overrides (e.g. from secrets_ref) take priority.
    session = _boto_session.Session()
    resolver = session.get_component("credential_provider")
    creds = resolver.load()

    key_id = conn_cfg.get("aws_access_key_id")
    secret = conn_cfg.get("aws_secret_access_key")
    token = conn_cfg.get("aws_session_token")

    if key_id and secret:
        creds = botocore.credentials.Credentials(
            access_key=key_id,
            secret_key=secret,
            token=token or None,
        )
    elif creds is None:
        raise SourceError(
            "auth_type='aws_sigv4': no AWS credentials found. "
            "Set aws_access_key_id/aws_secret_access_key in connection.configure or "
            "configure the environment (AWS_ACCESS_KEY_ID, instance profile, etc.)."
        )

    return _SigV4Auth(creds, service, region)

def fetch_oauth2_token(conn_cfg: Dict[str, Any]) -> str:
    """Fetch a short-lived Bearer token using the OAuth2 client credentials flow.

    Expected connection configure keys:
        - ``token_url`` (str, required): Token endpoint URL.
        - ``client_id`` (str, required): OAuth2 client ID.
        - ``client_secret`` (str, required): OAuth2 client secret — should be
          resolved from a secret reference before this is called.
        - ``scope`` (str, optional): Space-separated scopes.
        - ``token_auth_method`` (str, optional): ``"client_secret_post"`` (default)
          includes credentials in the POST body; ``"client_secret_basic"`` sends
          them via HTTP Basic Auth (required by Okta, Ping Identity, etc.).
        - ``token_request_body_format`` (str, optional): ``"form"`` (default,
          ``application/x-www-form-urlencoded``) or ``"json"``
          (``application/json``) — required by GitHub Apps and some others.
        - ``token_request_extras`` (dict, optional): Extra POST body fields
          forwarded verbatim to the token endpoint (e.g. ``audience`` for Auth0).

    The result is cached in-process keyed by ``(token_url, client_id)``.
    Subsequent calls with the same credentials return the cached token
    without a network round-trip until 30 seconds before expiry.

    Returns the ``access_token`` string.
    """
    token_url = conn_cfg.get("token_url", "")
    client_id = conn_cfg.get("client_id", "")
    client_secret = conn_cfg.get("client_secret", "")

    if not token_url:
        raise SourceError(
            "auth_type='oauth2_client_credentials' requires 'token_url' in connection.configure",
        )
    if not client_id or not client_secret:
        raise SourceError(
            "auth_type='oauth2_client_credentials' requires 'client_id' and "
            "'client_secret' in connection.configure",
        )

    cache_key = (token_url, client_id)
    with _OAUTH2_TOKEN_CACHE_LOCK:
        cached = _OAUTH2_TOKEN_CACHE.get(cache_key)
        if cached is not None:
            cached_token, expires_at = cached
            if time.monotonic() < expires_at:
                logger.debug(
                    "OAuth2 token served from cache for %s",
                    safe_url(token_url),
                )
                return cached_token

    auth_method = conn_cfg.get("token_auth_method", "client_secret_post").lower()
    body_format = conn_cfg.get("token_request_body_format", "form").lower()

    payload: Dict[str, Any] = {"grant_type": "client_credentials"}
    scope = conn_cfg.get("scope", "")
    if scope:
        payload["scope"] = scope
    extras = conn_cfg.get("token_request_extras") or {}
    payload.update(extras)

    # client_secret_post: credentials go in the body (RFC 6749 §2.3.1, method 2)
    # client_secret_basic: credentials go in HTTP Basic Auth header (method 1)
    request_kwargs: Dict[str, Any] = {"timeout": 30}
    if auth_method == "client_secret_basic":
        request_kwargs["auth"] = (client_id, client_secret)
    else:  # client_secret_post (default)
        payload["client_id"] = client_id
        payload["client_secret"] = client_secret

    if body_format == "json":
        request_kwargs["json"] = payload
    else:  # form (default, application/x-www-form-urlencoded)
        request_kwargs["data"] = payload

    token_error: Optional[SourceError] = None
    try:
        response = httpx.post(token_url, **request_kwargs)
    except httpx.HTTPError as exc:
        # Raise outside this handler so the raw client exception (and any
        # URL/token text it carries) is not retained as the error context.
        token_error = SourceError(
            f"OAuth2 token request failed ({type(exc).__name__})",
            details={"method": "POST", "token_url": safe_url(token_url)},
        )
    if token_error is not None:
        raise token_error

    if response.status_code >= 400:
        raise SourceError(
            f"OAuth2 token endpoint returned HTTP {response.status_code}",
            details={
                "method": "POST",
                "token_url": safe_url(token_url),
                "status": response.status_code,
            },
        )

    try:
        resp_json = response.json()
    except (TypeError, ValueError):
        raise SourceError(
            "OAuth2 token endpoint returned invalid JSON",
            details={"method": "POST", "token_url": safe_url(token_url)},
        ) from None
    if not isinstance(resp_json, dict):
        raise SourceError(
            "OAuth2 token endpoint returned a non-object JSON value",
            details={"method": "POST", "token_url": safe_url(token_url)},
        )
    token = resp_json.get("access_token", "")
    if not token:
        raise SourceError(
            "OAuth2 token response did not contain 'access_token'",
            details={"method": "POST", "token_url": safe_url(token_url)},
        )

    expires_in = float(resp_json.get("expires_in", 3600))
    with _OAUTH2_TOKEN_CACHE_LOCK:
        _OAUTH2_TOKEN_CACHE[cache_key] = (token, time.monotonic() + expires_in - 30)
    logger.debug(
        "OAuth2 token fetched and cached for %s (expires_in=%.0fs)",
        safe_url(token_url),
        expires_in,
    )
    return token

__all__ = ["apply_auth", "fetch_oauth2_token", "get_http_auth"]
