"""Database authentication helpers — token generators and SQLAlchemy event hooks.

Supports: AWS RDS IAM, Azure AD (Entra ID), OAuth2 client_credentials, MSSQL token struct.
"""

from __future__ import annotations

import os
import struct
import sys
from urllib.parse import quote_plus


def resolve_env_var(value: str) -> str:
    """If value starts with $, resolve from environment. Otherwise return as-is."""
    if value and value.startswith("$"):
        env_name = value[1:]
        resolved = os.environ.get(env_name)
        if not resolved:
            print(f"ERROR: env var '{env_name}' not set.", file=sys.stderr)
            sys.exit(1)
        return resolved
    return value


# ---------------------------------------------------------------------------
# Token generators
# ---------------------------------------------------------------------------

def generate_rds_iam_token(host: str, port: int, user: str, region: str | None = None) -> str:
    """Generate an AWS RDS IAM authentication token via boto3."""
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 required for RDS IAM auth — pip install boto3", file=sys.stderr)
        sys.exit(1)

    session = boto3.Session(region_name=region)
    client = session.client("rds")
    token = client.generate_db_auth_token(
        DBHostname=host,
        Port=port,
        DBUsername=user,
        Region=region or session.region_name,
    )
    return token


def generate_azure_ad_token(scope: str = "https://database.windows.net/.default") -> str:
    """Generate an Azure AD (Entra ID) token using DefaultAzureCredential."""
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError:
        print("ERROR: azure-identity required for Azure AD auth — pip install azure-identity", file=sys.stderr)
        sys.exit(1)

    credential = DefaultAzureCredential()
    token = credential.get_token(scope)
    return token.token


def generate_oauth2_token(token_url: str, client_id: str, client_secret: str,
                          scope: str | None = None) -> str:
    """Get an OAuth2 access token via client_credentials grant."""
    try:
        import requests
    except ImportError:
        print("ERROR: requests required for OAuth2 auth — pip install requests", file=sys.stderr)
        sys.exit(1)

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope

    resp = requests.post(token_url, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def build_mssql_token_struct(token: str) -> bytes:
    """Build the token bytes struct expected by pyodbc for MSSQL token auth.

    Format: 4-byte little-endian length prefix + UTF-16LE encoded token.
    This is passed via attrs_before={SQL_COPT_SS_ACCESS_TOKEN: struct}.
    """
    token_bytes = token.encode("UTF-16-LE")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


# ---------------------------------------------------------------------------
# Connection string builders
# ---------------------------------------------------------------------------

def build_connection_string(
    dialect: str,
    host: str,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    auth: str | None = None,
    region: str | None = None,
    token_url: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    driver: str | None = None,
) -> str:
    """Build a SQLAlchemy connection string with optional token-based auth.

    auth modes: None (password), 'rds_iam', 'azure_ad', 'oauth2'
    """
    # Resolve env vars in credentials
    if user:
        user = resolve_env_var(user)
    if password:
        password = resolve_env_var(password)
    if client_id:
        client_id = resolve_env_var(client_id)
    if client_secret:
        client_secret = resolve_env_var(client_secret)

    # Default ports by dialect
    default_ports = {
        "postgresql": 5432,
        "mysql": 3306,
        "mssql": 1433,
        "oracle": 1521,
    }
    port = port or default_ports.get(dialect, 5432)

    # Token-based auth: generate password dynamically
    if auth == "rds_iam":
        password = generate_rds_iam_token(host, port, user, region)
    elif auth == "azure_ad":
        password = generate_azure_ad_token()
    elif auth == "oauth2":
        if not all([token_url, client_id, client_secret]):
            print("ERROR: --token-url, --client-id, --client-secret required for oauth2 auth.", file=sys.stderr)
            sys.exit(1)
        password = generate_oauth2_token(token_url, client_id, client_secret)

    # Build URL per dialect
    if dialect == "postgresql":
        drv = "postgresql+psycopg2"
        url = f"{drv}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database or ''}"
    elif dialect == "mysql":
        drv = "mysql+pymysql"
        url = f"{drv}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database or ''}"
    elif dialect == "mssql":
        drv = driver or "ODBC+Driver+18+for+SQL+Server"
        if auth == "azure_ad":
            # For MSSQL with Azure AD, use token struct via event hook
            odbc = (
                f"DRIVER={{{drv.replace('+', ' ')}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database or 'master'};"
            )
            url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"
        else:
            odbc = (
                f"DRIVER={{{drv.replace('+', ' ')}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database or 'master'};"
                f"UID={user};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
            )
            url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"
    elif dialect == "oracle":
        drv = "oracle+oracledb"
        url = f"{drv}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database or ''}"
    else:
        # Generic fallback
        url = f"{dialect}://{quote_plus(user or '')}:{quote_plus(password or '')}@{host}:{port}/{database or ''}"

    return url


def register_mssql_token_hook(engine, token: str) -> None:
    """Register SQLAlchemy event hook for MSSQL Azure AD token auth.

    Injects token struct into pyodbc connection via attrs_before.
    """
    from sqlalchemy import event

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_struct = build_mssql_token_struct(token)

    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token_struct}
