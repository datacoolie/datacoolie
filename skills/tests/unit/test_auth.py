"""Unit tests for _auth.py — build_connection_string, build_mssql_token_struct, resolve_env_var."""
import os
import struct
import sys
from unittest.mock import MagicMock, patch
from urllib.parse import quote_plus

import pytest

from _auth import build_connection_string, build_mssql_token_struct, resolve_env_var


# ---------------------------------------------------------------------------
# resolve_env_var
# ---------------------------------------------------------------------------

class TestResolveEnvVar:
    def test_plain_value_returned_as_is(self):
        assert resolve_env_var("mypassword") == "mypassword"

    def test_env_var_resolved(self, monkeypatch):
        monkeypatch.setenv("DB_PASS", "s3cr3t")
        assert resolve_env_var("$DB_PASS") == "s3cr3t"

    def test_missing_env_var_exits(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(SystemExit):
            resolve_env_var("$MISSING_VAR")

    def test_empty_string_returned_as_is(self):
        assert resolve_env_var("") == ""


# ---------------------------------------------------------------------------
# build_mssql_token_struct
# ---------------------------------------------------------------------------

class TestBuildMssqlTokenStruct:
    def test_struct_format(self):
        token = "test-token"
        result = build_mssql_token_struct(token)
        token_bytes = token.encode("UTF-16-LE")
        expected = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        assert result == expected

    def test_struct_length_prefix_correct(self):
        token = "hello"
        result = build_mssql_token_struct(token)
        # First 4 bytes = little-endian uint32 = length of UTF-16-LE bytes
        length = struct.unpack_from("<I", result, 0)[0]
        assert length == len(token.encode("UTF-16-LE"))

    def test_non_ascii_token(self):
        token = "tök€n"
        result = build_mssql_token_struct(token)
        assert isinstance(result, bytes)
        assert len(result) > 4  # has both prefix and content


# ---------------------------------------------------------------------------
# build_connection_string (no token generation; password auth only)
# ---------------------------------------------------------------------------

class TestBuildConnectionStringPassword:
    """Test connection string output for password-based auth (no cloud token calls)."""

    def test_postgresql_defaults(self):
        url = build_connection_string("postgresql", "db.host", user="user", password="pass", database="mydb")
        assert url.startswith("postgresql+psycopg2://")
        assert "db.host:5432" in url
        assert "mydb" in url

    def test_postgresql_custom_port(self):
        url = build_connection_string("postgresql", "db.host", port=5433, user="u", password="p", database="d")
        assert "5433" in url

    def test_mysql_defaults(self):
        url = build_connection_string("mysql", "db.host", user="u", password="p", database="sakila")
        assert url.startswith("mysql+pymysql://")
        assert "db.host:3306" in url

    def test_oracle_defaults(self):
        url = build_connection_string("oracle", "db.host", user="hr", password="hr", database="FREEPDB1")
        assert url.startswith("oracle+oracledb://")
        assert "db.host:1521" in url

    def test_mssql_password_auth(self):
        url = build_connection_string(
            "mssql", "db.host", port=1433, user="sa", password="pass",
            database="mydb", driver="ODBC+Driver+18+for+SQL+Server"
        )
        assert "mssql+pyodbc" in url
        assert "mssql+pyodbc" in url

    def test_mssql_azure_ad_skips_uid_pwd(self):
        # azure_ad path calls generate_azure_ad_token — mock it
        with patch("_auth.generate_azure_ad_token", return_value="fake_token"):
            url = build_connection_string(
                "mssql", "db.host", port=1433, database="mydb",
                auth="azure_ad"
            )
        assert "UID=" not in url
        assert "PWD=" not in url

    def test_special_chars_in_password_are_encoded(self):
        url = build_connection_string("postgresql", "db.host", user="u", password="p@ss!w0rd#", database="d")
        assert quote_plus("p@ss!w0rd#") in url

    def test_generic_dialect_fallback(self):
        url = build_connection_string("clickhouse", "db.host", user="u", password="p", database="d")
        assert url.startswith("clickhouse://")

    def test_env_var_resolved_for_password(self, monkeypatch):
        monkeypatch.setenv("MY_DB_PASS", "envpass")
        url = build_connection_string("postgresql", "db.host", user="u", password="$MY_DB_PASS", database="d")
        assert quote_plus("envpass") in url

    def test_rds_iam_calls_generate_token(self):
        with patch("_auth.generate_rds_iam_token", return_value="iam-token") as mock_tok:
            url = build_connection_string(
                "postgresql", "rds.host", user="admin", database="prod",
                auth="rds_iam", region="us-east-1"
            )
        mock_tok.assert_called_once_with("rds.host", 5432, "admin", "us-east-1")
        assert quote_plus("iam-token") in url

    def test_oauth2_calls_generate_token(self):
        with patch("_auth.generate_oauth2_token", return_value="oauth-tok") as mock_tok:
            url = build_connection_string(
                "postgresql", "db.host", user="u", database="d",
                auth="oauth2",
                token_url="https://auth.example.com/token",
                client_id="client",
                client_secret="secret",
            )
        mock_tok.assert_called_once()
        assert quote_plus("oauth-tok") in url

    def test_oauth2_missing_params_exits(self):
        with pytest.raises(SystemExit):
            build_connection_string(
                "postgresql", "db.host", user="u", database="d",
                auth="oauth2",
                # token_url, client_id, client_secret all missing
            )
