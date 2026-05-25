"""Unit tests for datacoolie-deploy: promote.py + new preflight checks."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# conftest.py already adds deploy scripts to sys.path
import preflight
import promote


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_project(tmp_path: Path, *, with_metadata: bool = True) -> Path:
    """Create a minimal project directory."""
    if with_metadata:
        meta = tmp_path / "metadata"
        meta.mkdir()
        dataflows = {
            "dataflows": [{"name": "sales_load", "load_type": "full_load"}]
        }
        (meta / "dataflows.json").write_text(json.dumps(dataflows), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# check_secrets_resolution
# ---------------------------------------------------------------------------

class TestCheckSecretsResolution:
    def test_no_metadata_dir(self, tmp_path):
        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is True
        assert "No metadata" in msg

    def test_no_secrets_ref_in_files(self, tmp_path):
        _make_project(tmp_path)
        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is True
        assert "No secrets_ref" in msg or "All" in msg or "skip" in msg.lower()

    def test_secrets_ref_resolved(self, tmp_path, monkeypatch):
        meta = tmp_path / "metadata"
        meta.mkdir()
        conn = {"connections": [{"name": "db", "password": {"secrets_ref": "MY_SECRET"}}]}
        (meta / "connections.json").write_text(json.dumps(conn), encoding="utf-8")
        monkeypatch.setenv("MY_SECRET", "supersecret")

        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is True
        assert "1" in msg

    def test_secrets_ref_missing(self, tmp_path, monkeypatch):
        meta = tmp_path / "metadata"
        meta.mkdir()
        conn = {"connections": [{"name": "db", "password": {"secrets_ref": "MISSING_SECRET"}}]}
        (meta / "connections.json").write_text(json.dumps(conn), encoding="utf-8")
        monkeypatch.delenv("MISSING_SECRET", raising=False)

        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is False
        assert "MISSING_SECRET" in msg

    def test_multiple_secrets_some_missing(self, tmp_path, monkeypatch):
        meta = tmp_path / "metadata"
        meta.mkdir()
        conn = {
            "connections": [
                {"name": "a", "key": {"secrets_ref": "SECRET_A"}},
                {"name": "b", "key": {"secrets_ref": "SECRET_B"}},
            ]
        }
        (meta / "connections.json").write_text(json.dumps(conn), encoding="utf-8")
        monkeypatch.setenv("SECRET_A", "val_a")
        monkeypatch.delenv("SECRET_B", raising=False)

        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is False
        assert "SECRET_B" in msg

    def test_yaml_file_with_secrets_ref(self, tmp_path, monkeypatch):
        import yaml
        meta = tmp_path / "metadata"
        meta.mkdir()
        data = {"connections": [{"name": "svc", "token": {"secrets_ref": "SVC_TOKEN"}}]}
        (meta / "connections.yaml").write_text(yaml.dump(data), encoding="utf-8")
        monkeypatch.setenv("SVC_TOKEN", "mytoken")

        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        assert ok is True

    def test_malformed_file_is_skipped(self, tmp_path, monkeypatch):
        meta = tmp_path / "metadata"
        meta.mkdir()
        # Write invalid JSON that contains the string "secrets_ref"
        (meta / "bad.json").write_text('{"secrets_ref": broken}', encoding="utf-8")

        ok, msg = preflight.check_secrets_resolution(tmp_path, "dev")
        # Should not raise — malformed file is skipped
        assert isinstance(ok, bool)


# ---------------------------------------------------------------------------
# check_infra_exists
# ---------------------------------------------------------------------------

class TestCheckInfraExists:
    def test_local_always_ok(self, tmp_path):
        ok, msg = preflight.check_infra_exists("local", tmp_path, "dev")
        assert ok is True
        assert "local" in msg

    def test_provision_log_all_created(self, tmp_path):
        log_dir = tmp_path / ".datacoolie" / "provision"
        log_dir.mkdir(parents=True)
        log = log_dir / "250101_provision-log.md"
        log.write_text("| bucket | created |\n| db | created |", encoding="utf-8")

        ok, msg = preflight.check_infra_exists("aws", tmp_path, "prod")
        assert ok is True
        assert "2" in msg or "created" in msg

    def test_provision_log_with_failures(self, tmp_path):
        log_dir = tmp_path / ".datacoolie" / "provision"
        log_dir.mkdir(parents=True)
        log = log_dir / "250101_provision-log.md"
        log.write_text("| bucket | created |\n| db | failed |", encoding="utf-8")

        ok, msg = preflight.check_infra_exists("aws", tmp_path, "prod")
        assert ok is False
        assert "failed" in msg.lower()

    def test_no_log_platform_reachable(self, tmp_path):
        with patch("preflight._run", return_value=(0, "")):
            ok, msg = preflight.check_infra_exists("aws", tmp_path, "dev")
        assert ok is True

    def test_no_log_platform_unreachable(self, tmp_path):
        with patch("preflight._run", return_value=(1, "connection refused")):
            ok, msg = preflight.check_infra_exists("aws", tmp_path, "dev")
        assert ok is False

    def test_unknown_platform_skip(self, tmp_path):
        ok, msg = preflight.check_infra_exists("openshift", tmp_path, "dev")
        assert ok is True
        assert "skip" in msg.lower() or "unknown" in msg.lower()


# ---------------------------------------------------------------------------
# run_preflight — ensure new checks are wired in
# ---------------------------------------------------------------------------

class TestRunPreflightExtended:
    def test_run_preflight_includes_secrets_and_infra(self, tmp_path, capsys):
        """run_preflight with env_name param should call new checks."""
        with (
            patch("preflight.check_cli", return_value=(True, "ok")),
            patch("preflight.check_auth", return_value=(True, "ok")),
            patch("preflight.check_datacoolie", return_value=(True, "ok")),
            patch("preflight.check_metadata", return_value=(True, "ok")),
            patch("preflight.check_functions", return_value=(True, "ok")),
            patch("preflight.check_secrets_resolution", return_value=(True, "all ok")) as mock_sec,
            patch("preflight.check_infra_exists", return_value=(True, "ok")) as mock_infra,
        ):
            result = preflight.run_preflight("aws", tmp_path, skip_auth=True, env_name="prod")

        assert result is True
        mock_sec.assert_called_once_with(tmp_path, "prod")
        mock_infra.assert_called_once_with("aws", tmp_path, "prod")

    def test_run_preflight_fails_on_missing_secret(self, tmp_path, capsys):
        with (
            patch("preflight.check_cli", return_value=(True, "ok")),
            patch("preflight.check_auth", return_value=(True, "ok")),
            patch("preflight.check_datacoolie", return_value=(True, "ok")),
            patch("preflight.check_metadata", return_value=(True, "ok")),
            patch("preflight.check_functions", return_value=(True, "ok")),
            patch("preflight.check_secrets_resolution", return_value=(False, "MISSING_SECRET unresolved")),
            patch("preflight.check_infra_exists", return_value=(True, "ok")),
        ):
            result = preflight.run_preflight("aws", tmp_path, skip_auth=True, env_name="dev")

        assert result is False


# ---------------------------------------------------------------------------
# promote.write_promotion_log
# ---------------------------------------------------------------------------

class TestWritePromotionLog:
    def test_creates_log_file(self, tmp_path):
        steps = [
            {"name": "Preflight", "status": "pass", "detail": "all ok"},
            {"name": "Deploy", "status": "pass", "detail": "deployed"},
        ]
        log = promote.write_promotion_log(steps, "dev", "prod", tmp_path)
        assert log.exists()
        content = log.read_text(encoding="utf-8")
        assert "dev" in content
        assert "prod" in content
        assert "Success" in content

    def test_log_shows_failure(self, tmp_path):
        steps = [
            {"name": "Preflight", "status": "fail", "detail": "CLI missing"},
        ]
        log = promote.write_promotion_log(steps, "dev", "prod", tmp_path)
        content = log.read_text(encoding="utf-8")
        assert "Failed" in content
        assert "CLI missing" in content

    def test_log_placed_in_datacoolie_promote(self, tmp_path):
        steps = [{"name": "Preflight", "status": "pass", "detail": "ok"}]
        log = promote.write_promotion_log(steps, "dev", "test", tmp_path)
        assert ".datacoolie" in str(log)
        assert "promote" in str(log)

    def test_log_filename_includes_envs(self, tmp_path):
        steps = [{"name": "Preflight", "status": "pass", "detail": "ok"}]
        log = promote.write_promotion_log(steps, "dev", "prod", tmp_path)
        assert "dev" in log.name
        assert "prod" in log.name


# ---------------------------------------------------------------------------
# promote.promote — high-level workflow
# ---------------------------------------------------------------------------

class TestPromote:
    def _all_pass(self):
        """Return a context that makes all sub-scripts succeed."""
        return patch.object(promote, "_run_script", return_value=(0, "ok"))

    def test_prod_requires_confirm(self, tmp_path):
        rc = promote.promote("dev", "prod", "aws", tmp_path, skip_auth=True, confirm=False)
        assert rc == 2

    def test_from_eq_to_blocked_in_main(self, tmp_path):
        """main() should reject --from X --to X with a non-zero return."""
        with patch("sys.argv", ["promote.py", "--from", "dev", "--to", "dev"]):
            rc = promote.main()
        assert rc != 0

    def test_preflight_failure_aborts(self, tmp_path):
        with patch.object(promote, "_run_script", return_value=(1, "CLI missing")):
            with pytest.raises(SystemExit) as exc:
                promote.promote("dev", "test", "aws", tmp_path, skip_auth=True, confirm=True)
        assert exc.value.code == 1
        # Partial log should exist
        log_dir = tmp_path / ".datacoolie" / "promote"
        assert log_dir.is_dir()

    def test_successful_promotion(self, tmp_path):
        with (
            self._all_pass(),
            patch.object(promote, "_find_metadata_scripts", return_value=None),
        ):
            rc = promote.promote("dev", "test", "local", tmp_path, skip_auth=True, confirm=True)
        assert rc == 0
        # Promotion log must exist
        log_dir = tmp_path / ".datacoolie" / "promote"
        logs = list(log_dir.glob("*.md"))
        assert len(logs) == 1

    def test_metadata_scripts_not_found_skips_gracefully(self, tmp_path):
        with (
            patch.object(promote, "_run_script", return_value=(0, "ok")),
            patch.object(promote, "_find_metadata_scripts", return_value=None),
        ):
            rc = promote.promote("dev", "test", "local", tmp_path, skip_auth=True, confirm=True)
        assert rc == 0

    def test_deploy_failure_logged(self, tmp_path):
        call_count = {"n": 0}

        def _mock_run_script(script_name, args, project_dir, label):
            call_count["n"] += 1
            # preflight (call 1) passes, apply.py (call 2) fails
            if "apply" in script_name:
                return (1, "deploy error")
            return (0, "ok")

        with (
            patch.object(promote, "_run_script", side_effect=_mock_run_script),
            patch.object(promote, "_find_metadata_scripts", return_value=None),
        ):
            rc = promote.promote("dev", "test", "local", tmp_path, skip_auth=True, confirm=True)

        assert rc == 1
        log_dir = tmp_path / ".datacoolie" / "promote"
        logs = list(log_dir.glob("*.md"))
        assert logs
        content = logs[0].read_text(encoding="utf-8")
        assert "fail" in content.lower() or "Failed" in content
