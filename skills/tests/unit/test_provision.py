"""Unit tests for datacoolie-provision scripts.

Tests cover:
- Architecture parser (parse_infra_requirements)
- Provider command generation (all 4 providers)
- Terraform file generation
- Environment naming
- Preflight checks (mocked)
- Path traversal protection (local provider)
- find_latest_architecture
- provision_cli orchestration
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_ARCHITECTURE = """\
# Architecture Design — TestProject

## Overview

- **Source count:** 3
- **Target platform:** AWS
- **Estimated daily volume:** 5 GB

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| AWS | S3 Bucket | bronze-data | Raw data landing |
| AWS | S3 Bucket | silver-data | Cleansed data |
| AWS | Glue Database | gold_db | Serving layer catalog |
| AWS | IAM Role | etl-role | ETL execution role |

## Stage Definitions

| Stage Name | Source | Destination | Load Type | Engine | Schedule |
|---|---|---|---|---|---|
| erp_source2bronze | erp_conn | bronze/orders | full_load | polars | daily |
"""

FABRIC_ARCHITECTURE = """\
# Architecture Design — FabricProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| Fabric | Lakehouse | bronze_lake | Raw data |
| Fabric | Lakehouse | silver_lake | Cleansed data |
| Fabric | Warehouse | gold_wh | Serving layer |
"""

LOCAL_ARCHITECTURE = """\
# Architecture Design — LocalProject

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| Local | Directory | data/bronze | Raw data |
| Local | Directory | data/silver | Cleansed data |
| Local | Directory | data/gold | Serving layer |
| Local | Directory | metadata | Metadata files |
"""


@pytest.fixture
def sample_arch_file(tmp_path):
    """Create a sample architecture file."""
    arch_file = tmp_path / "architecture.md"
    arch_file.write_text(SAMPLE_ARCHITECTURE, encoding="utf-8")
    return arch_file


@pytest.fixture
def fabric_arch_file(tmp_path):
    """Create a Fabric architecture file."""
    arch_file = tmp_path / "architecture.md"
    arch_file.write_text(FABRIC_ARCHITECTURE, encoding="utf-8")
    return arch_file


@pytest.fixture
def local_arch_file(tmp_path):
    """Create a local architecture file."""
    arch_file = tmp_path / "architecture.md"
    arch_file.write_text(LOCAL_ARCHITECTURE, encoding="utf-8")
    return arch_file


# ---------------------------------------------------------------------------
# Architecture parser tests
# ---------------------------------------------------------------------------


class TestParseInfraRequirements:
    """Tests for parse_infra_requirements."""

    def test_parses_aws_resources(self, sample_arch_file):
        from provision import parse_infra_requirements

        resources = parse_infra_requirements(sample_arch_file)
        assert len(resources) == 4
        assert resources[0]["resource_type"] == "S3 Bucket"
        assert resources[0]["name"] == "bronze-data"
        assert resources[0]["purpose"] == "Raw data landing"

    def test_parses_fabric_resources(self, fabric_arch_file):
        from provision import parse_infra_requirements

        resources = parse_infra_requirements(fabric_arch_file)
        assert len(resources) == 3
        assert resources[0]["resource_type"] == "Lakehouse"
        assert resources[2]["resource_type"] == "Warehouse"

    def test_parses_local_resources(self, local_arch_file):
        from provision import parse_infra_requirements

        resources = parse_infra_requirements(local_arch_file)
        assert len(resources) == 4
        assert resources[0]["name"] == "data/bronze"

    def test_returns_empty_if_no_table(self, tmp_path):
        from provision import parse_infra_requirements

        arch = tmp_path / "empty.md"
        arch.write_text("# No infra table here\n\nJust text.", encoding="utf-8")
        assert parse_infra_requirements(arch) == []


# ---------------------------------------------------------------------------
# Environment naming tests
# ---------------------------------------------------------------------------


class TestEnvNaming:
    """Tests for apply_env_suffix."""

    def test_prod_no_suffix(self):
        from provision import apply_env_suffix

        assert apply_env_suffix("bronze_lake", "prod") == "bronze_lake"

    def test_dev_adds_suffix(self):
        from provision import apply_env_suffix

        assert apply_env_suffix("bronze_lake", "dev") == "bronze_lake_dev"

    def test_test_adds_suffix(self):
        from provision import apply_env_suffix

        assert apply_env_suffix("bronze-data", "test") == "bronze-data_test"


# ---------------------------------------------------------------------------
# Provider command generation tests
# ---------------------------------------------------------------------------


class TestAwsProvider:
    """Tests for AWS provider command generation."""

    def test_s3_create_command(self):
        from providers import aws

        cmd = aws.create_command("S3 Bucket", "my-bucket")
        assert cmd == ["aws", "s3", "mb", "s3://my-bucket"]

    def test_s3_bucket_variant(self):
        from providers import aws

        cmd = aws.create_command("s3", "my-bucket")
        assert cmd == ["aws", "s3", "mb", "s3://my-bucket"]

    def test_glue_database_command(self):
        from providers import aws

        cmd = aws.create_command("Glue Database", "my_db")
        assert cmd is not None
        assert "create-database" in cmd

    def test_iam_role_command(self):
        from providers import aws

        cmd = aws.create_command("IAM Role", "etl-role")
        assert cmd is not None
        assert "create-role" in cmd
        assert "etl-role" in cmd

    def test_unsupported_resource(self):
        from providers import aws

        cmd = aws.create_command("unknown_type", "name")
        assert cmd is None


class TestFabricProvider:
    """Tests for Fabric provider command generation."""

    def test_lakehouse_command(self):
        from providers import fabric

        cmd = fabric.create_command("lakehouse", "bronze_lake")
        assert cmd is not None
        assert "lakehouse" in cmd
        assert "create" in cmd
        assert "bronze_lake" in cmd

    def test_warehouse_command(self):
        from providers import fabric

        cmd = fabric.create_command("warehouse", "gold_wh")
        assert cmd is not None
        assert "warehouse" in cmd
        assert "gold_wh" in cmd

    def test_workspace_command(self):
        from providers import fabric

        cmd = fabric.create_command("workspace", "my_ws")
        assert cmd is not None
        assert "workspace" in cmd

    def test_unsupported_resource(self):
        from providers import fabric

        cmd = fabric.create_command("unknown", "name")
        assert cmd is None


class TestDatabricksProvider:
    """Tests for Databricks provider command generation."""

    def test_catalog_command(self):
        from providers import databricks

        cmd = databricks.create_command("catalog", "main")
        assert cmd is not None
        assert "create-catalog" in cmd
        assert "main" in cmd

    def test_schema_command(self):
        from providers import databricks

        cmd = databricks.create_command("schema", "bronze")
        assert cmd is not None
        assert "create-schema" in cmd

    def test_volume_command(self):
        from providers import databricks

        cmd = databricks.create_command("volume", "raw_data")
        assert cmd is not None
        assert "create-volume" in cmd
        assert "MANAGED" in cmd

    def test_unsupported_resource(self):
        from providers import databricks

        cmd = databricks.create_command("cluster", "name")
        assert cmd is None


class TestLocalProvider:
    """Tests for Local filesystem provider."""

    def test_create_command(self):
        from providers import local

        cmd = local.create_command("directory", "data/bronze")
        assert cmd is not None
        assert "mkdir" in cmd

    def test_create_directory_actual(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        ok, msg = local.create_directory("data/bronze")
        assert ok is True
        assert (tmp_path / "data" / "bronze").exists()

    def test_create_directory_idempotent(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        local.create_directory("data/bronze")
        ok, _ = local.create_directory("data/bronze")
        assert ok is True

    def test_check_exists(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        assert local.check_exists("directory", "data/bronze") is False
        (tmp_path / "data" / "bronze").mkdir(parents=True)
        assert local.check_exists("directory", "data/bronze") is True


# ---------------------------------------------------------------------------
# Terraform generator tests
# ---------------------------------------------------------------------------


class TestTerraformGenerator:
    """Tests for Terraform file generation."""

    def test_generates_main_tf(self, tmp_path):
        from terraform.generate import generate_terraform

        resources = [
            {"platform": "AWS", "resource_type": "S3 Bucket", "name": "bronze-data", "purpose": "Raw"},
        ]
        results = generate_terraform(resources, "aws", "dev", tmp_path)
        assert any(r["name"] == "main.tf" and r["status"] == "created" for r in results)
        assert (tmp_path / "main.tf").exists()

    def test_generates_variables_tf(self, tmp_path):
        from terraform.generate import generate_terraform

        resources = [
            {"platform": "AWS", "resource_type": "S3 Bucket", "name": "bronze-data", "purpose": "Raw"},
        ]
        results = generate_terraform(resources, "aws", "dev", tmp_path)
        assert (tmp_path / "variables.tf").exists()
        content = (tmp_path / "variables.tf").read_text()
        assert "aws_region" in content

    def test_databricks_variables(self, tmp_path):
        from terraform.generate import generate_terraform

        resources = [
            {"platform": "Databricks", "resource_type": "Catalog", "name": "main", "purpose": "Unity"},
        ]
        generate_terraform(resources, "databricks", "prod", tmp_path)
        content = (tmp_path / "variables.tf").read_text()
        assert "databricks_host" in content
        assert "sensitive" in content

    def test_missing_template_reports_error(self, tmp_path):
        from terraform.generate import generate_terraform

        resources = [
            {"platform": "Unknown", "resource_type": "X", "name": "y", "purpose": "z"},
        ]
        results = generate_terraform(resources, "nonexistent", "dev", tmp_path)
        assert results[0]["status"] == "error"


# ---------------------------------------------------------------------------
# Preflight tests
# ---------------------------------------------------------------------------


class TestPreflight:
    """Tests for preflight checks."""

    def test_local_always_passes(self):
        from provision import check_preflight

        ok, msg = check_preflight("local")
        assert ok is True

    @patch("provision.subprocess.run")
    def test_cli_found(self, mock_run):
        from provision import check_preflight

        mock_run.return_value = MagicMock(returncode=0, stdout="aws-cli/2.15.0", stderr="")
        ok, msg = check_preflight("aws")
        assert ok is True
        assert "aws" in msg.lower()

    @patch("provision.subprocess.run", side_effect=FileNotFoundError)
    def test_cli_not_found(self, mock_run):
        from provision import check_preflight

        ok, msg = check_preflight("fabric")
        assert ok is False
        assert "not found" in msg.lower()


# ---------------------------------------------------------------------------
# Provision log tests
# ---------------------------------------------------------------------------


class TestProvisionLog:
    """Tests for provision log writing."""

    def test_writes_log_file(self, tmp_path, sample_arch_file):
        from provision import write_provision_log

        results = [
            {
                "resource": {"platform": "AWS", "resource_type": "S3 Bucket", "name": "bronze", "purpose": "Raw"},
                "name": "bronze_dev",
                "action": "dry_run",
                "status": "pending",
                "command": ["aws", "s3", "mb", "s3://bronze_dev"],
                "output": "[DRY RUN] Would execute.",
            }
        ]
        log_path = write_provision_log(results, sample_arch_file, "aws", "cli", "dev", tmp_path)
        assert log_path.exists()
        content = log_path.read_text()
        assert "Dry Run" in content
        assert "bronze_dev" in content

    def test_log_records_failures(self, tmp_path, sample_arch_file):
        from provision import write_provision_log

        results = [
            {
                "resource": {"platform": "AWS", "resource_type": "S3 Bucket", "name": "x", "purpose": ""},
                "name": "x_dev",
                "action": "create",
                "status": "failed",
                "command": ["aws", "s3", "mb", "s3://x_dev"],
                "output": "Access Denied",
            }
        ]
        log_path = write_provision_log(results, sample_arch_file, "aws", "cli", "dev", tmp_path)
        content = log_path.read_text()
        assert "Errors" in content
        assert "Access Denied" in content


# ---------------------------------------------------------------------------
# Path traversal protection tests
# ---------------------------------------------------------------------------


class TestPathTraversal:
    """Tests for path traversal rejection in local provider."""

    def test_traversal_blocked_in_create(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        ok, msg = local.create_directory("../../etc/evil")
        assert ok is False
        assert "traversal" in msg.lower()

    def test_traversal_blocked_in_check_exists(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        assert local.check_exists("directory", "../../etc") is False

    def test_traversal_blocked_in_create_command(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        cmd = local.create_command("directory", "../../etc/evil")
        assert cmd is None

    def test_normal_nested_path_allowed(self, tmp_path):
        from providers import local

        local.set_base_path(tmp_path)
        ok, msg = local.create_directory("a/b/c")
        assert ok is True
        assert (tmp_path / "a" / "b" / "c").exists()


# ---------------------------------------------------------------------------
# find_latest_architecture tests
# ---------------------------------------------------------------------------


class TestFindLatestArchitecture:
    """Tests for find_latest_architecture."""

    def test_finds_latest_file(self, tmp_path):
        from provision import find_latest_architecture

        arch_dir = tmp_path / ".datacoolie" / "architect"
        arch_dir.mkdir(parents=True)
        (arch_dir / "240101_architecture.md").write_text("old")
        (arch_dir / "260520_architecture.md").write_text("new")
        result = find_latest_architecture(tmp_path)
        assert result is not None
        assert "260520" in result.name

    def test_returns_none_if_no_dir(self, tmp_path):
        from provision import find_latest_architecture

        assert find_latest_architecture(tmp_path) is None

    def test_returns_none_if_no_files(self, tmp_path):
        from provision import find_latest_architecture

        (tmp_path / ".datacoolie" / "architect").mkdir(parents=True)
        assert find_latest_architecture(tmp_path) is None


# ---------------------------------------------------------------------------
# provision_cli orchestration tests
# ---------------------------------------------------------------------------


class TestProvisionCli:
    """Tests for provision_cli end-to-end behavior."""

    def test_dry_run_returns_pending(self, tmp_path):
        from provision import provision_cli
        from providers import local

        local.set_base_path(tmp_path)
        resources = [
            {"platform": "Local", "resource_type": "Directory", "name": "data/bronze", "purpose": "Raw"},
        ]
        results = provision_cli(resources, "local", "dev", confirm=False)
        assert len(results) == 1
        assert results[0]["status"] == "pending"
        assert results[0]["action"] == "dry_run"

    def test_confirm_creates_local_dirs(self, tmp_path):
        from provision import provision_cli
        from providers import local

        local.set_base_path(tmp_path)
        resources = [
            {"platform": "Local", "resource_type": "Directory", "name": "data/bronze", "purpose": "Raw"},
            {"platform": "Local", "resource_type": "Directory", "name": "data/silver", "purpose": "Clean"},
        ]
        results = provision_cli(resources, "local", "dev", confirm=True)
        assert all(r["status"] == "created" for r in results)
        assert (tmp_path / "data" / "bronze_dev").exists()
        assert (tmp_path / "data" / "silver_dev").exists()

    def test_skips_existing_resources(self, tmp_path):
        from provision import provision_cli
        from providers import local

        local.set_base_path(tmp_path)
        (tmp_path / "data" / "bronze_dev").mkdir(parents=True)
        resources = [
            {"platform": "Local", "resource_type": "Directory", "name": "data/bronze", "purpose": "Raw"},
        ]
        results = provision_cli(resources, "local", "dev", confirm=True)
        assert results[0]["status"] == "already_exists"

    def test_unsupported_resource_type_skipped(self, tmp_path):
        from provision import provision_cli
        from providers import local

        local.set_base_path(tmp_path)
        resources = [
            {"platform": "Local", "resource_type": "database", "name": "mydb", "purpose": "Test"},
        ]
        results = provision_cli(resources, "local", "dev", confirm=True)
        assert results[0]["status"] == "unsupported"
