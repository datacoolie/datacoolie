"""Write discovery results as flat CSV files for easy human review.

Output structure (tables/files/lakehouse):
    {output_dir}/
    └── catalog.csv              # One row per column, opens in Excel

Output structure (API):
    {output_dir}/
    └── endpoints.csv            # One row per endpoint

These are reference documents for the next ETL design step, not machine config.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from _types import SourceResult


def write_assets(result: SourceResult, output_dir: Path) -> None:
    """Write flat CSV catalog for human review."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if result.source_type == "api":
        filepath = output_dir / "endpoints.csv"
        _write_endpoints_csv(result, filepath)
    else:
        filepath = output_dir / "catalog.csv"
        _write_catalog_csv(result, filepath)

    print(f"Written to {filepath}", file=sys.stderr)


def _write_catalog_csv(result: SourceResult, filepath: Path) -> None:
    """Write flat catalog CSV — one row per column, easy to open in Excel."""
    content = format_csv(result)
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        f.write(content)


def _write_endpoints_csv(result: SourceResult, filepath: Path) -> None:
    """Write flat endpoints CSV — one row per endpoint."""
    content = format_csv(result)
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        f.write(content)


def format_csv(result: SourceResult) -> str:
    """Format result as CSV string (for stdout)."""
    import io

    if result.source_type == "api":
        buf = io.StringIO()
        fieldnames = ["method", "path", "summary", "parameters", "response_fields", "pagination"]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for ep in result.endpoints:
            params = ", ".join(
                f"{p.name} ({p.location}{'*' if p.required else ''})"
                for p in ep.parameters
            )
            response_fields = ""
            schema = ep.response_schema
            if schema:
                props = schema.get("properties")
                if not props and schema.get("items"):
                    props = schema["items"].get("properties")
                if props:
                    response_fields = ", ".join(props.keys())
            writer.writerow({
                "method": ep.method,
                "path": ep.path,
                "summary": ep.summary,
                "parameters": params,
                "response_fields": response_fields,
                "pagination": ep.pagination_type or "",
            })
        return buf.getvalue()
    else:
        buf = io.StringIO()
        fieldnames = [
            "table", "schema", "column", "type", "nullable", "is_pk", "is_fk", "references",
            "ordinal", "row_estimate", "file_count", "format", "representative_file",
        ]
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for tbl in result.tables:
            if tbl.columns:
                for col in tbl.columns:
                    writer.writerow({
                        "table": tbl.table_name,
                        "schema": tbl.schema or "",
                        "column": col.name,
                        "type": col.data_type,
                        "nullable": col.nullable,
                        "is_pk": col.is_primary_key,
                        "is_fk": col.is_foreign_key,
                        "references": col.references or "",
                        "ordinal": col.ordinal,
                        "row_estimate": tbl.row_estimate or "",
                        "file_count": tbl.file_count or "",
                        "format": tbl.file_format or "",
                        "representative_file": tbl.representative_file or "",
                    })
            else:
                writer.writerow({
                    "table": tbl.table_name,
                    "schema": tbl.schema or "",
                    "column": "(no columns extracted)",
                    "type": "",
                    "nullable": "",
                    "is_pk": "",
                    "is_fk": "",
                    "references": "",
                    "ordinal": "",
                    "row_estimate": tbl.row_estimate or "",
                    "file_count": tbl.file_count or "",
                    "format": tbl.file_format or "",
                    "representative_file": tbl.representative_file or "",
                })
        return buf.getvalue()
