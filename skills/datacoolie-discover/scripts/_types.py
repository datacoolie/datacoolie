"""Shared data types for datacoolie-discover introspection results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Column:
    """A column in a table or file."""

    name: str
    data_type: str
    ordinal: int = 0
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: str | None = None  # "schema.table.column"
    comment: str | None = None
    watermark_candidate: bool = False


@dataclass
class ForeignKey:
    """A foreign key constraint."""

    columns: list[str]
    references_table: str
    references_columns: list[str]


@dataclass
class Index:
    """A table index."""

    name: str
    columns: list[str]
    unique: bool = False


@dataclass
class Table:
    """A discovered table (DB, file group, or lakehouse table)."""

    table_name: str
    schema: str | None = None
    table_type: str = "BASE TABLE"  # BASE TABLE | VIEW | EXTERNAL
    columns: list[Column] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    row_estimate: int | None = None
    # File-specific
    file_count: int | None = None
    file_format: str | None = None
    representative_file: str | None = None
    # Lakehouse-specific
    location: str | None = None
    partition_keys: list[str] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class ApiParameter:
    """An API endpoint parameter."""

    name: str
    location: str  # query | header | path | cookie
    param_type: str = "string"
    required: bool = False
    watermark_candidate: bool = False


@dataclass
class ApiEndpoint:
    """A discovered API endpoint."""

    path: str
    method: str
    summary: str = ""
    parameters: list[ApiParameter] = field(default_factory=list)
    response_schema: dict[str, Any] | None = None
    pagination_type: str | None = None  # offset | cursor | link_header | none


@dataclass
class SourceResult:
    """Top-level introspection result."""

    source_type: str  # db | file | api | lakehouse | other
    source_name: str = ""
    # DB-specific
    dialect: str | None = None
    auth_method: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    # File-specific
    path: str | None = None
    storage_backend: str | None = None  # local | s3 | adls | onelake | dbfs | gcs | hdfs
    # API-specific
    spec_url: str | None = None
    api_type: str | None = None  # openapi | graphql | odata | soap | rest
    # Lakehouse-specific
    catalog: str | None = None  # glue | unity | fabric | iceberg | delta | hive
    # Results
    tables: list[Table] = field(default_factory=list)
    endpoints: list[ApiEndpoint] = field(default_factory=list)
    # Metadata
    note: str | None = None
    schema_count: int = 0
    total_columns: int = 0
    estimated_total_rows: int = 0
