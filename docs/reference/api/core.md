---
title: Core — Python API Reference | DataCoolie
description: Python API reference for DataCoolie core modules covering models, registry, secret resolution, constants, and shared abstractions.
---

# Core

!!! info "Authored metadata and Python models have separate owners"
    Authored JSON fields are rendered on the [Metadata reference](../metadata-schema.md#metadata-document)
    page. Hydrated Python model details and runtime-only properties remain the
    responsibility of this API reference. Driver session and replay models are rendered on the
    [Runtime configuration](../runtime-configuration.md) page. This page owns
    core helpers, registries, secrets and exceptions.

The model constructors below use keyword fields. Authored JSON/YAML is a
separate input shape: a metadata provider resolves connection references and
hydrates nested `Connection`, `Source`, `Destination`, `Transform`, and
`DataFlow` objects before the Driver executes them. For example, this is a
small programmatic model graph and its derived file path:

```python
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source

connection = Connection(
    name="orders",
    format="csv",
    configure={"base_path": "input"},
)
source = Source(connection=connection, table="orders")

assert connection.connection_type == "file"
assert source.path == "input/orders"
```

The example uses Python keyword arguments. A provider can start from authored
JSON/YAML, resolve a `connection_name` reference, and then pass the resulting
values to these same model constructors. The schema reference remains the
authority for authored keys and validation; this page is the authority for the
hydrated Python objects.

## Registry factories

The package root exposes six registry-backed factories. Each `name` selects a
registered implementation and `**kwargs` are passed to that implementation.
The factories describe runtime construction; entry-point discovery and
metadata format validation are separate contracts.

::: datacoolie
    options:
      members:
        - create_engine
        - create_platform
        - create_source
        - create_destination
        - create_transformer
        - create_resolver

## Models and constants

`CompatModel` supplies the keyword construction, nested mapping coercion, and
`model_dump*` helpers used by the metadata models. Its subclasses use
`@dataclass(init=False)`, so their generated dataclass signature is not the
runtime constructor contract. The class signatures are hidden here and the
declared fields are listed explicitly so the reference does not suggest an
empty constructor.

::: datacoolie.core.models.base.CompatModel
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - model_fields
        - model_construct
        - model_copy
        - model_dump
        - model_dump_json

::: datacoolie.core.models.connection.Connection
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - name
        - connection_id
        - workspace_id
        - connection_type
        - format
        - catalog
        - database
        - configure
        - secrets_ref
        - is_active
        - base_path
        - host
        - port
        - username
        - password
        - database_type
        - schema_hint_type_system
        - auth_type
        - tenant_id
        - token
        - url
        - driver
        - read_options
        - write_options
        - merge_options
        - use_schema_hint
        - use_hive_partitioning
        - athena_output_location
        - generate_manifest
        - register_symlink_table
        - symlink_database_prefix
        - date_folder_partitions
        - date_backward

::: datacoolie.core.models.source.Source
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - connection
        - schema_name
        - table
        - query
        - python_function
        - watermark_columns
        - filter_expression
        - configure
        - full_table_name
        - namespace
        - path
        - read_options
        - has_watermark_state
        - date_backward

::: datacoolie.core.models.destination.PartitionColumn
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - column
        - expression

::: datacoolie.core.models.destination.Destination
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - connection
        - table
        - schema_name
        - load_type
        - merge_keys
        - partition_columns
        - configure
        - full_table_name
        - namespace
        - path
        - write_options
        - merge_options
        - partition_column_names
        - merge_keys_extended
        - scd2_effective_column
        - replace_by_watermark

::: datacoolie.core.models.transform.SchemaHint
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - column_name
        - data_type
        - format
        - precision
        - scale
        - default_value
        - ordinal_position
        - is_active

::: datacoolie.core.models.transform.AdditionalColumn
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - column
        - expression

::: datacoolie.core.models.transform.ValueRule
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - operation
        - columns
        - order
        - mode
        - pattern
        - replacement
        - value
        - mapping
        - on_unmapped

::: datacoolie.core.models.transform.HashColumn
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - target_column
        - columns
        - algorithm
        - serialization

::: datacoolie.core.models.transform.MaskingRule
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - method
        - columns
        - value
        - keep_start
        - keep_end
        - mask_char
        - bucket_size
        - unit

::: datacoolie.core.models.transform.Transform
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - deduplicate_columns
        - latest_data_columns
        - filter_expression
        - additional_columns
        - schema_hints
        - select_columns
        - drop_columns
        - rename_columns
        - value_rules
        - hash_columns
        - masking_rules
        - configure
        - missing_column_policy
        - deduplicate_column_names
        - convert_timestamp_ntz
        - timestamp_timezone
        - deduplicate_by_rank
        - schema_hints_dict

::: datacoolie.core.models.dataflow.DataFlow
    options:
      show_signature: false
      show_if_no_docstring: true
      members:
        - source
        - destination
        - dataflow_id
        - workspace_id
        - name
        - description
        - stage
        - group_number
        - execution_order
        - processing_mode
        - is_active
        - transform
        - configure
        - load_type
        - merge_keys
        - partition_columns
        - partition_column_names
        - deduplicate_columns
        - order_columns

::: datacoolie.core.constants
    options:
      members:
        - LoadType
        - Format
        - ConnectionType
        - ProcessingMode
        - DataFlowStatus
        - ExecutionType
        - DatabaseType
        - DatabaseAuthType
        - MaintenanceType
        - ColumnCaseMode
        - SystemColumn
        - FileInfoColumn
        - SCD2Column
        - CONNECTION_TYPE_FORMATS
        - TRAILING_COLUMNS
        - WATERMARK_FILE_NAME
        - DEFAULT_MAX_WORKERS
        - DEFAULT_RETRY_COUNT
        - DEFAULT_RETRY_DELAY
        - DEFAULT_RETENTION_HOURS
        - XXHASH64_SEED

::: datacoolie.core.registry
    options:
      show_submodules: false

::: datacoolie.core.exceptions
    options:
      show_submodules: false

::: datacoolie.core.secrets.provider
    options:
      show_submodules: false

::: datacoolie.core.secrets.resolver
    options:
      show_submodules: false
