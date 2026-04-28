"""Generate `reference/metadata-schema.md` from CompatModel dataclasses.

Runs as an mkdocs-gen-files script so the schema reference is always in
sync with `datacoolie.core.models`. We keep the output as an mkdocstrings
autoref page rather than a handwritten table so field docs come straight
from source.
"""

from __future__ import annotations

import mkdocs_gen_files

CONTENT = """# Metadata schema

DataCoolie's metadata contract lives in `datacoolie.core.models` as
`CompatModel`-backed dataclasses. This page is **generated at docs-build time**
from those models — field descriptions, defaults, and validation rules come
straight from source. Treat the models as the source of truth; this page as the
rendered view.

## Top-level run configuration

::: datacoolie.core.models.DataCoolieRunConfig
    options:
      show_bases: false
      members_order: source
      show_source: false

## Connection

::: datacoolie.core.models.Connection
    options:
      show_bases: false
      members_order: source
      show_source: false

## Source

::: datacoolie.core.models.Source
    options:
      show_bases: false
      members_order: source
      show_source: false

## Destination

::: datacoolie.core.models.Destination
    options:
      show_bases: false
      members_order: source
      show_source: false

## Transform

::: datacoolie.core.models.Transform
    options:
      show_bases: false
      members_order: source
      show_source: false

## DataFlow

::: datacoolie.core.models.DataFlow
    options:
      show_bases: false
      members_order: source
      show_source: false

## Supporting models

::: datacoolie.core.models.SchemaHint
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.models.PartitionColumn
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.models.AdditionalColumn
    options:
      show_bases: false
      show_source: false

## Enums

::: datacoolie.core.constants.LoadType
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.constants.Format
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.constants.ConnectionType
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.constants.ProcessingMode
    options:
      show_bases: false
      show_source: false

::: datacoolie.core.constants.DataFlowStatus
    options:
      show_bases: false
      show_source: false
"""

with mkdocs_gen_files.open("reference/metadata-schema.md", "w") as fp:
    fp.write(CONTENT)
