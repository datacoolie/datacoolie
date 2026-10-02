---
title: Metadata identity and overlays — topic links
description: Routes older metadata identity and overlay links to their current configuration guides.
---

# Metadata identity and overlays

Configuration guidance now sits with its owner:

- [Metadata document](index.md#metadata-document) for `$schema`, root fields and `extensions`.
- [Connections](connections.md#connection-identity) for connection IDs.
- [Dataflows](dataflows.md#dataflow-identity) for dataflow IDs.
- [Datatypes and schema hints](data-types.md#global-hints-for-a-source-table) for shared-hint identity.
- [Project environment overlays](../cli/project.md#environment-overlays) for preparation.

## Add the schema marker when tools need it

See [Choose a schema marker](index.md#choose-a-schema-marker).

## Use names as the normal identity

See [Connection identity](connections.md#connection-identity) and
[Dataflow identity](dataflows.md#dataflow-identity).

## Use explicit IDs only for stable external identity

See [Connection identity](connections.md#connection-identity) and
[Dataflow identity](dataflows.md#dataflow-identity).

## Keep shared schema hints name-first

See [Global hints for a source table](data-types.md#global-hints-for-a-source-table).

## Add project-owned extensions without changing framework behavior

See [Add project-owned extensions](index.md#add-project-owned-extensions).

## Use an environment overlay for prepared project variants

See [Project environment overlays](../cli/project.md#environment-overlays).

## Validate identity and preparation changes

Use the [Validation checklist](validation-checklist.md).
