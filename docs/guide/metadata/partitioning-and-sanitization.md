---
title: Partitioning and column sanitization — topic links
description: Routes older partitioning and sanitization links to the owning configuration guides.
---

# Partitioning and column sanitization

Configure [destination partitions](destination-and-load-patterns.md#partition_columns-partition-the-output-table)
and [column-name sanitization](transform-patterns.md#columnnamesanitizer)
in their owning pages. The combined
[stable keys and protected output](stable-keys-and-protected-output.md)
case shows how later transforms preserve merge and partition identity.

## Declarative partition columns

See [Destination partitioning](destination-and-load-patterns.md#partition_columns-partition-the-output-table).

## SQL expression portability

See [Partition expression portability](destination-and-load-patterns.md#partition-expression-portability).

### Polars limitations to know

See [Partition expression portability](destination-and-load-patterns.md#partition-expression-portability).

## Column name sanitization

See [ColumnNameSanitizer](transform-patterns.md#columnnamesanitizer).

## Gotchas

Review the [destination's common mistakes](destination-and-load-patterns.md#common-mistakes)
and [transform's common mistakes](transform-patterns.md#common-mistakes).
