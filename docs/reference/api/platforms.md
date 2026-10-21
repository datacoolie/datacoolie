---
title: Platforms — Python API Reference | DataCoolie
description: Python API reference for the DataCoolie platforms package — BasePlatform, LocalPlatform, AWSPlatform, FabricPlatform, and DatabricksPlatform.
---

# Platforms

::: datacoolie.platforms.base
    options:
      members:
        - BasePlatform
        - FileInfo

::: datacoolie.platforms.local_platform
::: datacoolie.platforms.aws_platform
::: datacoolie.platforms.fabric_platform
::: datacoolie.platforms.databricks_platform

## Fabric lifecycle

`FabricPlatform.close()` releases its owned Azure root clients and internally
created credential once. Quiesce callers before close; overlapping service I/O
and close is unsupported. Close is terminal, including cached secret reads and
file operations. Injected credentials and native NotebookUtils handles remain
borrowed. Driver does not close a supplied platform.

## Secret provider extension hook

`BasePlatform` inherits the secret-provider contract. Platform implementations override the
focused provider hook below to fetch one secret from their native vault; the public `get_secret`
method retains caching and delegates to it.

::: datacoolie.core.secrets.provider.BaseSecretProvider._fetch_secret
