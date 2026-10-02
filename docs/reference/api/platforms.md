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

## Secret provider extension hook

`BasePlatform` inherits the secret-provider contract. Platform implementations override the
focused provider hook below to fetch one secret from their native vault; the public `get_secret`
method retains caching and delegates to it.

::: datacoolie.core.secrets.provider.BaseSecretProvider._fetch_secret
