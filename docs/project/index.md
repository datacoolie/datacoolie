---
title: DataCoolie project resources
description: Contributing, architecture decisions, releases and public project resources for DataCoolie.
---

# Project resources

Use these pages when contributing to DataCoolie or checking the evidence behind
a public decision. To run your first pipeline, start with
[getting started](../guide/getting-started/index.md).

| Your task | Start here | Check before you finish |
|---|---|---|
| Edit docs or public API docstrings | [Contributor setup and docs workflow](contributing.md#documentation-workflow) | Strict build, search metadata and rendered links |
| Change framework code or packaging | [Choose contributor checks](contributing.md#choose-checks-for-your-change) and [testing strategy](testing.md) | The affected tests actually execute; explain skips and unverified cells |
| Validate a planned failure | [Expected-failure scenarios](expected-failure-scenarios.md) | Expected child exit, configured assertions and scenario receipt |
| Compare local engine workloads | [Benchmark reproduction](benchmarks.md#running) | Complete successful JSON cases and comparable run provenance |
| Prepare a package release | [Release verification](contributing.md#release-verification) | Every local release-gate stage passes |
| Understand a public design contract | [Architecture decisions](decisions/index.md) | Current consumer contract and linked source/test owner |

For companion application workflows, see [DataCoolie Studio](../studio/index.md).
The [blog](../blog/index.md) provides background and dated examples. The internal
workspace wiki holds engineering rationale; public tasks must be usable through
the docs and repository sources linked here.

The public docs build is configured by `properdocs.yml` and verified with
`properdocs build --strict`. Generated schema and API pages must be fixed at
their source generator or docstring, not edited in the generated output.
