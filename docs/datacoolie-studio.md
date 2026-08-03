---
title: DataCoolie Studio — Complete Visual Guide
description: Complete DataCoolie Studio guide for projects, metadata, assets, lineage, sources, and ETL monitoring, with setup steps and screenshots.
---

# DataCoolie Studio — Visual Metadata, Lineage, and ETL Monitoring

**DataCoolie Studio** is the local-first visual companion to the DataCoolie ETL
framework. Use it to organize projects and environments, edit pipeline metadata,
trace lineage, inspect assets, connect storage, and investigate ETL runs from one
web interface.

[Explore DataCoolie Studio on GitHub](https://github.com/datacoolie/datacoolie-studio){ .md-button .md-button--primary }
[Run Studio locally](#run-studio-locally){ .md-button }

<figure markdown>
  <img src="../images/studio/home-projects.png"
       alt="DataCoolie Studio Projects workspace showing project readiness, environments, metadata, lineage, and monitoring coverage"
       width="1912" height="900" fetchpriority="high" decoding="async">
  <figcaption>Projects brings environment readiness and source coverage into one workspace.</figcaption>
</figure>

!!! info "Beta: install from source"
    DataCoolie Studio is currently beta software and requires Python 3.11 or
    newer. The supported beta path on this page installs the public GitHub
    source. Review the repository before using Studio with production metadata
    or shared infrastructure.

## DataCoolie and Studio work together

DataCoolie remains the execution framework. Studio makes its files, lineage,
and operational evidence easier to explore; it is not a replacement for an ETL
engine or a workflow orchestrator.

| Component | Responsibility |
|---|---|
| **DataCoolie** | Reads declarative metadata and executes ETL with Polars or Spark on local, Fabric, Databricks, or AWS runtimes. |
| **DataCoolie Studio** | Organizes projects and environments, edits metadata, derives assets and lineage, and visualizes ETL logs. |
| **Source of truth** | Your original JSON, YAML, XLSX, SQL, Python, and log files remain authoritative. Studio keeps only local application state, cache, and backups. |

Already have a DataCoolie project? Add its metadata to a Studio environment,
then attach logs and code artifacts when you need richer monitoring and lineage.

## Choose a screen by question

Use this map when you know the question you need to answer but not which Studio
page to open.

| Question | Open this page | What you get |
|---|---|---|
| Which projects and environments are ready? | [**Projects**](#1-projects-organize-workspaces) | Project readiness, environments, source coverage, and quick navigation. |
| What needs attention in this environment? | [**Overview**](#2-environment-overview-assess-readiness) | Metadata, lineage, monitoring, freshness, and next-action summaries. |
| How is this pipeline configured? | [**Metadata**](#3-metadata-inspect-and-edit-pipeline-definitions) | Connections, dataflows, schema hints, ordered transforms, and validation issues. |
| What tables, paths, and references exist? | [**Assets**](#4-assets-inventory-the-data-estate) | Searchable asset inventory, roles, provenance, dependencies, and unresolved references. |
| What is upstream or downstream of this asset? | [**Lineage**](#5-lineage-trace-relationships-across-files) | Interactive metadata, SQL, and Python relationships with run-status context. |
| Where do metadata, code, and logs come from? | [**Sources**](#6-sources-connect-metadata-code-and-logs) | Local or cloud bindings, read/cache status, validation, synchronization, and log refresh. |
| Is the environment healthy overall? | [**Monitoring · Overview**](#7-monitoring-overview-triage-environment-health) | Health KPIs, trends, runtime context, and attention signals. |
| Which job run failed or took too long? | [**Monitoring · Jobs**](#8-jobs-investigate-orchestrated-runs) | Job status, duration, stages, child flows, reconciliation, and drill-in evidence. |
| Which source-to-destination run has a problem? | [**Monitoring · Dataflows**](#9-dataflows-analyze-source-to-destination-execution) | Phase timing, workload, watermarks, and source/destination context. |
| Are failures repeating or spreading? | [**Monitoring · Failures**](#10-failures-find-repeated-causes-and-blast-radius) | Failure categories, signatures, blast radius, endpoints, and stages. |
| Is data stale or is a watermark stuck? | [**Monitoring · Freshness**](#11-freshness-inspect-age-and-watermark-movement) | Check age, stale signals, watermark coverage, movement, and skip streaks. |
| Where is runtime spent? | [**Monitoring · Performance**](#12-performance-locate-runtime-pressure) | Duration distributions, bottleneck phases, throughput, and optimization candidates. |
| Which workloads read or write the most? | [**Monitoring · Volume**](#13-volume-understand-workload-and-storage-change) | Rows, bytes, files, workload mix, storage deltas, and high-volume candidates. |
| Is table maintenance effective? | [**Monitoring · Maintenance**](#14-maintenance-verify-table-care-outcomes) | Maintenance health, reclaimed storage, removed files, and destination efficiency. |
| Can the monitoring evidence be trusted? | [**Monitoring · Diagnostics**](#15-diagnostics-validate-monitoring-evidence) | Job linkage, reconciliation, evidence completeness, and cache/source warnings. |
| How is Studio itself configured? | [**Settings**](#settings-configure-studio-itself) | Timezone, source checks, workspace database, caches, diagnostics, and capability modules. |

!!! note "Screenshot coverage"
    This guide includes all **15 official screenshots** committed in the
    DataCoolie Studio repository. They cover the complete screenshot-backed
    workflow shown with a populated local environment. Settings is documented
    from the shipped source but does not yet have an official screenshot. The
    capability-gated Master Data route is a placeholder, so it is not presented
    as an operational feature.

## What you can do in Studio

<div class="grid cards" markdown>

-   :material-table-edit: **Edit pipeline metadata**

    ---

    Work with JSON, YAML, and XLSX metadata in a structured editor. Studio
    validates changes and creates a backup before replacing the original file.

-   :material-source-branch: **Explore assets and lineage**

    ---

    Discover assets and trace upstream or downstream relationships inferred
    from metadata, SQL, and Python evidence without creating merged metadata.

-   :material-chart-timeline-variant-shimmer: **Investigate ETL operations**

    ---

    Review jobs, dataflows, failures, diagnostics, performance, volume,
    maintenance, and freshness from Dataflow, Job, and System logs.

-   :material-cloud-outline: **Connect where your project lives**

    ---

    Read from local storage, Amazon S3, MinIO, ADLS, OneLake, Google Cloud
    Storage, or Databricks. Cloud credentials use the operating-system
    credential store.

</div>

## Product tour and detailed guide

### 1. Projects — organize workspaces

The Projects page is the starting point. A project groups related environments;
each environment can point to different metadata, source code, and logs. Use
this page to compare readiness before opening an environment.

The screenshot at the top of this guide shows the Projects workspace. From
there you can:

- create, rename, or remove projects;
- add environments such as `dev`, `test`, or `prod`;
- see metadata, log, and code-source coverage;
- open project-level **Overview**, **Environments**, or **Reference mappings**;
- enter an environment workspace when its sources are ready.

Deleting a Studio environment removes its Studio settings and cached data; it
does not delete the original source files.

Project-level navigation has three focused pages:

| Project page | Use it for |
|---|---|
| **Overview** | Compare setup progress, environment coverage, and the next project-level action. |
| **Environments** | Create, rename, open, configure, or remove `dev`, `test`, `prod`, and custom environments. |
| **Reference mappings** | Review automatic, manual, and unresolved logical references and map them consistently across environments. |

### 2. Environment Overview — assess readiness

Open Overview first when you enter an environment. It combines source status,
metadata counts, lineage coverage, recent run health, and recommended next
actions so you can decide where to investigate.

<figure markdown>
  <img src="../images/studio/overview.png"
       alt="DataCoolie Studio Environment Overview with source readiness, metadata counts, lineage coverage, monitoring health, and next actions"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Environment Overview connects setup readiness, the data estate, operational health, and attention items.</figcaption>
</figure>

Use the four summary areas deliberately:

- **Sources** — confirm metadata, logs, and code are current.
- **Metadata and Lineage** — check enabled definitions, parse errors, coverage,
  and unresolved mappings.
- **Monitoring** — scan recent success rate and failures.
- **Attention and next actions** — follow the shortest route to the page that
  can resolve the issue.

### 3. Metadata — inspect and edit pipeline definitions

The metadata workspace presents connections, dataflows, schema hints, and
ordered transforms together. This helps reviewers understand a pipeline before
they run it and catches configuration issues closer to the source. Use search
and filters to narrow large metadata files, then select a definition to inspect
or edit its structured fields.

<figure markdown>
  <img src="../images/studio/metadata.png"
       alt="DataCoolie Studio metadata editor with connections, dataflows, schema hints, transforms, and validation issues"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Edit and validate source-defined metadata without hiding the original files.</figcaption>
</figure>

Before saving, verify the selected source and any reported issues. Studio
validates the document and creates a backup before it replaces the original
file. Revision-aware saves also protect against silently overwriting a newer
source revision.

### 4. Assets — inventory the data estate

Assets turns connection and dataflow evidence into a searchable inventory. Use
it when you need to find a table or path, understand whether it acts as a source
or destination, or locate unresolved SQL/Python references.

<figure markdown>
  <img src="../images/studio/assets.png"
       alt="DataCoolie Studio Assets inventory with asset type, connection, usage, lineage, dependencies, attention state, and provenance"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Assets combines inventory, usage, dependency, attention, and provenance evidence.</figcaption>
</figure>

Useful filters include connection, format, type, role, and attention state. Open
an asset when you need its source definitions, observations, consumers, or
reference-resolution context.

### 5. Lineage — trace relationships across files

Studio combines references found in metadata, SQL, and Python for display. Use
filters and upstream or downstream traversal to focus on the part of the graph
you are investigating. Run-status context helps connect a structural
relationship to recent operational evidence.

<figure markdown>
  <img src="../images/studio/lineage.png"
       alt="DataCoolie Studio interactive lineage graph with upstream and downstream asset relationships"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Lineage connects source definitions and code evidence while leaving source files authoritative.</figcaption>
</figure>

Start from a known asset when possible. Expand upstream to investigate where
data came from, downstream to assess impact, or both when reviewing a change.
Reference mappings can resolve logical references that Studio cannot match
automatically across environments.

### 6. Sources — connect metadata, code, and logs

Sources is where an environment learns what to read. Add a local path or cloud
URI, test access, scan a project, and synchronize the materialized cache. Each
source reports whether it is enabled, readable, and current.

<figure markdown>
  <img src="../images/studio/sources.png"
       alt="DataCoolie Studio Sources page with local and cloud bindings for metadata, Python code, and ETL logs plus read and cache status"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Sources keeps metadata, code, and logs separate while exposing validation, synchronization, and refresh state.</figcaption>
</figure>

Use metadata sources for DataCoolie definitions, code sources for referenced
Python functions and dependency analysis, and log sources for Monitoring. Log
sources can refresh on a schedule; metadata and code are checked when the app
navigates or returns to the foreground.

Choose the provider that matches where the source lives:

| Platform or location | Studio provider | Installation |
|---|---|---|
| Local filesystem | Local | Base installation |
| AWS | Amazon S3 | `.[s3]` |
| S3-compatible storage | MinIO | `.[minio]` |
| Microsoft Fabric | OneLake or ADLS | `.[onelake]` or `.[adls]` |
| Google Cloud | GCS | `.[gcs]` |
| Databricks | Databricks | Base installation |

Cloud credentials are stored through the operating-system credential store.
Use **Test connection** before scanning, then validate and synchronize the
source. The source remains authoritative; Studio reads and caches it for the
workspace.

## Monitoring — choose the focused operational page

The monitoring workspace turns DataCoolie logs into operational views for run
health, failures, diagnostics, duration, data volume, maintenance, and
freshness. Logs are optional, so you can start with metadata and add monitoring
evidence later.

All Monitoring pages share the same command bar. Set the date range and grain,
then filter by status, operation, stage, connection, or search text. Moving
between tabs preserves the environment context so you can follow an issue from
summary to evidence.

### 7. Monitoring Overview — triage environment health

Use Overview at the start of an operational review. It answers whether jobs and
dataflows are succeeding, how run health is trending, and which runtime context
or phase deserves attention.

<figure markdown>
  <img src="../images/studio/monitoring-overview.png"
       alt="DataCoolie Studio monitoring overview with job health, dataflow status, failures, performance, and volume signals"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Use the overview for triage, then move into focused monitoring pages for investigation.</figcaption>
</figure>

Look first at health KPIs and attention signals, then follow the related Jobs,
Dataflows, Failures, Performance, Maintenance, Freshness, or Diagnostics page.

### 8. Jobs — investigate orchestrated runs

Jobs summarizes parent runs and their child-dataflow impact. Use it to compare
job success rate and duration, identify failed stages, and open the evidence for
a specific run.

<figure markdown>
  <img src="../images/studio/monitoring-jobs.png"
       alt="DataCoolie Studio Monitoring Jobs page with job KPIs, status trend, duration distribution, workload efficiency, fan-out, and job-run table"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Jobs combines run-level health, runtime context, stage impact, duration, fan-out, and drill-in evidence.</figcaption>
</figure>

Use the run table to move from aggregate symptoms to a single job ID. Compare
its stages, child-flow counts, volume, reconciliation state, and issue message.

### 9. Dataflows — analyze source-to-destination execution

Dataflows is the most detailed execution view for individual pipeline steps. It
separates source, transform, destination, and overhead phases while retaining
source/destination, volume, watermark, status, and parent-job context.

<figure markdown>
  <img src="../images/studio/monitoring-dataflows.png"
       alt="DataCoolie Studio Monitoring Dataflows page with execution KPIs, phase timing, route health, workload, watermarks, and dataflow runs"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Dataflows helps isolate the slow or failing phase and the affected source-to-destination route.</figcaption>
</figure>

Use this page when a job summary is too broad. Filter to the dataflow, route, or
stage, then compare phase contribution and the run-level evidence.

### 10. Failures — find repeated causes and blast radius

Failures groups operational errors instead of forcing you to read every run
individually. Use it to spot repeated signatures, the most affected endpoints,
and whether the issue is concentrated in one phase or stage.

<figure markdown>
  <img src="../images/studio/monitoring-failures.png"
       alt="DataCoolie Studio Monitoring Failures page with failure KPIs, incident queue, trends, repeated signatures, endpoint impact, and failing stages"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Failures prioritizes the newest incidents while exposing repeated causes and their operational reach.</figcaption>
</figure>

Start with the latest failed-dataflow queue. If a signature repeats, use its
category, phase, route, and stage distribution to determine whether one fix can
remove several incidents.

### 11. Freshness — inspect age and watermark movement

Freshness answers whether expected data is arriving and advancing. It combines
source check age, stale-data signals, consecutive skips, watermark coverage,
and watermark movement.

<figure markdown>
  <img src="../images/studio/monitoring-freshness.png"
       alt="DataCoolie Studio Monitoring Freshness page with stale dataflows, check age, watermark coverage, movement, adjustments, and skip streaks"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Freshness separates stale checks from watermark configuration and movement problems.</figcaption>
</figure>

Check whether a dataflow is genuinely stale, merely unobserved, or missing a
watermark configuration. Use the registry to follow a specific dataflow after
the aggregate charts identify an age band, stage, or skip streak.

### 12. Performance — locate runtime pressure

Performance breaks duration into phases and runtime contexts. Use it to find
slow profiles, outliers, throughput pressure, and candidates where optimization
work is most likely to matter.

<figure markdown>
  <img src="../images/studio/monitoring-performance.png"
       alt="DataCoolie Studio Monitoring Performance page with duration distribution, phase cost, trends, workload efficiency, runtime profiles, and optimization candidates"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Performance links duration and throughput symptoms to phases, dataflows, engines, platforms, and providers.</figcaption>
</figure>

Use P50 for typical behavior, P95 for tail latency, and outliers for exceptional
runs. Compare source, transform, destination, and overhead contribution before
changing engine, compute, or storage settings.

### 13. Volume — understand workload and storage change

Volume tracks rows, bytes, files, and row changes across the selected period.
Use it to find high-volume dataflows, unusual storage deltas, file churn, or a
route whose read and write behavior no longer matches expectations.

<figure markdown>
  <img src="../images/studio/monitoring-volume.png"
       alt="DataCoolie Studio Monitoring Volume page with rows, bytes, files, storage delta trends, workload mix, routes, and high-volume dataflows"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Volume connects workload trends and storage impact to operations, routes, and individual dataflows.</figcaption>
</figure>

Read rows and estimated rows written together. For lakehouse destinations, also
review bytes added or removed and files changed so a small logical update does
not hide disproportionate physical churn.

### 14. Maintenance — verify table-care outcomes

Maintenance focuses on optimize, vacuum, and related destination operations.
Use it to confirm coverage, identify no-op or slow work, and compare time spent
with storage reclaimed or files removed.

<figure markdown>
  <img src="../images/studio/monitoring-maintenance.png"
       alt="DataCoolie Studio Monitoring Maintenance page with operation health, trends, bytes reclaimed, files removed, table efficiency, and destination registry"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Maintenance shows whether table-care work is healthy and whether its runtime produces useful storage outcomes.</figcaption>
</figure>

Use the destination registry to find lagging or low-efficiency tables. A long
maintenance run with little reclaimed storage can be a scheduling or policy
candidate rather than an engine-performance issue.

### 15. Diagnostics — validate monitoring evidence

Diagnostics checks the evidence behind the other Monitoring pages. Use it when
metrics look incomplete, a job has no child dataflows, reconciliation does not
match, or a log source/cache warning may explain missing results.

<figure markdown>
  <img src="../images/studio/monitoring-diagnostics.png"
       alt="DataCoolie Studio Monitoring Diagnostics page with core integrity, job linkage, reconciliation, evidence coverage, log sources, and investigation queue"
       width="1912" height="900" loading="lazy" decoding="async">
  <figcaption>Diagnostics makes evidence quality explicit before you trust or compare operational metrics.</figcaption>
</figure>

Resolve core-integrity and linkage problems before interpreting higher-level
KPIs. Evidence coverage distinguishes required fields from conditional fields,
helping you decide whether a missing metric is a logging-version problem or a
valid absence of evidence.

## Settings — configure Studio itself

Settings is global rather than environment-specific. Use it to review or change:

- Studio timezone and the source of that timezone;
- adaptive source-check intervals and failure-pause behavior;
- workspace database backend, location, size, and compaction;
- disposable result and analytics caches, including clear, optimize, and retry
  operations;
- diagnostic information and the `/api/v1` backend prefix;
- capability modules exposed in navigation.

Cache actions target disposable derived data. Read confirmation text carefully
before maintenance actions, especially on a shared database.

## Recommended investigation workflow

1. Open **Overview** and identify the affected area.
2. Confirm metadata, code, and log currency in **Sources**.
3. Use **Metadata** or **Assets** to find the definition and affected object.
4. Trace impact in **Lineage**.
5. Triage run health in **Monitoring Overview**.
6. Move to the focused Monitoring page that matches the symptom.
7. Check **Diagnostics** before concluding that missing evidence means healthy
   or failed behavior.

## Run Studio locally

Clone the public repository and create an isolated Python environment:

=== "Windows PowerShell"

    ```powershell
    git clone https://github.com/datacoolie/datacoolie-studio.git
    cd datacoolie-studio
    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install -e .
    datacoolie-studio
    ```

=== "macOS or Linux"

    ```bash
    git clone https://github.com/datacoolie/datacoolie-studio.git
    cd datacoolie-studio
    python3.11 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip
    python -m pip install -e .
    datacoolie-studio
    ```

The launcher creates the local workspace on first run, starts Studio at
`http://127.0.0.1:8765`, and opens it in your browser. Because it binds to the
loopback interface by default, other machines cannot connect unless you change
the host setting.

### Add only the cloud integrations you need

Install an extra from the cloned repository before starting Studio:

```bash
python -m pip install -e ".[s3]"       # Amazon S3
python -m pip install -e ".[minio]"    # MinIO
python -m pip install -e ".[adls]"     # Azure Data Lake Storage
python -m pip install -e ".[onelake]"  # Microsoft OneLake / Fabric
python -m pip install -e ".[gcs]"      # Google Cloud Storage
python -m pip install -e ".[cloud]"    # All optional cloud providers above
```

Databricks SDK support is included in the base installation.

### Common launcher options

```powershell
datacoolie-studio --port 8765
datacoolie-studio --host 127.0.0.1
datacoolie-studio --db .\.scratch\studio.db
datacoolie-studio --database-url "postgresql+psycopg://user:password@host:5432/datacoolie_studio"
datacoolie-studio --no-open
```

Use `--no-open` when another process manages the browser. Keep the default
loopback host unless you have reviewed authentication, network access, database
sharing, and source permissions.

### Local workspace

By default, Studio stores its own state under
`~\.datacoolie\datacoolie-studio\`:

```text
db\studio.db
backups\
cache\
logs\
```

These paths contain Studio state, backups, and disposable derived data. They do
not replace the original metadata, code, or ETL logs you configured as sources.

## Create your first workspace

1. Create a **Project**.
2. Add an **Environment**, such as `dev`, `test`, or `prod`.
3. Add a metadata file or scan a DataCoolie project.
4. Optionally add ETL logs for Monitoring.
5. Optionally add Python code artifacts referenced by metadata.
6. Open **Metadata**, **Assets**, **Lineage**, or **Monitoring**.

Metadata is required. Logs and code artifacts are optional, so you can adopt
Studio incrementally.

## Troubleshooting the first workspace

| Symptom | Check |
|---|---|
| Metadata page is empty | Confirm at least one metadata source is enabled, readable, synchronized, and points to JSON, YAML, XLSX, or a project metadata folder. |
| Lineage is incomplete | Add referenced SQL/Python code, review unresolved references in Assets, and configure project reference mappings where automatic resolution is ambiguous. |
| Monitoring has no evidence | Add the correct ETL and System log paths, validate them, synchronize the source, and confirm the selected date range includes runs. |
| Cloud source cannot be read | Install the matching provider extra, save the credential through Studio, test the connection, and verify URI and provider permissions. |
| Operational metrics look incomplete | Open Monitoring · Diagnostics and check linkage, evidence coverage, read/cache warnings, and log-source currency. |
| Browser does not open | Start with `datacoolie-studio --no-open`, then open `http://127.0.0.1:8765` manually. |

!!! warning "Before sharing Studio on a network"
    Studio is local-first and binds to `127.0.0.1` by default. Before changing
    the host or serving multiple users, review authentication, network access,
    the database configuration, and permissions for every connected source.

## Frequently asked questions

### Does Studio run my ETL pipelines?

DataCoolie executes the pipelines. Studio currently focuses on project setup,
metadata, assets, lineage, sources, and monitoring evidence. Use your existing
scheduler or orchestrator to decide when ETL jobs run.

### Does Studio overwrite my metadata?

The original metadata remains the source of truth. When you save from the
editor, Studio validates the document and creates a backup before replacing the
file.

### Can Studio work with Fabric, Databricks, and AWS?

Yes. OneLake and ADLS cover Microsoft Fabric storage, Databricks support is
included in the base installation, and the `s3` extra supports Amazon S3. The
Studio repository also provides MinIO and Google Cloud Storage integrations.

## Continue exploring

- [Install the DataCoolie ETL framework](getting-started/installation.md)
- [Understand the DataCoolie metadata model](concepts/metadata-model.md)
- [Learn how DataCoolie logging is structured](operations/logging-layout.md)
- [View the Studio source and report issues](https://github.com/datacoolie/datacoolie-studio)
