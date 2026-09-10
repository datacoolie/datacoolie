# Dataflow Orchestration

Read when selecting stages, authoring dependencies, sizing concurrency, or exposing job parameters.
This reference owns execution guidance; `runner-contract.md` owns parameter transports.
Verified against the framework source on 2026-09-09. Recheck the installed version before assuming
different scheduling, cancellation, or resource limits.

Stage values are project-defined runtime selections. Use the user's metadata and dependency
contracts to identify upstream and downstream work; do not hardcode stage names, layer names,
their count, or their order. Names and numbers below illustrate arrangements, not reserved values.

## Choose the smallest execution arrangement

| Situation | Arrangement |
|---|---|
| Independent dataflows, one job | Omit `group_number` and `execution_order`; use default `job_num=1`, `job_index=0`. |
| Independent dataflows, multiple jobs | Keep group/order absent; invoke the same runner with `job_num=N` and each `job_index=0..N-1`. |
| A consumer must wait for producers in one normal ETL call | Give the producers and consumer the same non-null group; producers have lower orders. |
| Several independent dependency chains | Use one group per chain; orders express dependencies inside each group. |
| Several dependent stages | Prefer separate stage invocations, with completion and quality checks between stages. |
| Combined stages in one normal ETL call | Encode every required in-call dependency with shared groups and increasing orders. |
| Dependencies across jobs or runner invocations | Use an external orchestration barrier; metadata groups cannot wait across invocations. |

Do not add groups/orders merely because there are many flows or several stage names. Group only
when co-location, dependency ordering, or an explicit serialization requirement needs it. Runtime
does not infer dependencies from source/destination tables, SQL, lineage, or file ordering.

## Single job and scale-out

`job_num` is the total number of job shards; `job_index` identifies this invocation's shard.
`DataCoolieRunConfig` defaults to `1` and `0`. A single-job caller can omit both parameters and still
run dataflows concurrently using `max_workers`.

For scale-out, an external scheduler starts N invocations. Setting `job_num=N` alone only filters
the work of the current invocation; it does not launch other jobs or wait for them.

All shards of one run must use the same framework/metadata snapshot, environment, stage selection,
and N, with one invocation per index. The framework validates `N >= 1` and `0 <= index < N`.
It does not detect missing indexes, duplicate invocations, or overlapping scheduler runs. Reusing
an index can repeat writes; omitting an index leaves its assigned work unprocessed. An empty shard
is valid. Shared environment watermark state stays keyed by dataflow; do not create separate
watermark histories per job index just because job count can change.

Assignment when N > 1:

| Dataflow metadata | Owning index |
|---|---|
| Non-null `group_number` (including `0`) | `group_number % N` |
| `group_number=None` | `int(MD5(str(dataflow_id).encode('utf-8')).hexdigest(), 16) % N` |

Assignment is deterministic, not random and not load-balanced by duration or data volume. The same
ID/group and N yield the same index. Different groups may land on the same index. Changing N or a
group may move work; stop the previous shard set before starting a differently partitioned run.
Preserve dataflow identities, since they also identify incremental state.

Every member of a group is kept on one job. One large group therefore cannot use all N jobs;
additional jobs help only when enough independent flows/groups and execution resources exist.
Scale from measured runtime and source/destination capacity, not flow count alone. Sharding is
work allocation, not exactly-once processing or protection against concurrent writes to one target.

## Ordering inside one normal ETL call

`driver.run()` / `run_dataflow()` use these rules after selection:

| Group | Order | Behavior |
|---|---|---|
| `None` | absent / `None` | Independent task; eligible for parallel execution. |
| `None` | specified | Still independent. Order may affect submission sequence but creates no wait/completion guarantee. |
| Same non-null group | different values | Complete the lower-order bucket before starting the next bucket. |
| Same non-null group | equal values | Eligible for parallel execution in the same bucket. |
| Same non-null group | `None` and `0` | Both belong to order `0`; they may overlap. |
| Different non-null groups | any values | Independent, even on the same job; smaller group numbers have no priority/barrier guarantee. |

For a chain A then B, assign both group 10 and orders 10/20. For fan-in A and B then C, put all
three in group 10: A/B order 10, C order 20. For fan-out A then B and C, use A order 10, B/C order
20. A separate chain may use group 11 and run alongside group 10.

Groups implement ordered batches, not arbitrary dependency graphs. Every flow in order 20 waits
for the entire order-10 bucket, including unrelated members. A flow has one group, so a consumer
cannot express a join across two independent groups. Put the required connected dependency set
in one group or use an external barrier. Do not duplicate a producer in several groups.

With `max_workers=1`, work in a job is serialized, but incidental submission order is still not a
dependency contract. Explicit group/order metadata remains necessary if later concurrency changes
must preserve dependencies.

## Stage selection and completion gates

Prefer separate invocations for dependent upstream and downstream stages to isolate retries,
validation, and operational control. Independent flows within a stage can remain ungrouped.
Dependencies within that stage still need ordering if they must use outputs produced by the
current run. Apply this to any project-defined stage graph, including branches and joins.

The API accepts `stage=[upstream_stage, downstream_stage]` or a comma string containing the selected
names. Both select a union of flows; the list's order provides no stage barrier. The generated CLI
passes a single comma string unchanged. An omitted stage also selects across stages and needs the
same dependency analysis.

For example, a project might name stages `source2bronze`, `bronze2silver`, and `silver2gold`;
another might use `ingest`, `normalize`, and `publish`. Neither vocabulary implies execution order.
In a combined selection, an orders producer and its downstream consumer can use group 10/orders
10 and 20. A separate customers chain can use group 11/orders 10 and 20.
This lets each chain advance independently; it does not wait for every upstream flow before any
downstream flow starts. A consumer joining orders and customers needs both producers in its group or
a completed upstream stage barrier.

For stage-by-stage scale-out, the external orchestrator launches every upstream shard, waits for
all of them, checks failures and required quality evidence, then launches the downstream stage's
shards. An upstream job finishing its own shard is insufficient evidence for the whole stage.
The next stage may use a different N after that barrier. Normal generated runners perform one
driver call; external orchestration owns repeated invocations.

Selection does not expand to include prerequisites. A consumer selected alone will run even if a
lower-order producer is inactive, filtered out, or absent. Existing upstream data is acceptable
only when the project's freshness/completeness contract allows it.

## Concurrency and failure limits

`max_workers` controls each Python thread pool, not job count or Spark executor count. The normal
ETL executor has an outer pool for groups/independent tasks and an inner pool for each multi-item
order bucket. Thus multiple groups can exceed `max_workers` active dataflows in one job: with
`max_workers=2`, two groups with two tied flows each can execute four flows concurrently. Engine
threads, API pagination, and external jobs add their own concurrency. The framework run-config
default is 8; templates may choose a smaller explicit default.

Ordering normally waits for completion, not success. With the framework default
`stop_on_error=False`, a later bucket can run after a producer fails. For dependent chains, use
`stop_on_error=True` (the bundled runners do): a failed dataflow prevents later buckets in that
group. Other groups and ungrouped flows keep running; this is not global fail-fast. A skipped
producer is not treated as failed and does not block the next bucket.

Already-running work cannot be undone by cancelling futures. The executor attempts to cancel
queued peers in a failing bucket, and its pool waits for running peers. Aggregated counters after
early stop may not represent every completed peer; use per-dataflow evidence for reconciliation.
Retries are per dataflow; the group waits for the retry outcome before advancing. Stage gates
must check the returned result and the project's required freshness/quality evidence, rather than
treating successful return from `run()` as successful execution of every prerequisite.

## Selection and other operations

- `run(stage=...)` loads active flows and applies job assignment. Passing `run(dataflows=...)`
  bypasses loading, active filtering, job assignment, and any accompanying `stage` selection;
  normal group/order scheduling still applies to the supplied list. Use
  `load_dataflows(stage=...)` first when selection and assignment are needed.
- `run_replay(dataflows=...)` uses flat parallel execution across the supplied flows, ignoring
  group/order sequencing; chunks within one flow are sequential. The replay templates call
  `load_dataflows` first, so job assignment still applies. Replay dependent stages separately.
  For dependencies within one stage, explicitly select and replay the prerequisite set, check
  completion/success, then replay its consumers; a shared group cannot enforce replay ordering.
- `run_maintenance(connection=...)` deduplicates physical destinations before job assignment, then
  runs flat parallel maintenance. With explicit `dataflows=...`, it deduplicates but does not shard
  the list again. Group/order do not schedule maintenance dependencies.
- Flat execution (replay/maintenance) currently does not stop other flows on a normal returned
  `failed` status, even with `stop_on_error=True`. A failed replay chunk stops that flow's remaining
  chunks. Use the operation's result and external gates for downstream progression.

## Evidence to verify when the runtime changes

In the framework repository, inspect `src/datacoolie/orchestration/{driver,job_distributor,
parallel_executor}.py` and `src/datacoolie/core/{models,constants}.py`. Runtime characterization
tests live under `tests/unit/orchestration/`; runner transport checks live under
`ai/skills/tests/unit/`. These are repository verification paths, not generated-project imports.
Python's [Future cancellation and executor shutdown contract](https://docs.python.org/3/library/concurrent.futures.html)
explains why cancelling queued futures cannot interrupt work already executing.
