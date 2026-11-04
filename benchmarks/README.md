# Benchmark results and experiment log

Results recorded through **November 4, 2026**. All money amounts are USD
list-price estimates, not invoices or total project spending.

## At a glance

| Experiment | Status | Main result |
|---|---|---|
| Original Q1 pipeline | One observed successful run | 707.270 s total job duration; $0.145978 attributed compute estimate |
| Frozen Q1 query baseline | Four queries, three measured repetitions each | Repeatable results with SQL result caching disabled |
| Standard vs Performance Optimized | Three pipeline replays per mode; all output audits passed | Median job duration 558.317 s vs 233.703 s; estimated compute $0.139400 vs $0.330759 |
| Full vs incremental Gold | Three March updates per strategy; all output audits passed | Median Gold time 11.304 s vs 32.667 s; estimated job compute $0.114243 vs $0.173948; incremental wrote 63.4% fewer data-file bytes |
| SQL warehouse 2X-Small vs X-Small | Three sessions per size, nine measured repetitions per query; all results matched | Modest, mixed latency differences; billing observed, but hourly records and uneven shutdowns prevent clean cost attribution |
| Isolated SQL sizing rerun | Six fresh warehouses, ten-minute windows; 8,504 measured queries | Median throughput 141.815 vs 139.064 queries/min; full-sample billing pending |
| Partitioned vs liquid-configured Q1 copies | Exact row audits and 96 query profiles passed | Clustering skipped on write; `OPTIMIZE FULL` planned zero physical clustering tasks; weekly scans unchanged |

**Current finding:** Performance Optimized had **58.14% lower median total job
duration**, but only **5.44% lower median pipeline-call duration**. Most of the
observed difference was outside pipeline execution. Its observed median compute
estimate was **2.37 times** Standard's, not 58% cheaper.

For Gold, the incremental implementation reduced data-file writes but was
**2.89 times slower**, with **52.3% higher median job compute cost**, at this scale. Production remains on Standard compute
and full Gold rebuilds.

## 1. Shared Q1 dataset and storage

The dataset includes source files for **January through March 2024**, selected by
`source_month`, not by pickup timestamp. Out-of-range pickup dates present in
those files are retained under the existing quality rules.

| Metric | Value |
|---|---:|
| Input / Bronze rows | 9,554,778 |
| Valid Silver rows | 9,551,977 |
| Quarantined rows | 2,801 |
| Original source Parquet bytes | 160,389,205 |
| Active frozen Delta data bytes | 988,500,178 |
| Active frozen Delta files | 13 |

| Frozen table suffix | Active data bytes | Files |
|---|---:|---:|
| `bronze` | 473,431,449 | 3 |
| `trips` (Silver) | 512,970,251 | 3 |
| `quarantine` | 182,561 | 3 |
| `hourly_zone_demand` | 1,868,619 | 1 |
| `monthly_fares` | 2,504 | 1 |
| `airport_activity` | 41,144 | 1 |
| `trip_patterns` | 3,650 | 1 |

Frozen tables use the prefix `nyc_taxi_dev.benchmark.q1_2024_`. Bronze, Silver,
and quarantine are partitioned by `source_month`; Gold is unpartitioned.
There is no clustering. The snapshot materialized data from source Delta
versions **6** into separate tables at version **0**; it is not an exact copy
of the original physical layout.

Storage counts exclude transaction logs, obsolete files, soft deletes,
production copies, and subsequent experiment copies. They are not total Azure
storage usage.

Evidence: [baseline_q1_2024.json](baseline_q1_2024.json), including source
checksums and frozen table versions.

## 2. Original pipeline observation

Job run: `264596207956120`. Compute: **serverless Jobs, Standard mode**.
This is one operational observation, not a repeated controlled experiment.

| Metric | Value |
|---|---:|
| Processed input rows | 9,554,778 |
| Full job duration | 707.270 s (11 min 47 s) |
| Internally timed processing | 198.498 s |
| Approximate full-job throughput | 13,509 rows/s |
| Approximate timed-processing throughput | 48,135 rows/s |
| Attributed DBUs | 0.3105915139 |
| Attributed list-price compute estimate | $0.145978 |
| Estimate per million processed input rows | $0.015278 |

Stage times are shown alongside the new experiment in section 4. They do not
cover every pipeline operation and cannot allocate the compute bill by layer.
The internal processing timer excludes earlier initialization, compute setup,
and final metrics persistence.

The cost applies to this entire attributed job run, not only Silver and Gold.
It excludes storage, discounts, credits, and taxes. Serverless infrastructure is
already included in its compute pricing; do not add a separate VM charge.

## 3. Controlled SQL query baseline

Measured **October 28, 2026** on a dedicated **2X-Small serverless SQL warehouse**,
Photon enabled, one cluster, CURRENT channel, SQL engine version **2026.43**.
This warehouse is separate from the serverless Jobs compute used for pipeline
processing.

Each query received **one warmup and three measured repetitions**: 16 statements
total, of which 12 contributed to the medians. SQL result caching was disabled
and verified in a persistent session. The I/O cache was warm, not cleared.

| Query | Layer | Median client time (ms) | Median engine total (ms) | Median execution (ms) | Median read bytes | Read / pruned files |
|---|---|---:|---:|---:|---:|---:|
| `01_monthly_fares` | Silver | 708.079 | 576 | 382 | 67,903,788 | 3 / 0 |
| `02_weekly_zone_demand` | Silver | 611.647 | 483 | 293 | 19,154,764 | 1 / 2 |
| `03_airport_corridors` | Silver | 575.789 | 436 | 257 | 52,348,627 | 3 / 0 |
| `04_gold_demand` | Gold | 410.221 | 254 | 128 | 1,868,619 | 1 / 0 |

All repetitions returned consistent results, and every statement had a collected
query profile. Median remote-read bytes and spill bytes were zero; the reads
were served from warm I/O cache.

The observed warehouse-window compute estimate was **$0.100916**, representing
**0.1441655556 DBU**. This allocation is **PROVISIONAL**: overlapping usage can
include startup, idle time, and metadata operations. It is not exact per-query
cost and excludes later report-refresh sessions.

These are different queries, so their times do **not** establish a
Silver-versus-Gold speedup. Three measured samples do not justify p95 claims.

## 4. Standard vs Performance Optimized pipeline experiment

Measured **October 30, 2026**. Experiment: `perf_20261030_a`.
Job: `824241423958859`.

Both configurations used **serverless Jobs compute**, the same pipeline code,
and environment version **4**. We changed `performance_target`, not an
explicit VM type or worker count.

Standard prioritizes cost efficiency; Performance Optimized prioritizes fast
startup and execution. Databricks manages the underlying resources. Both modes
use the same billing SKU, but billable DBU consumption can differ. This was not
a classic-cluster worker-sizing or SQL warehouse experiment.

### Method

The same checksum-verified Q1 Parquet files and zone lookup were copied once
from Azure landing into an isolated experiment input directory. Every measured
sample started with new, empty schemas and rebuilt Bronze, Silver, quarantine,
and Gold. Each processed **9,554,778 input rows** and downloaded **zero new
source bytes**.

Live source discovery and internet downloads were excluded. Runs were serial
and interleaved in this order: Standard 1, Performance Optimized 1, Performance
Optimized 2, Standard 2, Standard 3, Performance Optimized 3. Physical serverless
cache state was not forcibly cleared, so this is not a cold-cache experiment.

Preparation and the separate full output audit are excluded from the measured
runtime/cost comparison. No measured sample required a task retry.

### All six measurements

| Sample | Job run ID | Total job (s) | Pipeline call (s) | Internal processing (s) |
|---|---|---:|---:|---:|
| Standard 1 | `439231191932953` | 558.317 | 218.018 | 207.554 |
| Performance Optimized 1 | `264342436179684` | 230.955 | 194.439 | 184.343 |
| Performance Optimized 2 | `54071163987905` | 233.703 | 198.774 | 189.153 |
| Standard 2 | `391012681294518` | 545.009 | 199.385 | 188.490 |
| Standard 3 | `872158117752671` | 615.588 | 210.211 | 198.637 |
| Performance Optimized 3 | `467317722416005` | 250.306 | 209.718 | 198.716 |

### Summary

| Metric | Standard | Performance Optimized |
|---|---:|---:|
| Measured repetitions | 3 | 3 |
| Minimum total job duration | 545.009 s | 230.955 s |
| Median total job duration | 558.317 s | 233.703 s |
| Maximum total job duration | 615.588 s | 250.306 s |
| Median pipeline-call duration | 210.211 s | 198.774 s |
| Median internally timed processing | 198.637 s | 189.153 s |
| Median full-job throughput | 17,114 rows/s | 40,884 rows/s |
| Median compute cost | $0.139400 | $0.330759 |
| Median cost per million input rows | $0.014590 | $0.034617 |

Timing definitions matter:

- **Total job:** Databricks job start-to-end time, including startup, schema
  setup, input verification, and metrics persistence.
- **Pipeline call:** Time around the pipeline function, including its
  initialization and operational report writes.
- **Internal processing:** The pipeline's own timer, excluding earlier
  initialization and final metrics persistence.

### Layer processing times

| Stage | Original Standard observation (s) | New Standard median (s) | New Performance Optimized median (s) |
|---|---:|---:|---:|
| Bronze | 57.498 | 57.763 | 57.557 |
| Silver | 78.070 | 79.259 | 75.766 |
| Gold | 12.568 | 12.336 | 11.772 |
| Reconciliation | Not included in compact baseline | 1.621 | 1.544 |

Each layer median is computed independently; these values do not sum to the
median total pipeline duration and omit operations outside those stages.

### Correctness and cost status

All **six samples passed seven-table equivalence checks** against the frozen
baseline. Bronze, Silver, and quarantine matched exact row multisets, including
duplicate multiplicity, excluding run-specific provenance. Gold keys and exact
metrics matched; floating-point aggregates used absolute `1e-9` plus relative
`1e-9` tolerance. Every sample reconciled to **9,551,977 valid rows** and
**2,801 quarantined rows**. Frozen baseline table versions remained unchanged.

Usage was initially pending on October 30. A billing refresh on **November 2
at 12:08 PM PST** (`2026-11-02T20:08:11Z`) found records for all six runs.

| Sample | Observed list-price compute estimate |
|---|---:|
| Standard 1 | $0.139400 |
| Standard 2 | $0.130331 |
| Standard 3 | $0.142685 |
| Performance Optimized 1 | $0.330759 |
| Performance Optimized 2 | $0.344693 |
| Performance Optimized 3 | $0.310831 |

Cost is calculated from signed DBU usage, including corrections,
multiplied by the applicable USD list price. Preparation, output-audit, SQL
billing-collection, and storage costs are outside the measured-run comparison.

The **58.14%** improvement compares the two new replay configurations only.
Do not compare the original 707.270 s ingestion observation directly with these
replays as an optimization saving: input-landing work and execution conditions
are different. Performance Optimized's observed median compute estimate was
**2.37 times** Standard's. Standard remains the cost-oriented choice for this
daily workload when several extra minutes of startup are acceptable.

Evidence: [performance_modes_q1_2024.json](performance_modes_q1_2024.json),
updated with the November 2 billing refresh. No measured workload was rerun.

## 5. Validation and operational notes

The latest full local suite passed **57 tests** on November 2:
`.venv/bin/python -m pytest -q`. It reported nine existing PySpark datetime
deprecation warnings. Ruff lint/format checks, both bundle validations, and Git
whitespace checks also passed. The October 30 experiment had 50 passing tests;
56 passed before the Gold measurements, with one additional write-accounting
test added during post-run metrics enrichment.

Focused experiment tests cover isolated output namespaces, fixed Q1 inputs,
unsafe/incomparable configuration rejection, changed-code/input detection,
checksum failures, exact row multiplicity, Gold floating-point tolerance,
pending billing, explicit experiment-run billing attribution, and idempotent
controller resumption with actual performance-mode verification.

Earlier baseline checks also confirmed that a READY snapshot rerun did not
rewrite frozen tables, a no-new-data pipeline run was a no-op, and forced
January replay did not produce duplicate row growth.

During this experiment, the CLI token refresh failed. The controller resumed
the same recorded run IDs using the existing Azure CLI authentication; it did
not rerun completed samples or change their compute modes.

Two preceding scheduled production runs had failed at library installation
because their wheel dependency was missing. A full production bundle deployment
restored that dependency. The experiment uses a separate bundle deployment root
to avoid cross-deployment artifact cleanup.

At experiment completion, the dedicated SQL warehouse was **STOPPED**, no
experiment runs were active, and production remained **Standard**, scheduled
daily at **8 AM America/Los_Angeles**, unpaused. Production output tables and
the frozen baseline were not rewritten by the experiment. Restoring the wheel
does not establish that the previously failed production runs were reprocessed.

## 6. Full rebuild vs incremental Gold

Measured **November 2, 2026**. Experiment: `gold_20261102_a`.
Job: `423663284180647`. Evidence:
[gold_incremental_q1_2024.json](gold_incremental_q1_2024.json).

This isolates the **Gold update**, not the entire ingestion pipeline. Both
strategies used Performance Optimized serverless Jobs compute, environment
version 4, the same pinned Q1 Silver snapshot, and identical January-February
Gold starting tables. Seed tables were deep-cloned during a separate preparation
run, before timing or billing the measured samples.

| Input | Valid Silver rows |
|---|---:|
| January-February starting state | 5,970,477 |
| Newly available March batch | 3,581,500 |
| Final Q1 state | 9,551,977 |

**Full:** recompute and overwrite all four Gold tables from current Q1 Silver.
**Incremental:** identify aggregation keys touched by March source rows,
recompute those groups from current Q1 Silver, and MERGE replacements into
existing Gold. This does not add averages together or blindly add counts on
replay. It still reads historical Silver as needed for affected groups.

Keys are based on the actual aggregation, not simply the source month. Some
March source records have pickup dates in other months.

| Gold table | Initial groups | Groups touched by March | Final Q1 groups |
|---|---:|---:|---:|
| Hourly zone demand | 151,881 | 89,010 | 240,874 |
| Monthly fares | 7 | 4 | 8 |
| Airport activity | 1,887 | 962 | 2,847 |
| Trip patterns | 54 | 28 | 79 |

### Measurements

Runs were serial and interleaved as listed. Physical serverless caches were not
cleared. There were three measured repetitions per strategy.

| Sample | Job run ID | Gold time (s) | Whole job (s) | Data-file bytes added |
|---|---|---:|---:|---:|
| Full 1 | `859792248740436` | 18.030 | 67.600 | 1,915,917 |
| Incremental 1 | `24033449925514` | 32.667 | 85.685 | 701,223 |
| Incremental 2 | `114271476042011` | 33.622 | 86.673 | 701,160 |
| Full 2 | `396200783453174` | 11.233 | 46.186 | 1,915,921 |
| Full 3 | `920120895376649` | 11.304 | 44.107 | 1,915,917 |
| Incremental 3 | `269326382772769` | 21.725 | 55.477 | 679,144 |

| Metric | Full rebuild | Incremental affected-key recomputation |
|---|---:|---:|
| Minimum Gold time | 11.233 s | 21.725 s |
| Median Gold time | 11.304 s | 32.667 s |
| Maximum Gold time | 18.030 s | 33.622 s |
| Median whole-job duration | 46.186 s | 85.685 s |
| Median data-file bytes added | 1,915,917 | 701,160 |
| Median data files added | 4 | 8 |
| Median active files after update | 4 | 10 |
| Median active data bytes after update | 1,915,917 | 1,913,225 |
| Median job compute estimate | **$0.114243** | **$0.173948** |

Gold time covers planning and the four aggregate writes. Whole-job time also
includes startup, version checks, and metadata reporting. Attributed job cost
will therefore not be a precise per-stage Gold bill. Separate preparation,
correctness auditing, and later metrics collection are excluded from the
measured-run comparison.

Write accounting uses **all Delta commits between the seed and measured
versions**, including automatic compaction after MERGE. Looking only at the
latest commit can miss the MERGE when OPTIMIZE follows it. These are data-file
bytes, not total storage I/O: transaction logs, deletion-vector sidecars,
source reads, and storage transaction charges are excluded.

### Findings and limits

All **six samples passed equivalence checks for all four Gold tables** against
the frozen Q1 outputs. Keys, counts, and decimal metrics matched exactly;
floating-point aggregates used absolute `1e-9` plus relative `1e-9` tolerance.
Frozen input and seed table versions remained unchanged.

Incremental wrote **63.4% fewer data-file bytes**, but Gold processing took
**2.89 times as long** and left more active files. Recomputing affected keys,
matching existing target rows, and MERGE/compaction overhead outweigh the
benefit for these small Gold outputs. This does not establish that incremental
processing is always slower.

**Decision: keep full Gold rebuilds in production at the measured scale.**
The experiment is append-only; upstream corrections, removed groups, and
deletions are not validated production behavior. No production Gold switch
was deployed.

Gold-run billing was initially **PENDING** at **11:26 AM PST on November 2**.
A refresh at **5:20 PM PST on November 2**
(`2026-11-03T01:20:12Z`) found priced records for all six runs, without rerunning
workloads.

| Sample | Observed list-price job compute estimate |
|---|---:|
| Full 1 | $0.134340 |
| Full 2 | $0.114243 |
| Full 3 | $0.109621 |
| Incremental 1 | $0.181455 |
| Incremental 2 | $0.173948 |
| Incremental 3 | $0.143354 |

Incremental's median job compute estimate was **52.3% higher** than full's.
Estimates use signed DBU usage and effective USD list prices; later billing
corrections remain possible. They exclude invoice discounts, credits, taxes,
storage, preparation, auditing and subsequent metrics collection. If costs are
normalized per million rows, the denominator here is the **3,581,500 newly
available valid March Silver rows**, not original Parquet input rows.

The measured source hashes and a source-identical wheel are archived in the
experiment's managed report directory. Post-run history enrichment preserved
the original measurement hashes and recorded separate collector hashes;
workloads were not rerun to collect the extra write metrics.

## 7. SQL warehouse sizing: 2X-Small vs X-Small

Measured **November 2, 2026, America/Los_Angeles** (November 3 UTC).
Experiment: `sql_sizes_20261102_a`. Evidence:
[sql_warehouse_sizes_q1_2024.json](sql_warehouse_sizes_q1_2024.json).

These are **new measurements of both sizes**, not a comparison against the
October 28 query baseline. The frozen Q1 tables and all four SQL templates were
unchanged. The existing `919966835b70007d` warehouse remained 2X-Small. A separate
X-Small warehouse, `25ce0220d15c1e6f`, isolated billing attribution by size.
Both were serverless PRO, Photon-enabled, CURRENT channel, single-cluster
(min/max one), five-minute auto-stop, and reported DBSQL version **2026.43**.

There were three sessions per size, each with one warmup and three measured
repetitions per query: **96 statements total, 72 measured and 24 warmups**.
Results below pool the nine measured repetitions per query per size, excluding
all warmups. Session order was 2X-Small 1, X-Small 1, X-Small 2, 2X-Small 2,
2X-Small 3, X-Small 3. Queries ran serially in a persistent session per run.
SQL result caching was disabled and verified; physical I/O caches were not
cleared. This is not a cold-cache or concurrency benchmark.

### Latency and query profiles

| Query | 2X-Small client median (ms) | X-Small client median (ms) | 2X-Small engine total (ms) | X-Small engine total (ms) |
|---|---:|---:|---:|---:|
| Monthly fares | 663.810 | 613.476 | 521 | 477 |
| Weekly zone demand | 636.415 | 621.232 | 503 | 467 |
| Airport corridors | 607.397 | 544.274 | 411 | 415 |
| Gold demand | 380.548 | 394.620 | 242 | 226 |

Engine execution excludes some planning/compilation and other engine overhead.
Client time additionally includes connector/network work and fetching results;
client and engine medians need not move together.

| Query | 2X-Small execution (ms) | X-Small execution (ms) | Median read bytes, both sizes | Read/pruned files, both sizes |
|---|---:|---:|---:|---|
| Monthly fares | 348 | 312 | 67,903,788 | 3/0 |
| Weekly zone demand | 303 | 277 | 19,154,764 | 1/2 |
| Airport corridors | 241 | 247 | 52,348,627 | 3/0 |
| Gold demand | 116 | 106 | 1,868,619 | 1/0 |

Both sizes had **zero median spill bytes and zero median remote-read bytes**
for every query. All **96 statements** had final profiles and consistent
result hashes/row counts per query across sizes. Frozen table versions and
workload/engine versions remained identical.

### Findings, costs and cleanup

X-Small's client medians were lower for three queries and slightly higher for
Gold demand. Engine medians did not improve uniformly. These small, warm-cache
queries scan at most about 68 MB; extra compute did not produce a compelling,
consistent latency benefit. This is not a statistically significant speedup
claim, a p95 estimate, or evidence about higher query concurrency.

**Decision: keep the original 2X-Small warehouse configuration.** All six
session billing collectors initially reported warehouse costs **PENDING**.
A refresh at **10:33 AM PST on November 3**
(`2026-11-03T18:33:32Z`) found priced records overlapping all sessions:

| Warehouse size | Distinct overlapping hourly records | Signed DBUs | Overlapping-record list-price estimate |
|---|---:|---:|---:|
| 2X-Small | 2 | 1.271660 | $0.890162 |
| X-Small | 1 | 0.7285183333 | $0.509963 |

**These are not isolated experiment costs or a fair cost comparison.** Billing
records cover whole hours, including startup, idle, metadata and shutdown work.
The first 2X-Small hour also includes the earlier Gold billing checks on that
warehouse. Restricting to the next hour would still compare two 2X-Small
sessions with three X-Small sessions, with unequal shutdown overhead.

Each signed billing record was counted **once per size**, even when it
overlapped multiple sessions. Both query-only window unions and the broader
recorded session-window unions select the same hourly records; shorter query
bounds do not isolate query costs. The raw records, record IDs, effective prices,
window memberships and attribution caveats are preserved in the full report.
Estimates are provisional, exclude invoice discounts, credits, taxes and
storage, and may receive later corrections. **No per-query cost or percentage
saving is claimed.** The refresh did not rerun workloads, and both warehouses
were again verified stopped.

The first session completed its queries but its shutdown timed out. Its
original report retains `WAREHOUSE_STOP_FAILED`, and its recorded window ends
at query completion rather than verified shutdown. The controller recovered
those measurements without rerunning queries and verified shutdown separately.
The shutdown helper was then fixed to check actual state and retry timed-out
stop requests; only cleanup changed, not query measurement logic. Some later
shutdowns also needed retries, so session window durations are not a fair
measure of SQL performance or directly comparable all-in cost boundaries.
Original source manifests are preserved in every report.

Both warehouses were verified **STOPPED** at completion. The comparison
warehouse remains available, stopped, for future experiments. Production
jobs, the original warehouse configuration, and frozen tables were unchanged.
The benchmark tests passed **16 tests**, including the shutdown-timeout retry
case; Ruff lint/format and Git whitespace checks passed.

## 8. Isolated SQL warehouse sizing and whole-sample costs

Successful samples measured **November 4, 2026**, following an interrupted
November 3 attempt. The experiment ID retains the original request date:
`sql_clean_20261103_b`. Evidence:
[sql_warehouse_clean_q1_2024.json](sql_warehouse_clean_q1_2024.json).

Every sample used a **fresh warehouse ID**, exclusively for that sample's
startup, metadata checks, warmups, query window and shutdown. No billing queries
or unrelated workloads ran on these warehouses. Both sizes used serverless PRO,
Photon, CURRENT channel, min/max one cluster and five-minute auto-stop. All
samples reported **DBSQL 2026.44** and used the same immutable Q1 tables and SQL.
The existing warehouses and production jobs were not reconfigured.

Each sample warmed up the four queries once, then repeated balanced four-query
cycles for a nominal **600 seconds**. A final cycle finished if it crossed the
deadline; actual windows were **600.510-601.430 seconds**. SQL result caching
was disabled and verified. Physical I/O caches were not controlled. Work was
serial, locally orchestrated through the SQL connector, not concurrent.

### Sample measurements

| Sample | Size | Measured queries | Actual query window (s) | Queries/min | Shutdown (s) |
|---|---|---:|---:|---:|---:|
| 2xs_1 | 2X-Small | 1,516 | 601.137 | 151.313 | 2.386 |
| xs_1 | X-Small | 1,392 | 600.585 | 139.064 | 3.989 |
| xs_2 | X-Small | 1,440 | 600.510 | 143.878 | 3.539 |
| 2xs_2 | 2X-Small | 1,400 | 601.430 | 139.667 | 2.397 |
| 2xs_3 | 2X-Small | 1,420 | 600.784 | 141.815 | 2.368 |
| xs_3 | X-Small | 1,336 | 601.073 | 133.362 | 3.890 |

There were **8,504 measured queries plus 24 warmups**. All **8,528 workload
statements** returned matching result hashes/counts per query and had final
profiles without result-cache hits. Frozen versions were checked before and
after each sample. Median spill was zero for both sizes across all four queries.

### Comparison

Figures are **medians of the three per-sample medians**, not pooled queries:

| Query | 2X-Small client (ms) | X-Small client (ms) | 2X-Small engine total (ms) | X-Small engine total (ms) |
|---|---:|---:|---:|---:|
| Monthly fares | 499.547 | 499.678 | 356 | 336 |
| Weekly zone demand | 423.085 | 440.203 | 307 | 317 |
| Airport corridors | 406.980 | 410.094 | 258 | 270.5 |
| Gold demand | 309.298 | 310.142 | 170 | 176 |

| Metric | 2X-Small | X-Small |
|---|---:|---:|
| Median measured queries/min | 141.815 | 139.064 |
| Median full lifecycle duration | 638.586 s | 637.969 s |
| Median whole-sample list-price estimate | **PENDING** | **PENDING** |
| Median amortized full-sample cost per measured query | **PENDING** | **PENDING** |

Larger compute did not provide a consistent latency or throughput benefit.
These are small, warm-cache scans with only three independent windows per size;
do not claim statistical significance or generalize to high concurrency.
Do not compare timings directly against section 7: the repetition pattern and
engine version changed. Throughput includes local orchestration, result hashing
and checkpoint overhead; client timings include connector/network/fetch time.

### Clean billing boundaries and pending usage

Billing is collected **afterward on the separate existing 2X-Small warehouse**.
Each fresh sample's entire signed DBU usage is selected by its warehouse ID,
including corrections, and priced at effective USD list prices. Because no
other activity uses that ID, hourly records need not be prorated or allocated
between runs. All startup, metadata, warmup and shutdown charges belong to the
sample; collector charges do not.

The first refresh, completed **11:27 AM PST on November 4**
(`2026-11-04T19:27:25Z`), found **no usage records for any sample**. Pending is
not zero. Once available, full-sample cost divided by completed measured queries
will be reported as **amortized operating cost/query**, not an execution-only
bill. List-price estimates exclude discounts, credits, taxes and storage and
can receive later corrections. Production remains unchanged pending evidence.

### Failed attempt, reproducibility and cleanup

The earlier `sql_clean_20261103_a` attempt failed after **608 recorded
statements** during a prolonged connector/network stall. Its partial report
and errors were preserved, and its warehouse was stopped. It is excluded from
successful-sample comparisons; its uncollected cost is **not zero** or included
in the six successful samples' cost denominator.

The restarted experiment used six new warehouse IDs, 30-second socket timeouts,
one request attempt and a wall-clock overrun guard. A scoped macOS idle-sleep
inhibitor was used only while the bounded controller ran. Successful shutdowns
took **2.368-3.989 seconds**; no failed sample was silently resumed or reused.
All six sample warehouses, the failed-attempt warehouse and the billing
collector were verified stopped and unused. The sample warehouses remain
stopped to preserve their identities for subsequent billing collection.

The exact measured Python sources and SQL templates were archived and their
hashes verified. The full local suite passed **65 tests** with nine existing
PySpark datetime warnings; Ruff lint/format and Git whitespace checks passed.

Measurement command (reusing this completed report does not rerun samples):

```bash
.venv/bin/python -m nyc_taxi_lakehouse.warehouse_experiment \
  --phase measure --experiment-id sql_clean_20261103_b \
  --duration-seconds 600 --auth-type azure-cli
```

Refresh billing without rerunning queries:

```bash
.venv/bin/python -m nyc_taxi_lakehouse.warehouse_experiment \
  --phase billing --experiment-id sql_clean_20261103_b --auth-type azure-cli
```

Commands require the existing workspace host/profile and Azure CLI identity.
Reports are persisted locally and in the managed volume; source/configuration
guards reject incomparable resumption, and partial failed samples are not
automatically rerun.

## 9. Partitioning vs liquid clustering: configuration/no-op observation

Measured **November 4, 2026**. Experiment: `layout_20261104_a`. Evidence:
[layout_q1_2024.json](layout_q1_2024.json).

**Important limitation: Databricks did not physically recluster this data.**
The liquid keys were registered, but CTAS reported
`Reason for skipping: Estimated ingestion size is not within the expected range`.
The returned `OPTIMIZE FULL` metrics showed **zero clustering tasks planned,
zero files added/removed and zero bytes rewritten**. This measures the platform's
configuration and no-op behavior on these inputs, not the performance of a fully
reorganized liquid-clustered table.

### Inputs, settings and observed file layouts

Both copies were created from **version 0 of the frozen Q1 Silver table**,
with **9,551,977 rows** each. The original frozen table remains unchanged at
**512,970,251 active bytes in three files**. New copies are fresh materializations,
not exact clones of the original file layout.

Both used ZSTD compression and the same **64 MiB best-effort `delta.targetFileSize`**
property. The smaller target was an experiment setting to expose potential
skipping granularity in this roughly 513 MB table, not a production recommendation.
It does not force splitting already large files. Catalog predictive optimization
remained disabled.

| Setting/metric | Month-partitioned copy | Liquid-configured copy |
|---|---|---|
| Partition columns | `source_month` | None |
| Clustering keys | None | `source_month`, `pickup_at` |
| Active files | 6 | 5 |
| Active data bytes | 512,979,991 | 512,812,875 |
| CTAS time, one observation | 15.027 s | 9.746 s |
| Requested optimization | `OPTIMIZE` | `OPTIMIZE FULL` |
| Optimization time, one observation | 3.041 s | 13.926 s |
| Physical optimization writes | 0 files, 0 bytes | 0 files, 0 bytes |

Liquid CTAS creates a different file arrangement, but neither subsequent
optimization rewrote files. Liquid optimization performed metadata/classification
work; a longer timer is **not** evidence of physical reclustering cost.
Source month and pickup timestamp were chosen to match the weekly query's
predicates. Boolean `airport_trip` is not a supported clustering-key type, so
the airport/full-scan queries are not expected to gain selective skipping from
these keys. The unchanged Gold table is a control.

### Query measurements and correctness

All queries used the **same 2X-Small serverless PRO warehouse**, Photon enabled,
one cluster, and DBSQL **2026.44**. There were three interleaved blocks per layout,
each with one warmup and three measured repetitions per query:
**96 workload statements, 72 measured and 24 warmups**.
SQL result caching was disabled and verified; physical I/O caches were not cleared.

| Query | Partitioned client median (ms) | Liquid-configured client median (ms) | Partitioned read bytes | Liquid-configured read bytes |
|---|---:|---:|---:|---:|
| Monthly fares | 513.433 | 563.330 | 67,903,788 | 67,447,081 |
| Weekly zone demand | 519.055 | 515.828 | 19,154,764 | 19,155,560 |
| Airport corridors | 412.784 | 511.510 | 51,896,665 | 51,825,888 |
| Gold demand, unchanged control | 360.417 | 314.095 | 1,868,619 | 1,868,619 |

The weekly query read **two files in both copies**, pruning four of six
partitioned files versus three of five liquid-configured files. The actual bytes
read were essentially identical. Read-file fractions alone would be misleading.
All query medians had zero disk spill.

Both copies passed exact, bidirectional **`EXCEPT ALL` row-multiset audits**
across all visible columns, preserving duplicate multiplicity. All 96 statements
returned consistent hashes/counts per query and had final profiles without result
cache hits. Frozen and alternative table versions were unchanged during queries.

The unchanged Gold control also had a noticeable client-time difference.
Do not attribute observed client latency differences to liquid clustering or
claim statistical significance from these small, warm-cache samples.

### Preparation costs, accounting and decision

Each layout's CTAS and optimization ran on a **fresh, dedicated 2X-Small
warehouse**. Exact row audits and query comparisons ran on the separate existing
warehouse. Whole preparation-warehouse costs will therefore be attributable by
warehouse ID, including startup, metadata and shutdown, but are not stage-level
or steady-state recurring-maintenance costs.

Both preparation costs were **PENDING** at **1:20 PM PST on November 4**
(`2026-11-04T21:20:24Z`). Missing is not zero. Billing collection did not rerun
writes or workload queries.

Delta history reported CTAS output bytes but omitted write counters for the
metadata-only liquid `OPTIMIZE` commits. Original history accounting remains
`INCOMPLETE` with null fields. Explicit returned OPTIMIZE metrics independently
confirm zero added/removed files and bytes; post-run enrichment parsed those
saved results without changing the measured source hashes or rerunning workloads.

**Decision: retain production month partitioning.** No measurable scan benefit
was demonstrated, and physical liquid clustering was not exercised. A meaningful
physical-clustering comparison needs inputs/layout for which the platform actually
schedules file rewrites; this is not a general verdict against liquid clustering.
No data was duplicated artificially or undocumented threshold settings forced
to manufacture an optimization benefit.

The exact measured source files and SQL templates are archived with verified hashes.
The preparation warehouses and query/billing collector were verified stopped.
Production and frozen baseline tables were untouched. The full suite passed
**69 tests**, with nine existing PySpark warnings; Ruff and whitespace checks passed.

Refresh preparation billing without rerunning writes or queries:

```bash
.venv/bin/python -m nyc_taxi_lakehouse.layout_experiment \
  --phase billing --experiment-id layout_20261104_a
```

The CLI uses the existing workspace host/profile and Azure CLI identity.
The measurement phase rejects an existing report or occupied target names
instead of overwriting benchmark tables.

## 10. Detailed evidence

The JSON files beside this document contain exact values, source hashes,
configuration, and run identifiers; displayed tables above round timings.
Measurements used working-tree changes, so neither the original query baseline
nor this experiment should be described as having run entirely from a later
clean commit.

Full baseline report in the managed Databricks volume:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/f36dd6bc-5c74-4e99-971e-74a3da449bec_3c11a579-34a3-47e5-ad26-0b386e69e416.json
```

Full performance-mode report:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/pipeline_experiments/perf_20261030_a/experiment_report.json
```

Full Gold comparison report:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/gold_experiments/gold_20261102_a/experiment_report.json
```

Full SQL warehouse sizing controller, containing all six session reports,
statement IDs, profiles, source manifests and original billing collections:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/sql_sizes_20261102_a_controller.json
```

Full isolated SQL sizing report and exact measured source archive:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/warehouse_experiments/sql_clean_20261103_b/experiment_report.json
/Volumes/nyc_taxi_dev/benchmark/reports/warehouse_experiments/sql_clean_20261103_b/measured_sources/manifest.json
```

Full layout configuration/no-op report, exact measured source archive, and
separate post-run accounting collector:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/layout_experiments/layout_20261104_a/experiment_report.json
/Volumes/nyc_taxi_dev/benchmark/reports/layout_experiments/layout_20261104_a/measured_sources/manifest.json
/Volumes/nyc_taxi_dev/benchmark/reports/layout_experiments/layout_20261104_a/post_run_collectors/layout_experiment.py
```

The performance experiment directory also contains its pinned input manifest,
per-sample measurements, and complete output-audit report. Local full reports
live under the Git-ignored `benchmark-results/` directory, including
`perf_20261030_a.json`. Billing-only refreshes update the full report without
rerunning measured pipeline workloads.
