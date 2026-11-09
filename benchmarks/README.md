# Benchmark results and experiment log

Results recorded through **November 6, 2026**. Money amounts are USD estimates
from effective Databricks list prices and, where available, observed Azure
pre-tax resource costs, not invoices or total project spending.

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
| Classic Jobs vs Standard serverless | Six Q1 replays; DBU and Azure infrastructure costs observed | Serverless: 43.19% lower median job time and 65.61% lower aggregate attributable compute/infrastructure cost |

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
| Median whole-sample list-price estimate | **$0.491101** | **$0.735544** |
| Median amortized full-sample cost per measured query | **$0.000345846** | **$0.000528408** |

Larger compute did not provide a consistent latency or throughput benefit.
These are small, warm-cache scans with only three independent windows per size;
do not claim statistical significance or generalize to high concurrency.
Do not compare timings directly against section 7: the repetition pattern and
engine version changed. Throughput includes local orchestration, result hashing
and checkpoint overhead; client timings include connector/network/fetch time.

### Clean billing boundaries and observed provisional costs

Billing is collected **afterward on the separate existing 2X-Small warehouse**.
Each fresh sample's entire signed DBU usage is selected by its warehouse ID,
including corrections, and priced at effective USD list prices. Because no
other activity uses that ID, hourly records need not be prorated or allocated
between runs. All startup, metadata, warmup and shutdown charges belong to the
sample; collector charges do not.

The first refresh, completed **11:27 AM PST on November 4**
(`2026-11-04T19:27:25Z`), found **no usage records for any sample**.
The **November 5** refresh (`2026-11-06T00:51:56Z-00:52:13Z`) found priced
records for **all six samples**, at **$0.70/DBU**. Seven unique records were
selected; the final X-Small sample spans two hourly buckets, both counted once.
Evidence: [warehouse_billing_20261105.json](warehouse_billing_20261105.json).

X-Small's median whole-sample estimate was **49.77% higher**, with no consistent
latency or throughput benefit. Its median amortized cost per measured query was
**52.79% higher** (about **$0.528 versus $0.346 per 1,000 measured queries**).
These compare medians of the three per-sample costs/ratios, not pooled statements.
Full-sample cost divided by completed measured queries is **amortized operating
cost/query**, not an execution-only bill. Estimates exclude discounts, credits,
taxes and storage and remain provisional, subject to later corrections.

**Decision: retain the existing 2X-Small baseline.** This validates a configuration
already in use; it is not newly achieved production savings. The earlier
confounded sizing experiment in section 7 remains separate.

### Failed attempt, reproducibility and cleanup

The earlier `sql_clean_20261103_a` attempt failed after **608 recorded
statements** during a prolonged connector/network stall. Its partial report
and errors were preserved, and its warehouse was stopped. It is excluded from
successful-sample comparisons. Its dedicated warehouse's whole-lifecycle cost
was later observed at **$0.488872**, provisional and kept separate from the six
successful samples and their denominators.

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
warehouse. Whole preparation-warehouse costs are therefore attributable by
warehouse ID, including startup, metadata and shutdown, but are not stage-level
or steady-state recurring-maintenance costs.

Both preparation costs were **PENDING** at **1:20 PM PST on November 4**
(`2026-11-04T21:20:24Z`). The November 5 refresh observed **$0.030242**
for partitioned preparation and **$0.269701** for liquid-configured preparation,
at effective USD list prices; records remain provisional.
The full recorded lifecycles were **46.838 s versus 354.299 s**. The liquid
warehouse encountered a stop-request timeout, retained in these costs.
This large operating-cost difference is **not a physical reclustering or
recurring-maintenance cost comparison**: OPTIMIZE rewrote zero files in both.
Billing collection did not rerun writes or workload queries. Raw records and
collector source hashes are in
[warehouse_billing_20261105.json](warehouse_billing_20261105.json).

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

### Annual preflight: physical clustering on write verified

Experiment `layout_annual_20261104_a` prepared **January-December 2024**
Silver-only copies, independently of production and Q1. Preparation/audits ran
**November 4-5, 2026 PST**; read-only physical evidence enrichment completed
November 5. Exact timestamps and measurements:
[layout_annual_2024_preflight.json](layout_annual_2024_preflight.json).

All twelve source months were already processed. The experiment pinned live
Silver **version 21** and manifest **version 66**, reconciled every month's valid
count with its complete checksummed manifest, and froze **41,156,210 rows** into
`nyc_taxi_dev.benchmark.fy_2024_trips` at version **0**. This separate annual
Silver snapshot contains **2,215,779,228 active bytes in twelve files**.
The source Parquet files total **693,001,713 bytes**. Selection is by source month,
not pickup date; existing out-of-month timestamps and duplicate multiplicity are
preserved. This is not a full annual Bronze/Silver/Gold dataset registration.

Both alternatives used fresh dedicated 2X-Small serverless preparation warehouses,
ZSTD and the same 64 MiB best-effort file target as Q1:

| Metric | Month-partitioned | Liquid, `source_month,pickup_at` |
|---|---:|---:|
| Rows | 41,156,210 | 41,156,210 |
| Active files | 24 | 34 |
| Active bytes | 2,215,818,314 | 2,217,253,050 |
| CTAS, one observation | 42.472 s | 52.647 s |
| OPTIMIZE / OPTIMIZE FULL, one observation | 2.936 s | 7.438 s |
| Files rewritten by OPTIMIZE | 0 | 0 |

**Unlike Q1, the annual liquid CTAS physically clustered on write.** Its history
explicitly reports **`clusteringOnWriteStatus = kdtree triggered`**, with
**34 output files and 2,217,253,050 written bytes**. Subsequent `OPTIMIZE FULL`
planned zero clustering tasks and rewrote zero files; it did not perform an
additional reclustering pass. A no-op OPTIMIZE alone therefore cannot establish
whether the preceding CTAS clustered.

The initial preflight gate checked only OPTIMIZE rewrites and returned
`NOT_VERIFIED`. A separate read-only collector recognized the explicit CTAS
clustering evidence, yielding **`VERIFIED`, method `CTAS_ON_WRITE`**. The report
retains the initial gate result, original measured source hashes, and the separate
post-run collector hash. No writes or performance queries were rerun to change
that verdict.

The annual freeze and both alternatives passed exact bidirectional `EXCEPT ALL`
audits across all visible columns, with zero row-multiset differences.
Per-file row counts, byte sizes, and source-month/pickup-time min/max ranges were
collected separately; their row/file/byte totals reconcile with table details.
All 24 partitioned and 34 liquid files individually contain a single source month.
These observations establish physical preparation, **not query-speed or scan
savings**. No performance repetitions were launched during the preflight itself.

Production was not modified; all seven Q1 snapshot versions remain unchanged.
The three annual tables remain at version 0, and all owned warehouses were
stopped. The November 5 billing refresh observed whole-preparation list-price
estimates of **$0.053317 partitioned** and **$0.301467 liquid**, provisional.
The full recorded lifecycles were **76.298 s and 394.863 s**; the liquid
warehouse's stop-request timeout is included. Freeze/audit and enrichment usage
ran on the separate existing collector and is excluded. These single
whole-lifecycle observations are not stage-level or recurring-maintenance costs.
The original preflight report remains byte-identical to preserve its hash in
the query report; billing is recorded separately in
[warehouse_billing_20261105.json](warehouse_billing_20261105.json).

The controlled query comparison below uses these physically prepared annual
layouts without forcing an unnecessary rewrite or altering production.
The Gold incremental crossover remains deferred.

### Annual query comparison: selective read reduction, little latency change

Measured **November 5, 2026, 10:46-10:51 AM PST**. Evidence:
[layout_annual_2024_queries.json](layout_annual_2024_queries.json).
Both annual layouts and the frozen Silver source remained at **version 0**.
The same fixed **2X-Small serverless Photon warehouse**, one cluster, DBSQL
**2026.44**, served both layouts. Three interleaved blocks per layout each ran
one warmup plus three measured repeats per query: **96 successful workload
statements, 72 measured and 24 warmups**. Each query/layout has nine measured
repeats across three blocks, not nine independently provisioned trials.

Monthly fares and airport corridors scan the annual input; weekly demand retains
the January 1-8 predicate from the original suite. The unchanged **Q1 Gold**
table is a timing control, not annual Gold. Result caching was disabled and all
96 final profiles explicitly reported `result_from_cache = false`. Physical
I/O caches were not cleared.

| Query | Partitioned client median (ms) | Liquid client median (ms) | Partitioned read bytes | Liquid read bytes |
|---|---:|---:|---:|---:|
| Monthly fares, decimal distance average | 1,329.323 | 1,330.711 | 292,461,179 | 295,616,129 |
| Weekly zone demand | 511.994 | 510.671 | 19,154,764 | 10,504,551 |
| Airport corridors | 585.729 | 615.537 | 226,204,247 | 226,728,494 |
| Q1 Gold control | 304.280 | 302.802 | 1,868,619 | 1,868,619 |

**The selective weekly query read 45.16% fewer bytes**, reading **one liquid
file versus two partitioned files**. It pruned 33/34 versus 22/24 files.
This is actual finer-grained data skipping, unlike the Q1 configuration/no-op
observation. Median client latency was essentially unchanged (**0.26% lower**).
Engine-total medians were **346 ms versus 333 ms**, and execution medians
**228 ms versus 205 ms**; these small samples do not establish a general
latency advantage.

Monthly and airport full scans showed no material read reduction. All query
medians had **zero spill and zero remote-read bytes**; median read bytes came
from the I/O cache. Read-byte reduction is therefore not a measured reduction
in Azure storage traffic or billing. The unchanged Gold control's per-block
client medians also varied (partitioned blocks **286-323 ms**, liquid blocks
**266-408 ms**), illustrating warm-cache/client timing noise.

#### Correctness failure preserved and deterministic average correction

The first annual attempt recorded all 96 statements but correctly **failed**
the exact-result gate. Monthly results differed only in the average distance
for the ten-row **2008-12 pickup-month** group: **12.12 versus 12.11**.
A separate diagnostic reproduced both original hashes. The tables still passed
exact row-multiset audits; floating-point aggregation order at a rounding
boundary, not changed source rows, caused the discrepancy.

The corrected annual query averages `CAST(trip_distance_miles AS DECIMAL(28,6))`
before rounding to two places. A full-source precision guard checked all
**41,156,210 rows**, confirming **zero** values failed to round-trip through
that decimal type back to the original DOUBLE. Historical Q1 templates remain
unchanged; the annual override and exact executed SQL hashes are archived.
No rows were filtered out to conceal the discrepancy, and exact result checks
were not weakened.

All **96 corrected statements** returned matching results per query across
layouts and blocks. Both alternatives again passed exact bidirectional
`EXCEPT ALL` audits; Q1, annual source and alternative versions remained stable.
The failed attempt and diagnostic retain separate reports/source evidence;
none of their samples are pooled into the successful comparison.

**Decision: keep production month partitioning.** Liquid clustering demonstrated
selective scan reduction on the annual input, but not a meaningful end-to-end
latency improvement or measured cost saving in this warm-cache serial workload.
Production was not modified and owned compute was verified stopped. Preparation
costs were subsequently collected separately, including shutdown overhead, as
described above. Shared query compute does not support isolated per-layout or
failed-query-attempt bills; no such attribution or query-serving savings is
claimed.

## 10. Classic Jobs versus Standard serverless

The isolated comparison `compute_20261105_a` completed on **November 6, 2026
UTC**; its ID uses the November 5 local-date label. Three fresh Q1 replays per
configuration processed identical checksum-pinned Parquet and taxi zones,
without live downloads. Each replay wrote new Bronze/Silver/Gold/ops schemas.
The interleaved order was serverless, classic, classic, serverless, serverless,
classic.

The approved classic profile used **one Standard_D4s_v3 driver plus one worker**
(**8 vCPUs / 32 GiB total**), **Photon, DBR 17.3 LTS, on-demand VMs**, no
autoscaling or instance pools, and no Jobs task retries. Each measurement used
a different Databricks job cluster ID. Standard serverless used environment 4.
Prepare and final audit used Performance Optimized serverless outside the six
measurements.

**This compares deployment configurations, not identical hardware/runtime
stacks.** The actual Spark versions were **4.2.0 serverless versus 4.0.0
classic**; both reported Python **3.12.3**. Serverless capacity was managed by
Databricks and was not matched to the classic cluster.

| Metric | Standard serverless | Classic Jobs |
|---|---:|---:|
| Median total job time | 540.825 s (9.01 min) | 952.004 s (15.87 min) |
| Total job time range | 522.140-546.375 s | 704.558-1,070.454 s |
| Median pipeline-call time | 193.418 s | 416.318 s |
| Median internal processing time | 183.766 s | 391.450 s |
| Median API task setup time | 185 s | 501 s |

Standard serverless had **43.19% lower median total job time** and **53.54%
lower median pipeline-call time** in these three trials per configuration.
All three serverless job times were below all three classic job times.
Task setup is **not** a complete measure of scheduling/environment overhead;
the total job timer includes work outside the pipeline call. The pipeline call
also includes final operational writes excluded from the internal processing
timer. These timings must not be interchanged.

**Fresh job clusters did not imply fresh Azure VMs.** Read-only
`system.compute.node_timeline` evidence mapped the first two classic cluster
IDs to the same pair of VM instance IDs, with their driver/worker roles swapped.
The third classic sample's active VM/disk resources were captured separately
and their `ClusterId` tags matched its cluster. Physical caches and platform
VM/image reuse were not controlled. Classic setup varied from **202 to 561
seconds**; these observations do not establish universal cold-start behavior.

All six samples processed **9,554,778 Bronze rows**, yielding **9,551,977 valid
Silver rows** and **2,801 quarantine rows**. The final audit passed exact
Bronze/Silver/quarantine row-multiset checks excluding provenance, exact Gold
keys/counts/decimal metrics, and the existing absolute-plus-relative `1e-9`
floating-aggregate tolerance. Frozen Q1 versions remained unchanged. All three
classic clusters terminated, both experiment jobs were inactive, and the
managed resource group had **zero remaining VM/disk resources** at final
verification. Production was not changed.

### Cost accounting: DBUs and attributable Azure infrastructure observed

A separate billing-only collection, `compute_billing_20261105_a`, queried all
eight exact job/run IDs, keeping prepare/audit separate from measurements.
At **03:52 UTC November 6**, **no DBU usage records** had arrived. The Azure
Cost Management query succeeded after an initial throttle but likewise
returned **no infrastructure cost rows**. Missing records are **not zero
cost**. The dedicated billing collector was stopped afterward; original
measurement reports remained byte-identical.

Classic cost must include **Databricks DBUs plus Azure VM/disk charges**.
The official West US Linux on-demand VM reference was **$0.234/node-hour**,
not an observed bill or complete estimate. The third sample had one **64-GiB
P6** and one **128-GiB P10 Premium SSD** per node. These disk observations are
not inferred for earlier allocations. Node-timeline minutes are not complete
Azure allocated lifetimes. Because the first two classic runs shared VM
instance IDs, future resource/day Azure charges must not be counted once for
each run; any allocation or amortization must be labeled explicitly.

#### November 6 billing refresh

The follow-up collection `compute_billing_20261106_b` at **07:33 UTC** found
priced DBU records for all six measured runs. Twelve measured usage records
were observed, all ORIGINAL at this collection; signed corrections remain
supported. Prepare (**$0.121589**) and audit (**$0.905398**) were recorded
separately and excluded from the comparison.

| Databricks DBU list-price estimate | Standard serverless | Classic Jobs |
|---|---:|---:|
| Median per measured replay | $0.134619 | $0.218073 |
| Total for three measured replays | $0.393539 | $0.677178 |

Serverless had **38.27% lower median Databricks charges**, in addition to its
43.19% lower median job time. **This is not the total cloud-cost savings
percentage:** serverless infrastructure is included in its SKU, whereas
classic Azure VM/disk charges were still uncollected at this refresh. The Azure Cost
Management request returned **HTTP 429**, so current infrastructure costs
could not be refreshed. All estimates remain provisional; the billing
collector was stopped and original measurement/earlier collection reports
were preserved.

#### Compute and Azure infrastructure closeout

The Azure query at **17:34 UTC November 6** returned cost rows for all **four
recorded VMs**, **eight disks**, and **four matching public IPs**, plus VM
bandwidth. Explicit zero-cost container-root disk rows were retained; no
missing disk cost was silently treated as zero. The first two classic clusters
shared VM IDs, so each resource's daily cost was counted **once**, not once per
job. Twenty attributable resource/service rows were reconciled.

The **17:36 UTC** DBU refresh left all six measured replay costs unchanged.
Additional late audit records raised its separately excluded estimate to
**$1.021279**. Preparation, audit and the billing collector are not part of the
measured configuration comparison.

| Three-replay cost component | Standard serverless | Classic Jobs |
|---|---:|---:|
| Databricks DBU list-price estimate | $0.393539 | $0.677178 |
| Azure VMs | Included in serverless SKU | $0.401707 |
| Azure managed disks | Included in serverless SKU | $0.047442 |
| Azure public IPs and bandwidth | Included in serverless SKU | $0.017989 |
| Combined attributable cost, three replays | $0.393539 | $1.144316 |
| Amortized mean cost per replay | $0.131180 | $0.381439 |

**Standard serverless had 65.61% lower observed aggregate compute/infrastructure
cost**, alongside **43.19% lower median total job time**, for these configurations.
The cost comparison uses **three-replay totals or amortized means**, not median
per-run infrastructure bills: shared resource/day charges do not permit that
per-run attribution.

These combined amounts use effective USD list-price DBU estimates plus observed
Azure pre-tax resource costs. One shared workspace storage-account row,
**$0.0141080595**, was recorded but excluded because it was not separately
attributable to classic measurements. Shared ADLS storage and invoice-level
adjustments are also excluded. This is an **attributable compute/infrastructure
comparison, not total project spending or a final invoice**; later billing
corrections remain possible. Exact resources, raw inputs, reconciliation source
and hashes are archived under `compute_cost_closeout_20261106_c`.

**Decision: retain Standard serverless on observed latency and attributable
compute/infrastructure cost for this workload.** Production already used Standard,
so this is supporting evidence, not newly realized production savings. Three
small trials with different runtimes and opaque serverless hardware do not
establish concurrency, scaling, or general price-performance claims.

Portable values and evidence hashes are in [`compute_q1_2024.json`](compute_q1_2024.json).
The isolated bundle is `compute-experiments/databricks.yml`. Billing can be
refreshed without rerunning the six measurements:

```bash
DATABRICKS_HOST=<workspace-host> .venv/bin/python -m nyc_taxi_lakehouse.compute_billing \
  --experiment-report benchmark-results/compute_20261105_a.json \
  --collection-id compute_billing_<new-collection-id>
```

Use a new lowercase identifier for each collection. Full measurements, exact
measured Python sources, infrastructure metadata and separate billing evidence:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/compute_experiments/compute_20261105_a/experiment_report.json
/Volumes/nyc_taxi_dev/benchmark/reports/compute_experiments/compute_20261105_a/measured_sources/
/Volumes/nyc_taxi_dev/benchmark/reports/compute_experiments/compute_20261105_a/post_run_evidence/manifest.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_billing_20261105_a/report.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_billing_20261105_a/collector_sources/
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_billing_20261106_b/report.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_billing_20261106_b/compute_azure_cost_20261106_b.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_billing_20261106_c/report.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_cost_closeout_20261106_c/report.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/compute_cost_closeout_20261106_c/manifest.json
```

## 11. Detailed evidence

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

Annual Silver/layout preflight, original source archive, and read-only enrichment:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/experiment_report.json
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/measured_sources/
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/post_run_collectors/layout_preflight.py
```

Annual query comparison, exact measured Python/SQL sources, and excluded
initial-attempt diagnostics:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/query_report_decimal.json
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/query_sources_decimal/measured_sources/
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/query_report.json
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/query_sources/measured_sources/
/Volumes/nyc_taxi_dev/benchmark/reports/layout_preflights/layout_annual_20261104_a/query_result_diagnostic.json
```

November 5 warehouse billing refresh, including raw signed usage, effective
prices, collector statement IDs and exact collector sources:

```text
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/warehouse_billing_20261105_a/report.json
/Volumes/nyc_taxi_dev/benchmark/reports/billing_refreshes/warehouse_billing_20261105_a/collector_sources/
```

This refresh selected eleven dedicated warehouses: six successful clean SQL
samples, four Q1/annual layout preparation warehouses, and the excluded failed
clean-SQL warehouse. Their observed estimates total **$4.831575**; this is **not
total project spending** and excludes shared collector/query compute, other
experiments, storage and non-list-price adjustments. Twelve unique records were
observed, all ORIGINAL at this collection; signed corrections will be retained
if present in later collections. All original measurement reports and source
hashes were left unchanged.

The performance experiment directory also contains its pinned input manifest,
per-sample measurements, and complete output-audit report. Local full reports
live under the Git-ignored `benchmark-results/` directory, including
`perf_20261030_a.json`. Earlier billing-only collectors update their reports;
the November 5 refresh instead persists separate evidence to preserve original
measurement hashes. Neither approach reruns measured pipeline workloads.
