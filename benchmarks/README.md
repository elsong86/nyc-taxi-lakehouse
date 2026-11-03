# Benchmark results and experiment log

Results recorded through **November 2, 2026**. All money amounts are USD
list-price estimates, not invoices or total project spending.

## At a glance

| Experiment | Status | Main result |
|---|---|---|
| Original Q1 pipeline | One observed successful run | 707.270 s total job duration; $0.145978 attributed compute estimate |
| Frozen Q1 query baseline | Four queries, three measured repetitions each | Repeatable results with SQL result caching disabled |
| Standard vs Performance Optimized | Three pipeline replays per mode; all output audits passed | Median job duration 558.317 s vs 233.703 s; estimated compute $0.139400 vs $0.330759 |
| Full vs incremental Gold | Three March updates per strategy; all output audits passed | Median Gold time 11.304 s vs 32.667 s; incremental wrote 63.4% fewer data-file bytes |

**Current finding:** Performance Optimized had **58.14% lower median total job
duration**, but only **5.44% lower median pipeline-call duration**. Most of the
observed difference was outside pipeline execution. Its observed median compute
estimate was **2.37 times** Standard's, not 58% cheaper.

For Gold, the incremental implementation reduced data-file writes but was
**2.89 times slower** at this scale. Production remains on Standard compute
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
| Median job compute estimate | **PENDING** | **PENDING** |

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

New Gold-run billing was still **PENDING** at **11:26 AM PST on November 2**
(`2026-11-02T19:26:46Z`). Pending is not zero. If costs are normalized per
million rows, the denominator here is the **3,581,500 newly available valid
March Silver rows**, not original Parquet input rows.

The measured source hashes and a source-identical wheel are archived in the
experiment's managed report directory. Post-run history enrichment preserved
the original measurement hashes and recorded separate collector hashes;
workloads were not rerun to collect the extra write metrics.

## 7. Detailed evidence

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

The performance experiment directory also contains its pinned input manifest,
per-sample measurements, and complete output-audit report. Local full reports
live under the Git-ignored `benchmark-results/` directory, including
`perf_20261030_a.json`. Billing-only refreshes update the full report without
rerunning measured pipeline workloads.
