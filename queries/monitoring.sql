USE CATALOG nyc_taxi_dev;

-- Processing time excludes Databricks job startup and final metrics persistence.
SELECT
  job_run_id,
  status,
  selected_months,
  processed_rows,
  processed_source_bytes,
  downloaded_bytes,
  timestampdiff(MILLISECOND, started_at, finished_at) / 1000.0 AS processing_seconds,
  processed_rows / nullif(
    timestampdiff(MILLISECOND, started_at, finished_at) / 1000.0, 0
  ) AS processed_rows_per_second,
  error_message
FROM ops.pipeline_runs
ORDER BY started_at DESC;

SELECT run_id, stage, elapsed_seconds, status
FROM ops.stage_metrics
ORDER BY recorded_at DESC;

-- Fingerprint matches are candidates, not proven duplicates; no source rows are dropped.
SELECT
  source_month,
  status,
  bronze_rows,
  valid_rows,
  quarantine_rows,
  duplicate_candidate_rows,
  100.0 * quarantine_rows / nullif(bronze_rows, 0) AS quarantine_percentage
FROM ops.file_manifest
ORDER BY source_month;

-- A row may violate multiple rules; these counts must not be summed as rejected rows.
SELECT source_month, error, count(*) AS affected_rows
FROM silver.quarantine
LATERAL VIEW explode(quality_errors) errors AS error
GROUP BY source_month, error
ORDER BY source_month, affected_rows DESC;
