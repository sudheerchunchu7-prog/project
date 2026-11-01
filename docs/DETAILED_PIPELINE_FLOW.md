# Detailed Pipeline Walkthrough (What Runs, What Checks, and Where to See It)

## 1. Business scenario

A fictional lending company exports loan applications as daily CSV files. The source producer sends one file per batch. This local project simulates that producer with synthetic data; it does not connect to a real lender or production API.

## 2. Actors and responsibilities

| Actor/system | Responsibility in this demo |
|---|---|
| Source producer | Synthetic generator writes customer reference and loan application CSV files. |
| Data engineer | Configures rules, runs batches, investigates rejects and alerts. |
| Pipeline operator | Starts a run, watches stage logs, checks failures, and retries after fixing causes. |
| Processing layer | Python reads, validates, profiles and compares batches. |
| Storage layer | SQLite stores accepted rows, rejects, profiles, alerts and run metadata. |
| Analytical consumer | Reads accepted loan application rows and run reports. |

There are no user-account/security roles in this starter solution. The table above describes system responsibilities, not RBAC permissions.

## 3. Grain, keys, and relationships

- `customers`: one row per customer; primary key `customer_id`.
- `loan_applications`: one row per application event; primary key `application_id`; foreign key `customer_id` references `customers.customer_id`.
- `rejected_records`: one rejected source row per rejection event, with `run_id`, `source_file`, `source_row_number`, raw JSON and reason.
- `batch_runs`: one row per pipeline attempt; `run_id` is the primary key.
- `processed_batches`: one row per SHA-256 content hash; prevents identical input content being loaded twice.
- `baseline_profiles`: one fixed baseline profile per monitored column.
- `current_profiles`: one profile per run and column.
- `quality_alerts`: one row per explainable alert.

## 4. What happens when one batch is run?

Command example:

```powershell
$env:PYTHONPATH = "src"
py run.py --input data/input/batch_001.csv
```

The terminal logs these stages with a run ID:

1. `01_INGESTION` — reads the CSV and counts data rows.
2. `02_REFERENCE_LOAD` — loads customer IDs from the customer reference file.
3. `03_SCHEMA_VALIDATION` — verifies required headers and reports unexpected columns. Missing required headers fail the batch; extra headers are ignored but alert.
4. `04_RECORD_VALIDATION` — validates each record, including required IDs, reference integrity, status, age, amount and timestamp; identifies duplicates within the batch and target.
5. `05_TARGET_PUBLISH` — inserts accepted rows and stores rejected raw rows with source location and reasons.
6. `06_DATA_PROFILING` — calculates row count, null count/rate, distinct count, min/max/mean/stddev for numeric columns, and distributions.
7. `07_BASELINE_AND_MONITORING` — establishes the first non-empty accepted batch as baseline; compares later batches and produces explainable alerts.
8. `08_RECONCILIATION` — verifies input = accepted + rejected and accepted = target output.
9. `09_REPORTING` — writes JSON report with run ID, status, counts, alert explanations, stage results and duration.

The report includes `stage_results` so the sequence is also visible after execution. The `batch_runs`, `rejected_records`, `current_profiles`, `baseline_profiles`, and `quality_alerts` tables preserve the operational evidence.

## 5. Rules and alert explanations

- Required application fields: application ID, customer ID, status and event timestamp.
- `application_id` must be non-empty and unique in both the current batch and target.
- `customer_id` must exist in the customer reference file.
- Status must be one of approved, rejected, pending.
- Age, when supplied, must be an integer from 18 through 100 inclusive. A missing age is permitted and counted in null profiling.
- Loan amount, when supplied, must be a positive finite number. A missing amount is permitted and counted in null profiling.
- Event timestamp must parse as ISO-8601.
- Volume alert: absolute accepted-row-count change exceeds configured threshold.
- Null-rate alert: increase in null percentage exceeds configured percentage-point threshold.
- Distribution alert: largest category/bucket share movement exceeds configured threshold.
- Freshness alert: newest accepted event is older than configured maximum lag.
- Schema drift alert: unexpected columns are found.

Alerts include baseline value, current value and a human-readable explanation. Thresholds are configurable in `config.json`; they are demonstration defaults and should be reviewed with a data owner in production.

## 6. Batch behaviour and recovery

- Identical file content is identified by SHA-256 and skipped on rerun.
- A new file with an application ID already in the target is quarantined as a duplicate.
- A failure is recorded in `batch_runs` with `FAILED` status and a failure reason.
- After fixing a failed dependency/input, retry the same file. A failed attempt is not marked as processed, so the retry can run.
- A backfill with new application IDs can be processed as a new file. Correcting already-published application IDs requires a governed correction/upsert policy; this starter project deliberately does not overwrite accepted history.
- Each accepted row retains `source_file` and `run_id`; each rejected row retains source filename and source row number.

## 7. Reproducible edge-case generator

```powershell
py -m quality_monitor.generator --customers data/input/customers.csv --output data/input/edge_cases.csv --rows 1000 --seed 42 --edge-cases
```

This adds boundary ages (18 and 100), an out-of-range age, negative amount, missing age, unknown customer reference and duplicate application ID. Expected-case labels are written to a separate `.manifest.json` sidecar, not into source business data.

Other scenarios:

```powershell
# Increased nulls and changed status mix
py -m quality_monitor.generator --customers data/input/customers.csv --output data/input/anomaly.csv --rows 1000 --seed 43 --anomaly

# Late events (approximately five hours old)
py -m quality_monitor.generator --customers data/input/customers.csv --output data/input/late.csv --rows 1000 --seed 44 --late
```

The generator uses a fixed pseudo-random seed for generated values. Event timestamps are anchored to generation time, so repeated generation at different times can produce different timestamp values. For exact byte-for-byte reruns, reuse the generated CSV file rather than regenerating it.

## 8. Performance

Run a 100,000-row local measurement:

```powershell
py benchmark/run_benchmark.py --rows 100000
```

It writes `benchmark/benchmark_result.json`. Results depend on the computer, Python version, disk and antivirus configuration; do not treat one machine's rate as a production guarantee. A reasonable next optimization for larger-than-memory files is chunked CSV reading with incremental profiling counters and bulk inserts inside controlled transactions.
