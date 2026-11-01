# Data Quality Baseline & Batch Monitoring

A reproducible, local Data Engineering project that ingests loan-application CSV batches, validates records, stores valid data in SQLite, quarantines rejected records, profiles important columns, compares each batch with a fixed baseline, and produces explainable alerts.

**Target level:** 0–3 years  
**Runtime:** Python 3.10+; SQLite is included with Python. No cloud credentials are required.

## Business problem

Daily data batches can silently change. This project detects changes in:
- **Volume:** incoming valid rows versus the baseline batch.
- **Null rate:** missing-value percentage for `age`, `loan_amount`, and `status`.
- **Distribution:** category shares for `status` and fixed value buckets for numeric columns.
- **Freshness:** age of the newest valid `event_timestamp` compared with the configured maximum lag.

The first non-empty successfully processed batch establishes the baseline. The baseline is intentionally fixed; later batches are compared with it rather than automatically moving the goalposts.

## Architecture

```text
CSV batch + customer reference CSV
              |
              v
          Ingestion
              |
              v
    Schema + business validation
         /             \
        v               v
  Valid records     Quarantine
        |
        v
    SQLite target
        |
        v
 Column profiling + baseline comparison
        |
        v
 Explainable alerts + JSON run report
```

## Quick start

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

Generate a 1,000-row baseline batch:

```bash
PYTHONPATH=src python -m quality_monitor.generator \
  --customers data/input/customers.csv \
  --output data/input/batch_001.csv \
  --rows 1000 --seed 42
```

Run it:

```bash
PYTHONPATH=src python run.py --input data/input/batch_001.csv
```

Generate a later batch with more missing ages and a changed status mix:

```bash
PYTHONPATH=src python -m quality_monitor.generator \
  --customers data/input/customers.csv \
  --output data/input/batch_002.csv \
  --rows 1000 --seed 43 --anomaly
PYTHONPATH=src python run.py --input data/input/batch_002.csv
```

Generate a late-arriving batch:

```bash
PYTHONPATH=src python -m quality_monitor.generator \
  --customers data/input/customers.csv \
  --output data/input/batch_late.csv \
  --rows 1000 --seed 44 --late
PYTHONPATH=src python run.py --input data/input/batch_late.csv
```

Generate a labelled edge-case batch (boundary ages, invalid age, negative amount, missing age, unknown customer, duplicate ID). Expected-answer labels are written to a separate manifest, not the source CSV:

```bash
PYTHONPATH=src python -m quality_monitor.generator \
  --customers data/input/customers.csv \
  --output data/input/edge_cases.csv \
  --rows 1000 --seed 42 --edge-cases
```

Generate a larger batch (100,000 rows):

```bash
PYTHONPATH=src python -m quality_monitor.generator \
  --customers data/input/customers.csv \
  --output data/input/batch_100k.csv \
  --rows 100000 --seed 45
PYTHONPATH=src python run.py --input data/input/batch_100k.csv
```

Run a reproducible local performance measurement:

```bash
python benchmark/run_benchmark.py --rows 100000
```

The script writes `benchmark/benchmark_result.json`. Latest preparation-environment result: 100,000 rows processed in 2.69 seconds (about 37,223 rows/second), with 100,000 accepted and zero rejected. This is machine-specific evidence, not a guaranteed performance target.

Run automated tests:

```bash
pytest -q
```

The CLI writes a JSON report to `reports/`, and the SQLite database defaults to `data/quality_monitor.db`. The terminal prints each numbered processing stage, and the JSON report contains the same `stage_results`, run duration, throughput and reconciliation counts. For a detailed explanation of each role/check and where its output is stored, read `docs/DETAILED_PIPELINE_FLOW.md`.

## Data model and grain

### Source entities

1. **customers** — one row per customer. Primary key: `customer_id`.
2. **loan applications** — one row per loan application/event. Primary key: `application_id`; `customer_id` references `customers.customer_id`.

The application grain is one submitted loan application, not one customer. A customer can have multiple applications.

### Target tables

- `customers`: reference customer master.
- `loan_applications`: accepted application records.
- `rejected_records`: rejected raw record, source filename, source row number, and reason.
- `batch_runs`: operational run metadata and counts.
- `processed_batches`: content hashes used for safe reruns.
- `baseline_profiles`: fixed first-batch profiles.
- `current_profiles`: per-run profiles.
- `quality_alerts`: explainable alerts.

## Data dictionary

| Column | Type | Nullable? | Rule / meaning |
|---|---|---:|---|
| application_id | string | No | Stable unique application key |
| customer_id | string | No | Must exist in customer reference |
| age | integer | Yes | If present, 18–100 inclusive |
| loan_amount | decimal | Yes | If present, positive finite amount |
| status | string | No | approved, rejected, pending |
| event_timestamp | ISO-8601 | No | Event time, including timezone |
| source_file | string | No | Input filename retained in target |
| run_id | string | No | Pipeline run identifier |

`age` and `loan_amount` are nullable so that the profiling layer can measure null-rate changes; invalid non-null values are rejected.

## Quality rules and thresholds

Thresholds are in `config.json`:
- Volume alert: absolute change greater than 30% from baseline.
- Null-rate alert: increase greater than 10 percentage points.
- Distribution alert: largest bucket/share change greater than 25 percentage points.
- Freshness alert: newest valid event is older than 120 minutes.
- Valid statuses: approved, rejected, pending.
- Age range: 18–100 inclusive.

Thresholds are demonstration defaults, not universal business rules. Tune them with data owners.

## Idempotency, retries, and backfills

- The SHA-256 content hash of an input file is stored in `processed_batches`.
- An identical file rerun is recorded as `SKIPPED_ALREADY_PROCESSED` and does not insert target rows again.
- A different file containing an already-loaded `application_id` is quarantined as a duplicate key.
- A failed run is recorded in `batch_runs` with a failure reason. Correct the input/configuration and rerun.
- To intentionally backfill changed data, supply the corrected batch as a new file; records with already-used application IDs remain duplicates unless a controlled correction/upsert policy is added.

## Operational metrics and reconciliation

Each run records run ID, source file, content hash, start/end timestamps, status, input/valid/rejected/output counts, and failure reason. `run.py` prints a reconciliation check:

`input_count = valid_count + rejected_count` and `valid_count = output_count`.

## Tests

The test suite currently has 14 tests and covers:
- Happy path and baseline establishment.
- Empty input.
- Missing required schema/header.
- Unexpected source column.
- Null-rate anomaly.
- Duplicate application ID.
- Invalid customer reference.
- Late-arriving records.
- Same-input rerun/idempotency.
- Negative loan amount.

## Performance measurement and one optimization

The generator supports 100,000+ rows. For a simple local benchmark, run the 100k generator and pipeline and record wall-clock duration. The current implementation reads a CSV batch into memory and profiles accepted rows in memory. A reasonable next optimization for multi-million-row batches is chunked CSV processing with incremental aggregate counters/histograms, while preserving deterministic validation and batch-level transactions.

## Assumptions and limitations

- Local CSV files represent the source producer; SQLite represents the target/metadata store.
- The customer reference file is loaded before application validation.
- The first non-empty valid batch establishes the baseline; no automatic baseline retraining is performed.
- Numeric distribution monitoring uses fixed business-readable buckets; categorical monitoring uses value shares.
- Freshness is based on the newest valid event timestamp and the pipeline's UTC processing time.
- Unexpected extra columns are ignored but generate a schema-change alert. Missing required columns fail the batch.
- The project is a focused vertical slice, not a production scheduler or distributed processing platform.

## Sample execution evidence

See `sample_outputs/` for a real edge-case run report, accepted/rejected row samples, and a second-batch report showing explainable null-rate and status-distribution alerts. `sample_execution.json` is the edge-case report at the project root.

## Assignment coverage map

See `docs/ACCEPTANCE_COVERAGE.md` for a requirement-by-requirement mapping, evidence locations and known limitations. See `docs/DETAILED_PIPELINE_FLOW.md` for actors, numbered processing stages, checks, alert logic, retry behaviour and edge-case generation.

## Suggested next steps

1. Add a configurable baseline version and approval workflow.
2. Add chunked processing and incremental profiles for very large files.
3. Publish alerts to email/Slack and schedule runs.
4. Move storage to ADLS Gen2 and processing to Databricks if cloud execution is required.
5. Add drift metrics such as PSI or statistical tests for larger numeric datasets.
