# Project walkthrough — understand the complete system

## 1. Business problem

A lending business receives loan-application CSV files every day. A file may contain missing values, invalid ages, negative loan amounts, unknown customers, duplicate application IDs, late events, or a changed mix of loan statuses. If bad data enters reporting tables silently, business dashboards and decisions become unreliable.

This project creates a small, explainable batch data-quality pipeline that detects and reports those issues.

## 2. End-to-end flow

1. **Source** — `data/input/` contains a customer reference file and sample loan-application batches.
2. **Ingestion** — Python reads the selected CSV file and records the source file and row number.
3. **Schema checks** — required columns must exist. Unexpected columns are surfaced as schema-change information.
4. **Business validation** — checks application ID, customer reference, status, age, loan amount, and event timestamp.
5. **Split** — valid rows continue; invalid rows are written to the rejected/quarantine output with a reason.
6. **Load** — accepted rows are stored in the local SQLite database by default.
7. **Idempotency** — SHA-256 file hash prevents the exact same file from being loaded twice.
8. **Profiling** — measures row volume, null rates, value distributions, and event freshness.
9. **Baseline comparison** — compares later batches with the first successful non-empty batch.
10. **Alerts/report** — writes explainable alerts and a JSON run report; checks row-count reconciliation.

## 3. Example scenario

- Batch 001 is the healthy baseline. It establishes the expected volume, null rates, status distribution, and freshness.
- Batch 002 intentionally has more missing ages and a changed status mix. The pipeline should identify the shifts.
- Batch 003 contains late events. The freshness check should identify the delay.
- Running Batch 001 again should be skipped as already processed, rather than inserting duplicate data.

## 4. What each folder does

| Path | Purpose |
|---|---|
| `run.py` | Main command-line entry point |
| `src/quality_monitor/pipeline.py` | Main ingestion, validation, loading, profiling, alerting logic |
| `src/quality_monitor/generator.py` | Generates reproducible sample batches |
| `src/quality_monitor/cli.py` | Command-line arguments and execution |
| `data/input/` | Small sample input CSV files |
| `reports/` | Example JSON run reports |
| `tests/` | Automated checks of core behavior |
| `config.json` | Alert thresholds and validation configuration |
| `sql/` | Optional SQL Server/Azure SQL schema and inspection queries for SSMS |
| `docs/` | Design, data dictionary, and walkthrough |
| `benchmark/` | Larger synthetic batch and environment-specific benchmark notes |

## 5. Where SSMS fits

The tested default implementation uses SQLite, which is included with Python and needs no database server. The `sql/` folder contains a SQL Server-compatible schema and useful queries so you can inspect the target model in SSMS (including an Azure SQL database you already have access to).

Important: the Python pipeline does **not** automatically write into SQL Server in this version. The SQL scripts are a database design/inspection companion, not a claim that the Python-to-SQL Server connector has been configured or tested. A SQL Server adapter can be added after confirming the server, database, authentication method, and ODBC driver. Do not put passwords or connection strings in GitHub.

## 6. How to explain the design in an interview

"I built a batch data-quality monitoring pipeline for loan application data. It reads CSV batches, validates schema and business rules, separates invalid records into quarantine, and loads valid records into a local analytical store. It uses file hashing for idempotency, tracks batch-level reconciliation, and profiles volume, null rates, distributions, and freshness against a fixed baseline. When a metric crosses a configurable threshold, it creates an explainable alert and JSON run report. I used local components so the project can be reproduced without paid cloud services."

## 7. Limitations to disclose

- It is a local batch-processing demonstration, not a distributed production platform.
- SQLite is the default tested store; the SQL Server scripts are a companion schema, not an integrated SQL Server sink.
- No scheduler or alert delivery integration is included.
- The first successful non-empty batch becomes the baseline; baseline approval/versioning is future work.
- Large-scale streaming, multi-million-row chunk processing, and production access controls are outside this scope.
