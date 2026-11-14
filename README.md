# PySpark Lending Data Pipeline

A local, reproducible lending-data pipeline demonstrating ingestion, validation/quarantine, business transformations, baseline vs optimized Spark joins, SQLite persistence, safe reruns, tests and a benchmark. It is designed for learning and for a single-machine assignment—not as a production lending platform.

## 1. Business flow

```text
Customer CSV + Loan Application CSV
              |
       PySpark ingestion
              |
   Validation + quarantine
       |              |
 valid customers   rejected rows
       |
 Join applications to customers
       |
 Age + loan-size category
       |
 BASELINE join / OPTIMIZED broadcast join
       |
 SQLite: customers, loan_applications, rejects, runs, batches, errors
       |
 JSON run report + execution plan + SQL checks
```

## 2. Technology

- Python 3.12 (recommended for the supplied PySpark 3.5.3 setup)
- Java 17 runtime (Spark uses the JVM; application code is Python)
- PySpark 3.5.3
- SQLite via Python's built-in `sqlite3`
- CSV, pytest, Git

## 3. Setup on Windows PowerShell

Open this folder in VS Code. Create/activate a virtual environment if needed:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

If PowerShell blocks activation, use the interpreter directly instead:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Select `.venv\Scripts\python.exe` using **Ctrl+Shift+P → Python: Select Interpreter**. Confirm Java is available with `java -version` and `$env:JAVA_HOME` points to the JDK root.

## 4. Generate reproducible data

```powershell
.\.venv\Scripts\python.exe run.py generate --customers 100 --applications 250 --seed 42 --out data/sample
```

The same seed and counts produce the same CSV contents. Customer and application inputs are separate files. `data/test_scenarios/` is reserved for deliberately invalid fixtures; use `generate_negative_scenarios` from the generator module to create examples.

## 5. Run the pipeline

```powershell
.\.venv\Scripts\python.exe run.py run --mode BASELINE
.\.venv\Scripts\python.exe run.py run --mode OPTIMIZED
```

Both modes use the same sample files. `BASELINE` uses a regular Spark join. `OPTIMIZED` broadcasts the valid customer dimension. Broadcast is appropriate only when the customer side is small enough for executor memory; measure it on your dataset. The Spark plan and duration are written into `reports/generated/`.

To use other input files or a separate database:

```powershell
.\.venv\Scripts\python.exe run.py run --customers data/sample/customers.csv --applications data/sample/loan_applications.csv --mode BASELINE --db database/demo.db
```

The database is created automatically at `database/lending_pipeline.db` by default. Live `.db` files are ignored by Git.

## 6. Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

The Spark integration test runs when PySpark is installed. Other tests cover deterministic generation, schema creation, batch idempotency keys and SQLite foreign keys.

## 7. Benchmark

```powershell
.\.venv\Scripts\python.exe benchmark/run_benchmark.py --customers 1000 --applications 5000 --seed 42
```

It runs the same generated inputs through both modes, compares business output rows, and saves plans and timings to `benchmark/results/latest_comparison.json`. Small local datasets may not show a speedup: Spark startup and broadcast setup can cost more than the work saved. Treat measured results as evidence, not a promise.

## 8. SQLite inspection

Open `database/lending_pipeline.db` with a SQLite viewer or run SQL from `reports/example_queries.sql`. The database stores actual curated customer and loan application records, rejects, run metadata, processed-batch keys and errors. SQLite is free and convenient for a local assignment. It supports many readers but serializes writes, so this project uses one controlled writer. It is not a distributed production database.

## 9. Data model and rules

**Customers**: one row per `customer_id`; customer name, date of birth, region, segment and creation timestamp are required.

**Loan applications**: one row per application event; `application_id` identifies the business application. A customer can have many applications. Each application must reference a valid customer.

- Status values: `APPROVED`, `REJECTED`, `PENDING` (case-normalized).
- Loan amount: greater than zero and at most the configurable INR 5,000,000 default (`config/settings.json`).
- Age: calculated at the application timestamp, not today's date.
- Categories: SMALL below INR 100,000; MEDIUM from INR 100,000 through INR 300,000; LARGE above INR 300,000.
- Timestamps use ISO-8601. Field names use snake_case.
- Duplicate business keys are quarantined in full; no arbitrary winner is selected.
- `source_file`, `source_row_number`, `run_id`, and `processing_mode` support lineage.

## 10. Reliability and limitations

The SHA-256 fingerprint covers both input files. A previously successful combination of file hash, processing mode and transformation version is skipped. BASELINE and OPTIMIZED can each process the same input once. Failed publishing is rolled back and is not marked as processed. Successful history is retained across runs.

Known limitations to review before submission:

1. Spark's `monotonically_increasing_id()` is used as a source row identifier. It is a generated lineage token, not a guaranteed physical CSV line number across repartitioning/reruns. For strict physical line numbers, ingest raw text with an explicit line-indexing strategy.
2. CSV fields are initially read as strings so malformed values can be validated; the Spark schema is explicit for the source column names, while domain casts happen during validation.
3. The optimization currently demonstrates a broadcast join. It does not assert that the optimized plan is faster or has fewer shuffles; inspect the actual plan and benchmark JSON.
4. Spark DataFrame output is streamed to a single SQLite writer with `toLocalIterator()` and bounded `executemany` batches. Spark still executes locally; SQLite is not a distributed sink.
5. Late-arriving records are retained as application events, but no separate event-time watermark or SCD history model is implemented.
6. A schema change that removes/renames a required CSV header may result in null fields and quarantine; a dedicated schema-version registry is not implemented.
7. This is an educational baseline. Authentication, encryption, PII governance, orchestration, alerting and production deployment are out of scope.

## 11. Interview explanation

“I built a local lending data pipeline using PySpark and SQLite. It reads customer and loan application CSV files, validates mandatory fields and business keys, quarantines invalid records with reasons, joins applications to valid customers, calculates age at application time and classifies loan amounts. I implemented a straightforward baseline join and an optimized broadcast join, then captured execution plans and timings using the same input data. Curated records, rejected records and run metadata are stored in SQLite. A SHA-256 batch key makes successful reruns idempotent by input, processing mode and transformation version. I used pytest for repeatability, database constraints and Spark transformation checks. I also documented the limits of local Spark and SQLite.”

## 12. Requirement checklist

See [`docs/REQUIREMENT_CHECKLIST.md`](docs/REQUIREMENT_CHECKLIST.md). Statuses are conservative: implementation does not equal tested evidence.
