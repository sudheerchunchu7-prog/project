# Requirement-to-evidence checklist

Status definitions: NOT STARTED, IN PROGRESS, IMPLEMENTED, TESTED, NOT IMPLEMENTED. Re-run tests and benchmark in the target Windows environment before submission.

| Requirement | Status in this package | Evidence / caveat |
|---|---|---|
| Source/target schemas and business rules | IMPLEMENTED | `config/settings.json`, `database/schema.sql`, README |
| Reproducible sample/scalable generator | IMPLEMENTED | `src/lending_pipeline/generator.py`; generator tests |
| Separate customer and application inputs | IMPLEMENTED | generator writes two CSV files |
| PySpark ingestion with explicit column schemas | IMPLEMENTED | `spark_pipeline.py`; CSV values initially strings |
| Validation and reject quarantine | IMPLEMENTED | `validate_and_transform`; rejects retain payload and lineage |
| Duplicate keys quarantined deterministically | IMPLEMENTED | all rows for duplicated non-null keys rejected |
| Business transformations and join | IMPLEMENTED | age-at-application, category, status normalization |
| Baseline and optimized paths | IMPLEMENTED | ordinary join vs broadcast customer join |
| Plan, duration, partition evidence | IMPLEMENTED | per-run JSON and benchmark JSON; metrics require execution |
| SQLite stores actual business data | IMPLEMENTED | `customers`, `loan_applications`, `rejected_records` |
| Run metadata and error records | IMPLEMENTED | `pipeline_runs`, `pipeline_errors` |
| Bounded single-writer SQLite publishing | IMPLEMENTED | `toLocalIterator()` + bounded `executemany` batches |
| Idempotency and retry-safe batch record | IMPLEMENTED | SHA-256 + mode + transformation version; success recorded in transaction |
| Reconciliation and output equivalence | IN PROGRESS | benchmark compares curated business columns; broader count checks need review |
| Automated tests | IMPLEMENTED | `tests/`; must run in user's venv for final evidence |
| Performance benchmark | IMPLEMENTED | `benchmark/run_benchmark.py`; no guaranteed speedup |
| Physical source row number | NOT IMPLEMENTED | Spark monotonic ID is a lineage token, not a physical CSV line number |
| Late-arrival event-time handling/watermarks | NOT IMPLEMENTED | Events retained, no watermark strategy |
| Dedicated schema evolution registry | NOT IMPLEMENTED | malformed/missing values quarantined; no schema registry |
| Dashboard/cloud/orchestration | NOT IMPLEMENTED | explicitly outside local mandatory scope |
