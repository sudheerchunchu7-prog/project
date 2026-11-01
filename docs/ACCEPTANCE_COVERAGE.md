# Assignment Acceptance Coverage

| Assignment requirement | Project implementation / evidence |
|---|---|
| Business context and baseline monitoring | `README.md`, `docs/DETAILED_PIPELINE_FLOW.md`; first non-empty accepted batch creates fixed baseline profiles. |
| Volume, null-rate, distribution, freshness alerts | `compare_profiles()` and freshness logic in `src/quality_monitor/pipeline.py`; explanations saved to `quality_alerts` and run JSON. |
| Source/target schema and assumptions | `docs/data_dictionary.md`, `sql/01_sqlserver_schema.sql`, SQLite DDL in `connect_db()`. |
| Sample data and scalable generator | `src/quality_monitor/generator.py`; `--rows`, `--seed`, `--anomaly`, `--late`, `--edge-cases`. |
| Ingestion, validation, transformation, publishing | `process_batch()`; stage logs and `stage_results` in JSON report. |
| Safe reruns / idempotency | SHA-256 `processed_batches`; existing target application IDs are rejected. |
| Run status, counts, rejects, errors | `batch_runs`, `rejected_records`, CLI summary and JSON report. |
| Stable IDs and naming | Source business keys retained; generated run ID; snake_case tables/columns; ISO-8601 UTC timestamps. |
| Traceable rejects | Raw record, source filename, source row number, application ID and reject reason stored. |
| History | Application rows are not silently overwritten; run and reject history retained. Corrections to existing application IDs need a governed future policy. |
| Tests: happy path, empty, malformed, duplicates, nulls | `tests/test_pipeline.py`. |
| Domain-specific edge cases | Invalid age, negative amount, unknown customer, null age, late timestamps, status distribution shift. |
| Partial failure and retry | `test_failed_run_can_be_retried_after_reference_file_is_fixed`. Failed attempts are recorded and not added to processed hashes. |
| Reconciliation | `reconciliation()` plus SQL Server companion query in `sql/02_sqlserver_useful_queries.sql`. |
| Performance measurement | `benchmark/run_benchmark.py`; latest local result in `benchmark/benchmark_result.json`. |
| One optimization | Chunked CSV processing and incremental profiles are documented as next optimization. |
| Documentation and trade-offs | README, data dictionary, detailed walkthrough, design note and setup guide. |

## Known scope limitations (be transparent)

- The fully tested Python target is SQLite. SQL Server scripts are a companion schema/query pack; Python is not currently wired to SQL Server.
- CSV files simulate the source producer. There is no live API, scheduler, message queue or cloud integration.
- Files are read into memory. Chunked processing is the next scale optimization.
- Distribution checks use fixed, explainable buckets/categories, not advanced statistical drift methods.
- Event freshness is checked against the current batch's newest event. The solution does not enforce strict event-time ordering within a file.
- A controlled upsert/correction process for previously published application IDs is out of scope; duplicates are quarantined instead.
