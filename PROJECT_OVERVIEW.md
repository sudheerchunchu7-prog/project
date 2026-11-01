# YottaFlex Data Quality Monitor — project overview

This repository is a reproducible local batch data-quality monitoring demo. Start with `README.md`, then read `docs/DETAILED_PIPELINE_FLOW.md`, `docs/ACCEPTANCE_COVERAGE.md`, `docs/PROJECT_WALKTHROUGH.md` and `docs/WINDOWS_SETUP_AND_GITHUB.md`.

**Execution path:** CSV files → Python ingestion → schema/business validation → valid/rejected split → SQLite storage → profiling → baseline comparison → explainable alerts + stage-by-stage console logs + JSON run report with reconciliation and timing.

**SSMS companion:** `sql/` contains optional SQL Server/Azure SQL DDL and inspection queries. The current Python execution path uses SQLite; it is not yet connected to SQL Server. This distinction is documented deliberately so the project does not overstate its implementation.
