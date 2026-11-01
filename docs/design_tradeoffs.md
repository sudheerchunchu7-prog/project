# Design and Trade-off Note

## Why Python + SQLite?
This is a small, reproducible local vertical slice. Python provides ingestion, validation and profiling; SQLite provides durable SQL tables without cloud setup or credentials.

## Why a fixed baseline?
A fixed baseline makes changes explainable and prevents a slowly degrading source from redefining its own expected behavior. In production, baseline changes should be versioned and approved.

## Why fixed numeric buckets?
They are easy to explain and deterministic. They are less statistically sensitive than PSI, KS tests or quantile-based drift detection, but are appropriate for a 72-hour beginner-level assignment.

## Why quarantine rather than drop?
The raw record, source filename, row number and rejection reason are retained so an operator can trace and correct the source issue.

## Limitations
The CSV is loaded into memory. The baseline is global rather than partitioned by source or business segment. Freshness uses processing time as the reference clock. These are explicit scope choices, not hidden guarantees.
