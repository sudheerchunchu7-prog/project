/* Run after 01_sqlserver_schema.sql in the dedicated demo database. */

-- 1. Row counts by application status
SELECT status, COUNT_BIG(*) AS application_count
FROM dbo.loan_applications
GROUP BY status
ORDER BY application_count DESC;
GO

-- 2. Reconciliation by batch: input = valid + rejected; valid = output
SELECT run_id, source_file, status, input_count, valid_count,
       rejected_count, output_count,
       CASE WHEN input_count = valid_count + rejected_count
                  AND valid_count = output_count
            THEN 'PASS' ELSE 'CHECK' END AS reconciliation_status
FROM dbo.batch_runs
ORDER BY started_at DESC;
GO

-- 3. Rejected records and their source traceability
SELECT TOP (100) run_id, source_file, source_row_number,
       application_id, reject_reason, raw_record
FROM dbo.rejected_records
ORDER BY rejected_at DESC;
GO

-- 4. Recent explainable quality alerts
SELECT TOP (100) run_id, alert_type, column_name, severity,
       baseline_value, current_value, explanation, created_at
FROM dbo.quality_alerts
ORDER BY created_at DESC;
GO

-- 5. Duplicate business keys (should return no rows)
SELECT application_id, COUNT(*) AS duplicate_count
FROM dbo.loan_applications
GROUP BY application_id
HAVING COUNT(*) > 1;
GO

-- 6. Referential-integrity check (should return no rows)
SELECT l.application_id, l.customer_id
FROM dbo.loan_applications AS l
LEFT JOIN dbo.customers AS c ON c.customer_id = l.customer_id
WHERE c.customer_id IS NULL;
GO
