-- ═══════════════════════════════════════════════════════════════════════════════
-- Sample SQL Queries for Lending Pipeline Analysis
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- These queries help inspect and analyze pipeline execution results
-- Run them in: sqlite3 database.db
--
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: EXECUTION TRACKING
-- ═══════════════════════════════════════════════════════════════════════════════

-- View all pipeline runs
SELECT 
    run_id,
    COUNT(*) as stages,
    MIN(start_time) as started,
    MAX(end_time) as completed,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_stages,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_stages
FROM EXECUTION_LOGS
GROUP BY run_id
ORDER BY MIN(start_time) DESC;

-- View execution log for a specific run
SELECT 
    run_id,
    pipeline_stage,
    status,
    start_time,
    end_time,
    ROUND((julianday(end_time) - julianday(start_time)) * 1000, 0) as duration_ms,
    input_row_count,
    output_row_count,
    rejected_row_count,
    error_message
FROM EXECUTION_LOGS
WHERE run_id = 'run_20241114_180105'
ORDER BY start_time;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: DATA VALIDATION & QUALITY
-- ═══════════════════════════════════════════════════════════════════════════════

-- Data quality summary by run
SELECT 
    run_id,
    validation_rule,
    total_records,
    passed_count,
    failed_count,
    ROUND(100.0 * passed_count / total_records, 1) as pass_percentage
FROM DATA_QUALITY_REPORT
ORDER BY run_id DESC, pass_percentage ASC;

-- Overall quality by run
SELECT 
    run_id,
    COUNT(DISTINCT validation_rule) as validation_rules_checked,
    SUM(total_records) as total_validated,
    SUM(passed_count) as total_passed,
    SUM(failed_count) as total_failed,
    ROUND(100.0 * SUM(passed_count) / SUM(total_records), 2) as overall_quality_pct
FROM DATA_QUALITY_REPORT
GROUP BY run_id
ORDER BY run_id DESC;

-- Rejected records by reason
SELECT 
    rejection_reason_code,
    rejection_reason_text,
    COUNT(*) as count,
    run_id
FROM REJECTED_RECORDS
GROUP BY run_id, rejection_reason_code, rejection_reason_text
ORDER BY run_id DESC, count DESC;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: CURATED OUTPUT ANALYSIS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Loan applications by category and status
SELECT 
    loan_amount_category,
    application_status,
    COUNT(*) as count,
    ROUND(AVG(loan_amount), 2) as avg_amount,
    ROUND(MIN(loan_amount), 2) as min_amount,
    ROUND(MAX(loan_amount), 2) as max_amount
FROM CURATED_OUTPUT
WHERE run_id = (SELECT run_id FROM CURATED_OUTPUT ORDER BY created_at DESC LIMIT 1)
GROUP BY loan_amount_category, application_status
ORDER BY loan_amount_category, application_status;

-- Applications by region and segment
SELECT 
    region,
    customer_segment,
    COUNT(*) as application_count,
    ROUND(AVG(loan_amount), 2) as avg_loan,
    ROUND(AVG(age_at_application), 1) as avg_age,
    SUM(CASE WHEN application_status = 'APPROVED' THEN 1 ELSE 0 END) as approved,
    SUM(CASE WHEN application_status = 'REJECTED' THEN 1 ELSE 0 END) as rejected
FROM CURATED_OUTPUT
WHERE run_id = (SELECT run_id FROM CURATED_OUTPUT ORDER BY created_at DESC LIMIT 1)
GROUP BY region, customer_segment
ORDER BY application_count DESC;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 4: PERFORMANCE COMPARISON (Baseline vs Optimized)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Baseline vs Optimized performance
SELECT 
    run_id,
    ROUND(baseline_duration_ms / 1000.0, 2) as baseline_seconds,
    ROUND(optimized_duration_ms / 1000.0, 2) as optimized_seconds,
    ROUND(100.0 * (baseline_duration_ms - optimized_duration_ms) / baseline_duration_ms, 1) as speedup_pct,
    baseline_shuffle_operations,
    optimized_shuffle_operations,
    (baseline_shuffle_operations - optimized_shuffle_operations) as shuffle_reduction,
    baseline_join_strategy,
    optimized_join_strategy
FROM PERFORMANCE_METRICS
ORDER BY run_id DESC;

-- Performance trend (if multiple runs)
SELECT 
    run_id,
    baseline_duration_ms,
    optimized_duration_ms,
    improvement_percentage,
    measured_at
FROM PERFORMANCE_METRICS
ORDER BY measured_at ASC;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 5: INPUT DATA STATISTICS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Customer statistics
SELECT 
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT region) as regions,
    COUNT(DISTINCT customer_segment) as segments
FROM CUSTOMERS;

-- Customer distribution by region and segment
SELECT 
    region,
    customer_segment,
    COUNT(*) as count
FROM CUSTOMERS
GROUP BY region, customer_segment
ORDER BY count DESC;

-- Loan application statistics
SELECT 
    COUNT(*) as total_applications,
    COUNT(DISTINCT application_id) as unique_applications,
    COUNT(DISTINCT customer_id) as customers_with_apps,
    ROUND(AVG(loan_amount), 2) as avg_loan,
    ROUND(MIN(loan_amount), 2) as min_loan,
    ROUND(MAX(loan_amount), 2) as max_loan,
    ROUND(STDEV(loan_amount), 2) as stdev_loan
FROM LOAN_APPLICATIONS;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 6: VALIDATION STATISTICS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Valid vs rejected counts by run
SELECT 
    run_id,
    (SELECT COUNT(*) FROM VALID_RECORDS WHERE VALID_RECORDS.run_id = EXECUTION_LOGS.run_id) as valid_count,
    (SELECT COUNT(*) FROM REJECTED_RECORDS WHERE REJECTED_RECORDS.run_id = EXECUTION_LOGS.run_id) as rejected_count
FROM EXECUTION_LOGS
WHERE pipeline_stage = 'validation'
GROUP BY run_id
ORDER BY run_id DESC;

-- Top rejection reasons
SELECT 
    rejection_reason_code,
    COUNT(*) as occurrences,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM REJECTED_RECORDS), 2) as percentage
FROM REJECTED_RECORDS
GROUP BY rejection_reason_code
ORDER BY occurrences DESC;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 7: VIEWS (Pre-built Queries)
-- ═══════════════════════════════════════════════════════════════════════════════

-- View execution summary (all runs)
SELECT * FROM v_execution_summary;

-- View quality summary (all runs)
SELECT * FROM v_quality_summary;

-- View performance comparison (all runs)
SELECT * FROM v_performance_comparison;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 8: DEBUGGING & INSPECTION
-- ═══════════════════════════════════════════════════════════════════════════════

-- List all tables and row counts
SELECT 
    name as table_name,
    CASE 
        WHEN name = 'CUSTOMERS' THEN (SELECT COUNT(*) FROM CUSTOMERS)
        WHEN name = 'LOAN_APPLICATIONS' THEN (SELECT COUNT(*) FROM LOAN_APPLICATIONS)
        WHEN name = 'VALID_RECORDS' THEN (SELECT COUNT(*) FROM VALID_RECORDS)
        WHEN name = 'REJECTED_RECORDS' THEN (SELECT COUNT(*) FROM REJECTED_RECORDS)
        WHEN name = 'CURATED_OUTPUT' THEN (SELECT COUNT(*) FROM CURATED_OUTPUT)
        WHEN name = 'EXECUTION_LOGS' THEN (SELECT COUNT(*) FROM EXECUTION_LOGS)
        WHEN name = 'PERFORMANCE_METRICS' THEN (SELECT COUNT(*) FROM PERFORMANCE_METRICS)
        WHEN name = 'DATA_QUALITY_REPORT' THEN (SELECT COUNT(*) FROM DATA_QUALITY_REPORT)
        ELSE 0
    END as row_count
FROM sqlite_master
WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'sqlite_sequence%'
ORDER BY name;

-- View recent execution logs (latest 20 entries)
SELECT 
    log_id,
    run_id,
    pipeline_stage,
    status,
    start_time,
    duration_ms,
    input_row_count,
    output_row_count,
    rejected_row_count
FROM EXECUTION_LOGS
ORDER BY log_id DESC
LIMIT 20;

-- Export curated output to CSV format
-- Run: sqlite3 database.db "SELECT * FROM CURATED_OUTPUT;" | head -20
-- Or save to file: .mode csv
--                  .output output.csv
--                  SELECT * FROM CURATED_OUTPUT;

-- ═══════════════════════════════════════════════════════════════════════════════
-- QUICK INSPECTION COMMANDS
-- ═══════════════════════════════════════════════════════════════════════════════

-- Open database and run these in sqlite3:
-- sqlite3 database.db
-- .tables                           -- List all tables
-- .schema CURATED_OUTPUT           -- View table schema
-- SELECT * FROM v_execution_summary;  -- View runs
-- SELECT * FROM v_performance_comparison;  -- Compare performance
