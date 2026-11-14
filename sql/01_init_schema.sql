-- ═══════════════════════════════════════════════════════════════════════════════
-- SQLite Schema for Lending Data Pipeline - Database-Centric Approach
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Setup:
--   sqlite3 database.db < sql/01_init_schema.sql
--
-- Tables:
--   1. Business data: CUSTOMERS, LOAN_APPLICATIONS, VALID_RECORDS, REJECTED_RECORDS
--   2. Output: CURATED_OUTPUT (final business output)
--   3. Execution tracking: EXECUTION_LOGS, PERFORMANCE_METRICS, DATA_QUALITY_REPORT
--
-- Author: Lending Pipeline Team
-- ═══════════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: INPUT TABLES (Raw Data)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS CUSTOMERS (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    date_of_birth TEXT NOT NULL,
    region TEXT NOT NULL CHECK (region IN ('NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL', 'NORTHEAST')),
    customer_segment TEXT NOT NULL CHECK (customer_segment IN ('RETAIL', 'CORPORATE', 'GOVT')),
    created_at TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS LOAN_APPLICATIONS (
    application_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    loan_amount DECIMAL(18, 2) NOT NULL CHECK (loan_amount > 0 AND loan_amount <= 5000000),
    application_status TEXT NOT NULL CHECK (application_status IN ('APPROVED', 'REJECTED', 'PENDING')),
    application_timestamp TEXT NOT NULL,
    source_system TEXT NOT NULL,
    updated_at TEXT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    UNIQUE (application_id, run_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: PROCESSING TABLES (Valid & Rejected Records)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS VALID_RECORDS (
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    age_at_application INTEGER NOT NULL,
    loan_amount DECIMAL(18, 2) NOT NULL,
    loan_amount_category TEXT NOT NULL CHECK (loan_amount_category IN ('SMALL', 'MEDIUM', 'LARGE')),
    application_status TEXT NOT NULL,
    application_timestamp TEXT NOT NULL,
    region TEXT NOT NULL,
    customer_segment TEXT NOT NULL,
    run_id TEXT NOT NULL,
    processing_mode TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS REJECTED_RECORDS (
    rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id TEXT,
    customer_id TEXT,
    rejection_reason_code TEXT NOT NULL,
    rejection_reason_text TEXT NOT NULL,
    rejected_data TEXT,
    run_id TEXT NOT NULL,
    source_file TEXT,
    source_row_number INTEGER,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: FINAL OUTPUT TABLE (Business Output)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS CURATED_OUTPUT (
    output_id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    age_at_application INTEGER NOT NULL,
    loan_amount DECIMAL(18, 2) NOT NULL,
    loan_amount_category TEXT NOT NULL,
    application_status TEXT NOT NULL,
    application_timestamp TEXT NOT NULL,
    region TEXT NOT NULL,
    customer_segment TEXT NOT NULL,
    run_id TEXT NOT NULL,
    baseline_output BOOLEAN DEFAULT 0,
    optimized_output BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 4: EXECUTION LOGGING TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS EXECUTION_LOGS (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    pipeline_stage TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed', 'partial')),
    input_row_count INTEGER,
    output_row_count INTEGER,
    rejected_row_count INTEGER,
    duration_ms INTEGER,
    error_message TEXT,
    json_log TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS PERFORMANCE_METRICS (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    baseline_duration_ms INTEGER,
    optimized_duration_ms INTEGER,
    baseline_shuffle_operations INTEGER,
    optimized_shuffle_operations INTEGER,
    improvement_percentage DECIMAL(5, 2),
    baseline_partitions INTEGER,
    optimized_partitions INTEGER,
    baseline_join_strategy TEXT,
    optimized_join_strategy TEXT,
    baseline_physical_plan TEXT,
    optimized_physical_plan TEXT,
    notes TEXT,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS DATA_QUALITY_REPORT (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    validation_rule TEXT NOT NULL,
    total_records INTEGER,
    passed_count INTEGER,
    failed_count INTEGER,
    pass_percentage DECIMAL(5, 2),
    sample_failed_records TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, validation_rule)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 5: INDEXES (Performance)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_customers_region ON CUSTOMERS(region);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON CUSTOMERS(customer_segment);
CREATE INDEX IF NOT EXISTS idx_applications_customer_id ON LOAN_APPLICATIONS(customer_id);
CREATE INDEX IF NOT EXISTS idx_applications_status ON LOAN_APPLICATIONS(application_status);
CREATE INDEX IF NOT EXISTS idx_applications_amount ON LOAN_APPLICATIONS(loan_amount);
CREATE INDEX IF NOT EXISTS idx_valid_records_category ON VALID_RECORDS(loan_amount_category);
CREATE INDEX IF NOT EXISTS idx_valid_records_run_id ON VALID_RECORDS(run_id);
CREATE INDEX IF NOT EXISTS idx_rejected_records_reason ON REJECTED_RECORDS(rejection_reason_code);
CREATE INDEX IF NOT EXISTS idx_rejected_records_run_id ON REJECTED_RECORDS(run_id);
CREATE INDEX IF NOT EXISTS idx_curated_output_run_id ON CURATED_OUTPUT(run_id);
CREATE INDEX IF NOT EXISTS idx_execution_logs_run_id ON EXECUTION_LOGS(run_id);
CREATE INDEX IF NOT EXISTS idx_execution_logs_stage ON EXECUTION_LOGS(pipeline_stage);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SECTION 6: VIEWS (Easy Querying)
-- ═══════════════════════════════════════════════════════════════════════════════

-- View: Pipeline Execution Summary
CREATE VIEW IF NOT EXISTS v_execution_summary AS
SELECT 
    run_id,
    COUNT(*) as total_stages,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_stages,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_stages,
    MIN(start_time) as pipeline_start,
    MAX(end_time) as pipeline_end,
    (julianday(MAX(end_time)) - julianday(MIN(start_time))) * 86400000 as total_duration_ms
FROM EXECUTION_LOGS
GROUP BY run_id;

-- View: Data Quality Summary
CREATE VIEW IF NOT EXISTS v_quality_summary AS
SELECT 
    run_id,
    SUM(total_records) as total_records_validated,
    SUM(passed_count) as records_passed,
    SUM(failed_count) as records_failed,
    ROUND(100.0 * SUM(passed_count) / SUM(total_records), 2) as overall_quality_percentage
FROM DATA_QUALITY_REPORT
GROUP BY run_id;

-- View: Baseline vs Optimized Comparison
CREATE VIEW IF NOT EXISTS v_performance_comparison AS
SELECT 
    run_id,
    ROUND(baseline_duration_ms / 1000.0, 2) as baseline_seconds,
    ROUND(optimized_duration_ms / 1000.0, 2) as optimized_seconds,
    ROUND(100.0 * (baseline_duration_ms - optimized_duration_ms) / baseline_duration_ms, 1) as speedup_percentage,
    baseline_shuffle_operations,
    optimized_shuffle_operations,
    (baseline_shuffle_operations - optimized_shuffle_operations) as shuffle_reduction,
    improvement_percentage,
    baseline_join_strategy,
    optimized_join_strategy
FROM PERFORMANCE_METRICS;
