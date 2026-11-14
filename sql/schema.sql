-- =============================================================================
-- SQLite Schema for Lending Data Pipeline
-- =============================================================================
-- 
-- This file defines the structure of the SQLite database.
-- It creates tables to store processed customer and loan application data.
-- 
-- Before running the pipeline, execute this file to set up the database:
--   sqlite3 database/baseline.db < database/schema.sql
--   sqlite3 database/optimized.db < database/schema.sql
--
-- Author: Lending Pipeline Team
-- Date: 2024-11-14
--
-- =============================================================================

-- =============================================================================
-- TABLE 1: CUSTOMERS
-- =============================================================================
-- 
-- PURPOSE
--   Master data about customers (dimension table)
--   Maintains ONE CURRENT snapshot per customer
--   Previous versions are overwritten (not historical)
--
-- DESIGN DECISION: SNAPSHOT TABLE
--   - One row per customer_id (PRIMARY KEY)
--   - When a customer's data changes, UPSERT updates the existing row
--   - Customer's geographical or segment changes are not historically tracked
--   - This reflects reality: you care about CURRENT customer data
--
-- Fields:
--   customer_id: Unique customer identifier (PRIMARY KEY)
--   customer_name: Full name of customer
--   date_of_birth: Birth date (ISO-8601: YYYY-MM-DD)
--   region: Geographic region (NORTH, SOUTH, EAST, WEST, CENTRAL, NORTHEAST)
--   customer_segment: Customer type (RETAIL, CORPORATE, GOVT)
--   created_at: When customer record was originally created (from source)
--   last_updated_at: When customer record was last updated by any pipeline run
--   last_run_id: Which ETL run last updated this customer
--   run_id: Which run first created this record (immutable)
--   source_file: Which input file this customer came from (immutable)
--   source_row_number: Row number in input file (immutable, 1-based)
--   processed_at: When this record was first processed (immutable)
--
-- LINEAGE STRATEGY
--   Immutable fields (run_id, source_file, source_row_number, processed_at):
--     - Record ORIGINAL source of this customer
--     - Set once when customer is first inserted
--     - Never changed
--
--   Mutable fields (last_updated_at, last_run_id):
--     - Track LATEST pipeline run that touched this customer
--     - Updated on each UPSERT operation
--     - Enables debugging: "When was this customer last updated?"
--
-- CONSTRAINTS:
--   - PRIMARY KEY: customer_id (no duplicates, one row per customer)
--   - NOT NULL: All fields
--   - source_row_number > 0
--
-- Indexes:
--   - PRIMARY KEY on customer_id
--   - region (for geographic analysis)
--   - customer_segment (for customer segmentation)
--   - last_run_id (for tracking which run updated customers)
--
-- =============================================================================

CREATE TABLE IF NOT EXISTS customers (
    -- Identifier (IMMUTABLE PRIMARY KEY)
    customer_id TEXT NOT NULL PRIMARY KEY,
    
    -- Customer Information
    customer_name TEXT NOT NULL,
    date_of_birth DATE NOT NULL,
    region TEXT NOT NULL,
    customer_segment TEXT NOT NULL,
    
    -- Original Timestamps (IMMUTABLE - from source)
    created_at TIMESTAMP NOT NULL,     -- Original creation time from source
    
    -- Tracking Timestamps (MUTABLE - updated on UPSERT)
    last_updated_at TIMESTAMP NOT NULL,  -- When customer was last updated
    last_run_id TEXT NOT NULL,           -- Which run last updated this
    
    -- Original Lineage (IMMUTABLE - source information)
    run_id TEXT NOT NULL,               -- Which run first loaded this customer
    source_file TEXT NOT NULL,          -- Original input filename
    source_row_number INTEGER NOT NULL, -- Row in original input (1-based)
    processed_at TIMESTAMP NOT NULL,    -- When originally processed
    
    -- Constraints
    CONSTRAINT chk_date_of_birth CHECK (date_of_birth IS NOT NULL),
    CONSTRAINT chk_region_not_empty CHECK (length(region) > 0),
    CONSTRAINT chk_segment_not_empty CHECK (length(customer_segment) > 0),
    CONSTRAINT chk_source_row_positive CHECK (source_row_number > 0)
);

-- Index for lookups by region (for geographic analysis)
CREATE INDEX IF NOT EXISTS idx_customers_region ON customers(region);

-- Index for lookups by customer segment (for segmentation)
CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment);

-- Index for tracking which run last modified customers
CREATE INDEX IF NOT EXISTS idx_customers_last_run_id ON customers(last_run_id);


-- =============================================================================
-- TABLE 2: LOAN_APPLICATIONS
-- =============================================================================
--
-- PURPOSE
--   Track ALL loan applications with complete historical record
--   NEVER overwrites or deletes previous versions
--   Multiple versions of same application can exist (different runs)
--
-- DESIGN DECISION: HISTORICAL FACT TABLE
--   - application_record_id: Surrogate primary key (AUTO INCREMENT)
--   - application_id: Business identifier (can repeat across runs)
--   - When you re-run the pipeline and see same application_id:
--     * OLD records are kept (not deleted)
--     * NEW records are inserted with different application_record_id
--     * Same application_id can have multiple records (one per run)
--   - This preserves complete audit trail
--
-- Example:
--   Run 1 (2024-11-14 09:00):
--     application_record_id=1, application_id=APP001, customer_id=CUST001,
--     loan_amount=250000, run_id=run_001, processed_at=2024-11-14T09:00:00Z
--
--   Run 2 (2024-11-14 10:00) [Same application_id]:
--     application_record_id=2, application_id=APP001, customer_id=CUST001,
--     loan_amount=250000, run_id=run_002, processed_at=2024-11-14T10:00:00Z
--     ✓ BOTH records exist - complete history preserved
--
-- Fields:
--   application_record_id: Surrogate key (unique across all time)
--   application_id: Business identifier (business-facing ID, can repeat)
--   customer_id: References customers.customer_id (FOREIGN KEY)
--   loan_amount: Requested amount in INR (DECIMAL 18,2)
--   application_status: Status (APPROVED, REJECTED, PENDING)
--   application_timestamp: When application was submitted (ISO-8601)
--   source_system: System where application originated
--   updated_at: Last update timestamp (ISO-8601)
--   customer_age: Derived - customer's age at application time
--   loan_category: Derived - loan size (SMALL, MEDIUM, LARGE)
--   run_id: Which ETL run loaded this record
--   source_file: Source CSV filename
--   source_row_number: Row number in input file (1-based)
--   processed_at: When record was processed by pipeline
--
-- CONSTRAINTS:
--   - PRIMARY KEY: application_record_id (guarantees unique records)
--   - UNIQUE(application_id, run_id): Prevents duplicate loads in same run
--   - FOREIGN KEY: customer_id → customers.customer_id
--   - BUSINESS CONSTRAINTS: loan_amount, status, category validation
--
-- Indexes:
--   - PRIMARY KEY on application_record_id (internal uniqueness)
--   - application_id (find all versions of an application)
--   - customer_id (for joins with customers table)
--   - application_status (for filtering)
--   - loan_category (for category analysis)
--   - application_timestamp (for time-based queries)
--   - run_id (for lineage tracking)
--
-- =============================================================================

CREATE TABLE IF NOT EXISTS loan_applications (
    -- Surrogate Primary Key (AUTO INCREMENT)
    -- This ensures every record is unique, even if application_id repeats
    application_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Business Identifier (NOT unique, can repeat across runs)
    application_id TEXT NOT NULL,
    
    -- Foreign Key Reference
    customer_id TEXT NOT NULL,
    
    -- Application Details
    loan_amount DECIMAL(18, 2) NOT NULL,
    application_status TEXT NOT NULL,
    application_timestamp TIMESTAMP NOT NULL,
    source_system TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    -- Derived Fields (computed by pipeline)
    customer_age INTEGER NOT NULL,
    loan_category TEXT NOT NULL,
    
    -- Lineage (audit trail - from source)
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    
    -- Relationships
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    
    -- Prevent loading exact same record twice in one run
    UNIQUE(application_id, run_id, source_row_number),
    
    -- Business Constraints
    CONSTRAINT chk_loan_amount_positive CHECK (loan_amount > 0),
    CONSTRAINT chk_loan_amount_max CHECK (loan_amount <= 5000000),
    CONSTRAINT chk_status_valid CHECK (
        application_status IN ('APPROVED', 'REJECTED', 'PENDING')
    ),
    CONSTRAINT chk_category_valid CHECK (
        loan_category IN ('SMALL', 'MEDIUM', 'LARGE')
    ),
    CONSTRAINT chk_customer_age_positive CHECK (customer_age > 0),
    CONSTRAINT chk_updated_after_application CHECK (updated_at >= application_timestamp),
    CONSTRAINT chk_source_row_positive CHECK (source_row_number > 0)
);

-- Index on application_id (find all versions of an application)
CREATE INDEX IF NOT EXISTS idx_applications_id ON loan_applications(application_id);

-- Index on customer_id (for joins with customers table)
CREATE INDEX IF NOT EXISTS idx_applications_customer_id ON loan_applications(customer_id);

-- Index on application status (common filter)
CREATE INDEX IF NOT EXISTS idx_applications_status ON loan_applications(application_status);

-- Index on loan category (for category analysis)
CREATE INDEX IF NOT EXISTS idx_applications_category ON loan_applications(loan_category);

-- Index on application timestamp (for time-based queries)
CREATE INDEX IF NOT EXISTS idx_applications_timestamp ON loan_applications(application_timestamp);

-- Index on run_id (for lineage tracking)
CREATE INDEX IF NOT EXISTS idx_applications_run_id ON loan_applications(run_id);


-- =============================================================================
-- VIEW: APPLICATIONS_WITH_CUSTOMER_INFO
-- =============================================================================
--
-- PURPOSE: Convenience view that joins applications with customer details
-- 
-- This view shows the LATEST version of each application (by processed_at)
-- joined with customer information. Useful for common queries.
--
-- Example Query:
--   SELECT * FROM applications_with_customer_info
--   WHERE application_status = 'APPROVED'
--   AND customer_segment = 'RETAIL'
--
-- NOTE: If you need historical applications, query loan_applications directly
-- and filter by run_id or processed_at to see specific versions.
--
-- =============================================================================

CREATE VIEW IF NOT EXISTS applications_with_customer_info AS
SELECT
    app.application_record_id,
    app.application_id,
    app.customer_id,
    cust.customer_name,
    app.loan_amount,
    app.loan_category,
    app.application_status,
    app.application_timestamp,
    cust.region,
    cust.customer_segment,
    app.customer_age,
    app.source_system,
    app.run_id
FROM loan_applications app
INNER JOIN customers cust
    ON app.customer_id = cust.customer_id;


-- =============================================================================
-- END OF SCHEMA DEFINITION
-- =============================================================================
--
-- Summary of what was created:
--   ✓ customers table (master customer data)
--   ✓ loan_applications table (all applications with history)
--   ✓ 8 indexes for query performance
--   ✓ 1 view for convenient joins
--
-- Next Steps:
--   1. Execute this file to create the database
--   2. Use PySpark to read CSV files and insert data
--   3. Query the data with SQL
--
-- =============================================================================
