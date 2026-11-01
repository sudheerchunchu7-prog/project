/* Optional SQL Server/Azure SQL companion schema for SSMS inspection.
   The tested Python execution path currently uses SQLite; this is a matching
   SQL Server design, not an active Python-to-SQL Server connector.
   Run in a dedicated empty database, e.g. YottaFlexDataQuality. */

CREATE TABLE dbo.customers (
    customer_id       NVARCHAR(50) NOT NULL PRIMARY KEY,
    customer_name     NVARCHAR(150) NOT NULL
);
GO

CREATE TABLE dbo.loan_applications (
    application_id    NVARCHAR(50) NOT NULL PRIMARY KEY,
    customer_id       NVARCHAR(50) NOT NULL,
    age               INT NULL,
    loan_amount       DECIMAL(18,2) NULL,
    status            NVARCHAR(20) NOT NULL,
    event_timestamp   DATETIMEOFFSET NOT NULL,
    source_file       NVARCHAR(260) NOT NULL,
    run_id            NVARCHAR(32) NOT NULL,
    CONSTRAINT fk_loan_applications_customers
      FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id),
    CONSTRAINT ck_loan_applications_age CHECK (age IS NULL OR age BETWEEN 18 AND 100),
    CONSTRAINT ck_loan_applications_loan_amount CHECK (loan_amount IS NULL OR loan_amount > 0),
    CONSTRAINT ck_loan_applications_status CHECK (status IN ('approved','rejected','pending'))
);
GO

CREATE TABLE dbo.rejected_records (
    reject_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id             NVARCHAR(32) NOT NULL,
    source_file        NVARCHAR(260) NOT NULL,
    source_row_number  INT NOT NULL,
    application_id     NVARCHAR(50) NULL,
    raw_record         NVARCHAR(MAX) NOT NULL,
    reject_reason      NVARCHAR(1000) NOT NULL,
    rejected_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.batch_runs (
    run_id             NVARCHAR(32) NOT NULL PRIMARY KEY,
    source_file        NVARCHAR(260) NOT NULL,
    batch_hash         CHAR(64) NOT NULL,
    started_at         DATETIME2 NOT NULL,
    ended_at           DATETIME2 NULL,
    status             NVARCHAR(30) NOT NULL,
    input_count        INT NOT NULL DEFAULT 0,
    valid_count        INT NOT NULL DEFAULT 0,
    rejected_count     INT NOT NULL DEFAULT 0,
    output_count       INT NOT NULL DEFAULT 0,
    failure_reason     NVARCHAR(2000) NULL
);
GO

CREATE TABLE dbo.processed_batches (
    batch_hash         CHAR(64) NOT NULL PRIMARY KEY,
    source_file        NVARCHAR(260) NOT NULL,
    first_run_id       NVARCHAR(32) NOT NULL,
    processed_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dbo.baseline_profiles (
    column_name        SYSNAME NOT NULL PRIMARY KEY,
    data_type          NVARCHAR(30) NOT NULL,
    profile_json       NVARCHAR(MAX) NOT NULL,
    established_from_run_id NVARCHAR(32) NOT NULL,
    established_at     DATETIME2 NOT NULL
);
GO

CREATE TABLE dbo.current_profiles (
    run_id             NVARCHAR(32) NOT NULL,
    column_name        SYSNAME NOT NULL,
    data_type          NVARCHAR(30) NOT NULL,
    profile_json       NVARCHAR(MAX) NOT NULL,
    CONSTRAINT pk_current_profiles PRIMARY KEY (run_id, column_name)
);
GO

CREATE TABLE dbo.quality_alerts (
    alert_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id             NVARCHAR(32) NOT NULL,
    alert_type         NVARCHAR(80) NOT NULL,
    column_name        SYSNAME NULL,
    severity           NVARCHAR(20) NOT NULL,
    baseline_value     NVARCHAR(MAX) NULL,
    current_value      NVARCHAR(MAX) NULL,
    explanation        NVARCHAR(2000) NOT NULL,
    created_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX ix_loan_applications_event_timestamp ON dbo.loan_applications(event_timestamp);
CREATE INDEX ix_loan_applications_customer_id ON dbo.loan_applications(customer_id);
CREATE INDEX ix_batch_runs_started_at ON dbo.batch_runs(started_at);
CREATE INDEX ix_quality_alerts_run_id ON dbo.quality_alerts(run_id);
GO
