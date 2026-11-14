PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS customers (
  customer_id TEXT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  date_of_birth TEXT NOT NULL,
  region TEXT NOT NULL,
  customer_segment TEXT NOT NULL,
  created_at TEXT NOT NULL,
  first_seen_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS loan_applications (
  application_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
  application_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  loan_amount REAL NOT NULL CHECK (loan_amount > 0),
  application_status TEXT NOT NULL CHECK (application_status IN ('APPROVED','REJECTED','PENDING')),
  age_at_application INTEGER NOT NULL,
  loan_amount_category TEXT NOT NULL CHECK (loan_amount_category IN ('SMALL','MEDIUM','LARGE')),
  application_timestamp TEXT NOT NULL,
  source_system TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  run_id TEXT NOT NULL,
  processing_mode TEXT NOT NULL,
  source_file TEXT NOT NULL,
  source_row_number INTEGER NOT NULL,
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
  UNIQUE(application_id, run_id, processing_mode)
);
CREATE INDEX IF NOT EXISTS idx_loan_customer ON loan_applications(customer_id);
CREATE INDEX IF NOT EXISTS idx_loan_status ON loan_applications(application_status);
CREATE INDEX IF NOT EXISTS idx_loan_run ON loan_applications(run_id, processing_mode);
CREATE TABLE IF NOT EXISTS rejected_records (
  reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  business_key TEXT,
  original_payload TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_message TEXT NOT NULL,
  run_id TEXT NOT NULL,
  processing_mode TEXT NOT NULL,
  source_file TEXT NOT NULL,
  source_row_number INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY,
  processing_mode TEXT NOT NULL,
  start_time TEXT NOT NULL,
  end_time TEXT,
  run_status TEXT NOT NULL,
  input_customer_count INTEGER DEFAULT 0,
  input_application_count INTEGER DEFAULT 0,
  valid_customer_count INTEGER DEFAULT 0,
  valid_application_count INTEGER DEFAULT 0,
  rejected_count INTEGER DEFAULT 0,
  output_count INTEGER DEFAULT 0,
  spark_duration_seconds REAL,
  publish_duration_seconds REAL,
  error_message TEXT
);
CREATE TABLE IF NOT EXISTS processed_batches (
  batch_hash TEXT NOT NULL,
  processing_mode TEXT NOT NULL,
  transformation_version TEXT NOT NULL,
  source_file TEXT NOT NULL,
  first_successful_run_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY(batch_hash, processing_mode, transformation_version)
);
CREATE TABLE IF NOT EXISTS pipeline_errors (
  error_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  error_time TEXT NOT NULL,
  error_type TEXT NOT NULL,
  error_message TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id)
);
