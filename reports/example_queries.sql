-- Total customers and applications
SELECT COUNT(*) AS total_customers FROM customers;
SELECT COUNT(*) AS total_loan_applications FROM loan_applications;
-- Applications by status and amount band
SELECT application_status, COUNT(*) AS application_count FROM loan_applications GROUP BY application_status;
SELECT loan_amount_category, COUNT(*) AS application_count FROM loan_applications GROUP BY loan_amount_category;
-- Average loan amount
SELECT ROUND(AVG(loan_amount), 2) AS average_loan_amount FROM loan_applications;
-- Rejections by reason
SELECT reason_code, COUNT(*) AS rejected_count FROM rejected_records GROUP BY reason_code;
-- Runs and modes
SELECT run_id, processing_mode, run_status, input_application_count, valid_application_count, rejected_count, output_count, spark_duration_seconds, publish_duration_seconds FROM pipeline_runs ORDER BY start_time DESC;
-- Baseline vs optimized counts
SELECT processing_mode, COUNT(*) AS output_count FROM loan_applications GROUP BY processing_mode;
-- Duplicate and referential integrity checks (both should return zero)
SELECT COUNT(*) AS duplicate_business_keys FROM (SELECT application_id, run_id, processing_mode FROM loan_applications GROUP BY application_id, run_id, processing_mode HAVING COUNT(*) > 1);
SELECT COUNT(*) AS orphan_applications FROM loan_applications l LEFT JOIN customers c ON l.customer_id=c.customer_id WHERE c.customer_id IS NULL;
