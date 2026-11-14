# Data dictionary

## customers.csv / customers

| Field | Type | Key | Null? | Meaning / validation |
|---|---|---|---|---|
| customer_id | TEXT | PK | No | Unique customer identifier |
| customer_name | TEXT | | No | Customer display name |
| date_of_birth | ISO date | | No | Used to calculate age at application |
| region | TEXT | | No | Customer region |
| customer_segment | TEXT | | No | Customer segment |
| created_at | ISO-8601 timestamp | | No | Customer creation time |
| first_seen_run_id | TEXT | | No | First ingestion run stored for the customer |

## loan_applications.csv / loan_applications

| Field | Type | Key | Null? | Meaning / validation |
|---|---|---|---|---|
| application_id | TEXT | Business key | No | Unique application identifier per source event |
| customer_id | TEXT | FK | No | Must exist in valid customers |
| loan_amount | REAL | | No | Must be > 0 and <= configured maximum INR |
| application_status | TEXT | | No | APPROVED, REJECTED, PENDING |
| application_timestamp | ISO-8601 | | No | Event time used for age |
| source_system | TEXT | | No | Originating application system |
| updated_at | ISO-8601 | | No | Source update time |
| age_at_application | INTEGER | | No | Derived age in completed years |
| loan_amount_category | TEXT | | No | SMALL / MEDIUM / LARGE |
| run_id | TEXT | | No | Pipeline execution identifier |
| processing_mode | TEXT | | No | BASELINE / OPTIMIZED |
| source_file | TEXT | | No | Input file lineage |
| source_row_number | INTEGER | | No | Spark-generated lineage token; see README limitation |

`loan_applications` uses a surrogate `application_record_id` primary key and a unique constraint on `(application_id, run_id, processing_mode)`, preserving the same business application across separate runs and modes.
