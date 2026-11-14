import sqlite3
from src.lending_pipeline.storage import connect, batch_already_processed, record_batch

def test_schema_and_idempotency_key(tmp_path):
    db=tmp_path/"test.db"; conn=connect(db)
    assert conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='loan_applications'").fetchone()
    assert not batch_already_processed(conn,"hash1","BASELINE","1.0")
    record_batch(conn,"hash1","BASELINE","1.0","input.csv","RUN_1"); conn.commit()
    assert batch_already_processed(conn,"hash1","BASELINE","1.0")
    assert not batch_already_processed(conn,"hash1","OPTIMIZED","1.0")
    conn.close()

def test_sqlite_foreign_key_enforced(tmp_path):
    conn=connect(tmp_path/"fk.db")
    try:
        conn.execute("INSERT INTO loan_applications(application_id,customer_id,loan_amount,application_status,age_at_application,loan_amount_category,application_timestamp,source_system,updated_at,run_id,processing_mode,source_file,source_row_number) VALUES ('A1','NO_CUSTOMER',1000,'PENDING',30,'SMALL','2025-01-01T00:00:00Z','SYS','2025-01-01T00:00:00Z','R1','BASELINE','x.csv',1)")
        conn.commit()
        assert False, "Foreign key should reject unknown customer"
    except sqlite3.IntegrityError:
        conn.rollback()
    finally: conn.close()
