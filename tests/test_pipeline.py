import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

from quality_monitor.pipeline import process_batch, connect_db, reconciliation

CONFIG = {
    "volume_change_threshold": 0.30,
    "null_rate_increase_threshold": 0.10,
    "distribution_change_threshold": 0.25,
    "max_freshness_lag_minutes": 120,
    "allowed_statuses": ["approved", "rejected", "pending"],
    "age_min": 18,
    "age_max": 100
}

def write_customers(path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["customer_id", "customer_name"])
        w.writerows([["C1", "One"], ["C2", "Two"], ["C3", "Three"]])

def write_batch(path, rows, extra_header=False):
    headers = ["application_id", "customer_id", "age", "loan_amount", "status", "event_timestamp"]
    if extra_header: headers.append("unexpected_field")
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(headers)
        for row in rows:
            w.writerow(row + (["extra"] if extra_header else []))

def good_rows(prefix, n=4, old=False):
    ts = datetime.now(timezone.utc) - (timedelta(hours=5) if old else timedelta(minutes=10))
    return [[f"{prefix}{i}", f"C{(i % 3) + 1}", 25 + i, 30000 + i * 1000,
             "approved" if i % 2 == 0 else "rejected", ts.isoformat()] for i in range(n)]

def setup(tmp_path):
    customers = tmp_path / "customers.csv"; write_customers(customers)
    db = tmp_path / "test.db"; reports = tmp_path / "reports"
    return customers, db, reports

def test_happy_path_and_baseline(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "batch1.csv"; write_batch(batch, good_rows("A"))
    result = process_batch(batch, customers, db, reports, CONFIG)
    assert result["status"] == "SUCCESS"
    assert result["baseline_established"] is True
    assert reconciliation(db, result["run_id"])["counts_reconcile"] is True

def test_nulls_are_profiled_and_alerted(tmp_path):
    customers, db, reports = setup(tmp_path)
    b1 = tmp_path / "batch1.csv"; write_batch(b1, good_rows("A", 20))
    process_batch(b1, customers, db, reports, CONFIG)
    rows = good_rows("B", 20)
    for i in range(12): rows[i][2] = ""
    b2 = tmp_path / "batch2.csv"; write_batch(b2, rows)
    result = process_batch(b2, customers, db, reports, CONFIG)
    assert any(a["alert_type"] == "null_rate_change" and a["column_name"] == "age" for a in result["alerts"])

def test_duplicate_and_invalid_reference_are_quarantined(tmp_path):
    customers, db, reports = setup(tmp_path)
    rows = good_rows("A", 2)
    rows.append(rows[0].copy())
    rows.append(["BAD", "UNKNOWN", 25, 50000, "approved", datetime.now(timezone.utc).isoformat()])
    batch = tmp_path / "batch.csv"; write_batch(batch, rows)
    result = process_batch(batch, customers, db, reports, CONFIG)
    assert result["valid_count"] == 2
    assert result["rejected_count"] == 2

def test_empty_batch_is_recorded(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "empty.csv"; write_batch(batch, [])
    result = process_batch(batch, customers, db, reports, CONFIG)
    assert result["input_count"] == 0
    assert result["status"] == "SUCCESS"

def test_malformed_schema_fails_and_is_logged(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "bad.csv"
    batch.write_text("application_id,customer_id\nA,C1\n")
    try:
        process_batch(batch, customers, db, reports, CONFIG)
        assert False, "Expected schema error"
    except ValueError as e:
        assert "Schema error" in str(e)
    conn = connect_db(db)
    assert conn.execute("SELECT status FROM batch_runs").fetchone()[0] == "FAILED"
    conn.close()

def test_schema_drift_is_alerted(tmp_path):
    customers, db, reports = setup(tmp_path)
    b1 = tmp_path / "b1.csv"; write_batch(b1, good_rows("A"))
    process_batch(b1, customers, db, reports, CONFIG)
    b2 = tmp_path / "b2.csv"; write_batch(b2, good_rows("B"), extra_header=True)
    result = process_batch(b2, customers, db, reports, CONFIG)
    assert any(a["alert_type"] == "schema_change" for a in result["alerts"])

def test_late_data_triggers_freshness_alert(tmp_path):
    customers, db, reports = setup(tmp_path)
    b1 = tmp_path / "b1.csv"; write_batch(b1, good_rows("A"))
    process_batch(b1, customers, db, reports, CONFIG)
    b2 = tmp_path / "b2.csv"; write_batch(b2, good_rows("B", old=True))
    result = process_batch(b2, customers, db, reports, CONFIG)
    assert any(a["alert_type"] == "freshness" for a in result["alerts"])

def test_same_input_rerun_is_idempotent(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "batch.csv"; write_batch(batch, good_rows("A"))
    first = process_batch(batch, customers, db, reports, CONFIG)
    second = process_batch(batch, customers, db, reports, CONFIG)
    assert second["status"] == "SKIPPED_ALREADY_PROCESSED"
    conn = connect_db(db)
    count = conn.execute("SELECT COUNT(*) FROM loan_applications").fetchone()[0]
    conn.close()
    assert count == first["output_count"]

def test_negative_loan_amount_is_rejected(tmp_path):
    customers, db, reports = setup(tmp_path)
    rows = good_rows("A", 2); rows[0][3] = -500
    batch = tmp_path / "batch.csv"; write_batch(batch, rows)
    result = process_batch(batch, customers, db, reports, CONFIG)
    assert result["rejected_count"] == 1


def test_categorical_distribution_shift_is_alerted(tmp_path):
    customers, db, reports = setup(tmp_path)
    baseline_rows = good_rows("A", 40)
    # Make the baseline status mix balanced.
    for i, row in enumerate(baseline_rows):
        row[4] = "approved" if i < 20 else "rejected"
    b1 = tmp_path / "b1.csv"; write_batch(b1, baseline_rows)
    process_batch(b1, customers, db, reports, CONFIG)

    current_rows = good_rows("B", 40)
    # A strong shift: all records now have the same status.
    for row in current_rows:
        row[4] = "approved"
    b2 = tmp_path / "b2.csv"; write_batch(b2, current_rows)
    result = process_batch(b2, customers, db, reports, CONFIG)
    assert any(a["alert_type"] == "distribution_change" and a["column_name"] == "status"
               for a in result["alerts"])


def test_failed_run_can_be_retried_after_reference_file_is_fixed(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "retry_me.csv"
    write_batch(batch, good_rows("RETRY", 3))
    missing_reference = tmp_path / "not_created_yet.csv"
    try:
        process_batch(batch, missing_reference, db, reports, CONFIG)
        assert False, "Expected reference-file failure"
    except FileNotFoundError:
        pass
    conn = connect_db(db)
    failed = conn.execute("SELECT status, failure_reason FROM batch_runs").fetchone()
    assert failed["status"] == "FAILED"
    assert failed["failure_reason"]
    conn.close()
    # Repair the missing dependency and retry the exact same source file.
    write_customers(missing_reference)
    retried = process_batch(batch, missing_reference, db, reports, CONFIG)
    assert retried["status"] == "SUCCESS"
    assert retried["valid_count"] == 3
    assert reconciliation(db, retried["run_id"])["counts_reconcile"] is True


def test_missing_required_record_fields_are_quarantined(tmp_path):
    customers, db, reports = setup(tmp_path)
    rows = good_rows("MISS", 2)
    rows[0][0] = ""  # missing application_id
    rows[1][5] = "not-a-timestamp"
    batch = tmp_path / "missing_fields.csv"
    write_batch(batch, rows)
    result = process_batch(batch, customers, db, reports, CONFIG)
    assert result["valid_count"] == 0
    assert result["rejected_count"] == 2


def test_stage_level_observability_is_in_report(tmp_path):
    customers, db, reports = setup(tmp_path)
    batch = tmp_path / "observable.csv"
    write_batch(batch, good_rows("OBS", 3))
    result = process_batch(batch, customers, db, reports, CONFIG)
    stages = [item["stage"] for item in result["stage_results"]]
    assert "01_INGESTION" in stages
    assert "04_RECORD_VALIDATION" in stages
    assert "07_BASELINE_AND_MONITORING" in stages
    assert "08_RECONCILIATION" in stages
    assert result["duration_seconds"] >= 0


def test_edge_case_generator_keeps_expected_labels_outside_csv(tmp_path):
    import json
    from quality_monitor.generator import generate
    customers = tmp_path / "customers.csv"
    batch = tmp_path / "edge.csv"
    generate(str(customers), str(batch), rows=10, seed=99, edge_cases=True)
    manifest = json.loads((tmp_path / "edge.csv.manifest.json").read_text())
    assert manifest["injected_cases"]
    header = batch.read_text().splitlines()[0]
    assert "expected" not in header
    assert "case" not in header
