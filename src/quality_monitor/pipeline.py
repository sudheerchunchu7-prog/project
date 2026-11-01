"""Local data-quality monitoring pipeline: CSV -> validation -> SQLite -> profiles/alerts."""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
import math
import sqlite3
import statistics
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

APP_COLUMNS = [
    "application_id", "customer_id", "age", "loan_amount",
    "status", "event_timestamp"
]
REQUIRED_COLUMNS = ["application_id", "customer_id", "status", "event_timestamp"]
NUMERIC_COLUMNS = {"age", "loan_amount"}
CATEGORICAL_COLUMNS = {"status"}
DATE_FORMAT_NOTE = "ISO-8601 timestamp with timezone, e.g. 2026-10-30T08:30:00+00:00"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; accept trailing Z."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        customer_name TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS loan_applications (
        application_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        age INTEGER,
        loan_amount REAL,
        status TEXT NOT NULL,
        event_timestamp TEXT NOT NULL,
        source_file TEXT NOT NULL,
        run_id TEXT NOT NULL,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
    );
    CREATE TABLE IF NOT EXISTS batch_runs (
        run_id TEXT PRIMARY KEY,
        source_file TEXT NOT NULL,
        batch_hash TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        status TEXT NOT NULL,
        input_count INTEGER DEFAULT 0,
        valid_count INTEGER DEFAULT 0,
        rejected_count INTEGER DEFAULT 0,
        output_count INTEGER DEFAULT 0,
        failure_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS processed_batches (
        batch_hash TEXT PRIMARY KEY,
        source_file TEXT NOT NULL,
        first_run_id TEXT NOT NULL,
        processed_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS rejected_records (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_row_number INTEGER NOT NULL,
        application_id TEXT,
        raw_record TEXT NOT NULL,
        reject_reason TEXT NOT NULL,
        rejected_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS baseline_profiles (
        column_name TEXT PRIMARY KEY,
        data_type TEXT NOT NULL,
        profile_json TEXT NOT NULL,
        established_from_run_id TEXT NOT NULL,
        established_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS current_profiles (
        run_id TEXT NOT NULL,
        column_name TEXT NOT NULL,
        data_type TEXT NOT NULL,
        profile_json TEXT NOT NULL,
        PRIMARY KEY(run_id, column_name)
    );
    CREATE TABLE IF NOT EXISTS quality_alerts (
        alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        column_name TEXT,
        severity TEXT NOT NULL,
        baseline_value TEXT,
        current_value TEXT,
        explanation TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """)
    conn.commit()
    return conn


def load_customers(path: str | Path) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or not {"customer_id", "customer_name"}.issubset(reader.fieldnames):
            raise ValueError("Customer reference file must contain customer_id and customer_name headers")
        customers = {}
        for row in reader:
            cid = (row.get("customer_id") or "").strip()
            name = (row.get("customer_name") or "").strip()
            if cid and name:
                customers[cid] = name
    return set(customers)


def ensure_customers(conn: sqlite3.Connection, path: str | Path) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or not {"customer_id", "customer_name"}.issubset(reader.fieldnames):
            raise ValueError("Customer reference file must contain customer_id and customer_name headers")
        rows = []
        for row in reader:
            cid = (row.get("customer_id") or "").strip()
            name = (row.get("customer_name") or "").strip()
            if cid and name:
                rows.append((cid, name))
    conn.executemany(
        "INSERT INTO customers(customer_id, customer_name) VALUES (?, ?) "
        "ON CONFLICT(customer_id) DO UPDATE SET customer_name=excluded.customer_name",
        rows
    )
    conn.commit()
    return {r[0] for r in rows}


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_batch(path: str | Path) -> tuple[list[dict[str, str]], list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if not headers:
            raise ValueError("Input file is empty or has no header row")
        missing = sorted(set(APP_COLUMNS) - set(headers))
        unexpected = sorted(set(headers) - set(APP_COLUMNS))
        if missing:
            raise ValueError(f"Schema error: missing columns {missing}")
        rows = [dict(row) for row in reader]
    return rows, unexpected


def validate_record(
    row: dict[str, str],
    customer_ids: set[str],
    allowed_statuses: set[str],
    age_min: int,
    age_max: int,
) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    application_id = (row.get("application_id") or "").strip()
    customer_id = (row.get("customer_id") or "").strip()
    status = (row.get("status") or "").strip().lower()
    if not application_id:
        reasons.append("missing application_id")
    if not customer_id:
        reasons.append("missing customer_id")
    elif customer_id not in customer_ids:
        reasons.append(f"unknown customer_id: {customer_id}")
    if not status:
        reasons.append("missing status")
    elif status not in allowed_statuses:
        reasons.append(f"invalid status: {status}")

    age_raw = (row.get("age") or "").strip()
    age = None
    if age_raw:
        try:
            age = int(age_raw)
            if not age_min <= age <= age_max:
                reasons.append(f"age outside permitted range [{age_min}, {age_max}]")
        except ValueError:
            reasons.append("age is not an integer")

    amount_raw = (row.get("loan_amount") or "").strip()
    amount = None
    if amount_raw:
        try:
            amount = float(amount_raw)
            if not math.isfinite(amount) or amount <= 0:
                reasons.append("loan_amount must be a positive finite number")
        except ValueError:
            reasons.append("loan_amount is not numeric")

    timestamp_raw = (row.get("event_timestamp") or "").strip()
    timestamp = None
    if not timestamp_raw:
        reasons.append("missing event_timestamp")
    else:
        try:
            timestamp = iso(parse_timestamp(timestamp_raw))
        except (ValueError, TypeError):
            reasons.append("event_timestamp is not a valid ISO-8601 timestamp")

    if reasons:
        return None, reasons
    return {
        "application_id": application_id,
        "customer_id": customer_id,
        "age": age,
        "loan_amount": amount,
        "status": status,
        "event_timestamp": timestamp,
    }, []


def _bucket(value: float, boundaries: list[float]) -> str:
    for boundary in boundaries:
        if value <= boundary:
            return f"<= {boundary:g}"
    return f"> {boundaries[-1]:g}"


def profile_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Calculate reproducible per-column profiles from accepted rows."""
    profiles: dict[str, dict[str, Any]] = {}
    n = len(records)
    for col in ("age", "loan_amount", "status"):
        values = [r[col] for r in records]
        non_null = [v for v in values if v is not None and v != ""]
        null_count = n - len(non_null)
        profile: dict[str, Any] = {
            "row_count": n,
            "non_null_count": len(non_null),
            "null_count": null_count,
            "null_rate": (null_count / n) if n else 0.0,
            "distinct_count": len(set(non_null)),
        }
        if col in NUMERIC_COLUMNS:
            nums = [float(v) for v in non_null]
            profile.update({
                "min": min(nums) if nums else None,
                "max": max(nums) if nums else None,
                "mean": statistics.fmean(nums) if nums else None,
                "stddev": statistics.pstdev(nums) if len(nums) > 1 else 0.0 if nums else None,
            })
            if col == "age":
                boundaries = [25, 35, 50, 65]
            else:
                boundaries = [25000, 50000, 75000, 100000]
            counts = Counter(_bucket(v, boundaries) for v in nums)
            profile["distribution"] = {k: counts[k] / len(nums) for k in sorted(counts)} if nums else {}
        else:
            counts = Counter(str(v) for v in non_null)
            profile["distribution"] = {k: counts[k] / len(non_null) for k in sorted(counts)} if non_null else {}
        profiles[col] = profile
    return profiles


def compare_profiles(
    baseline: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    baseline_volume: int,
    current_volume: int,
    config: dict[str, Any],
    run_id: str,
    now: datetime,
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    volume_threshold = float(config["volume_change_threshold"])
    if baseline_volume > 0:
        change = (current_volume - baseline_volume) / baseline_volume
        if abs(change) > volume_threshold:
            direction = "increased" if change > 0 else "decreased"
            alerts.append({
                "alert_type": "volume_change", "column_name": "",
                "severity": "warning",
                "baseline_value": str(baseline_volume), "current_value": str(current_volume),
                "explanation": f"Batch volume {direction} by {abs(change):.1%}: baseline {baseline_volume} rows, current {current_volume} rows."
            })

    for col, cur in current.items():
        base = baseline.get(col)
        if not base:
            continue
        delta = cur["null_rate"] - base["null_rate"]
        if delta > float(config["null_rate_increase_threshold"]):
            alerts.append({
                "alert_type": "null_rate_change", "column_name": col, "severity": "high",
                "baseline_value": f'{base["null_rate"]:.1%}', "current_value": f'{cur["null_rate"]:.1%}',
                "explanation": f"{col} null rate increased from {base['null_rate']:.1%} to {cur['null_rate']:.1%} (+{delta:.1%}); {cur['null_count']} of {cur['row_count']} rows are null."
            })

        base_dist = base.get("distribution", {})
        cur_dist = cur.get("distribution", {})
        if base_dist and cur_dist:
            keys = set(base_dist) | set(cur_dist)
            max_delta = max(abs(cur_dist.get(k, 0.0) - base_dist.get(k, 0.0)) for k in keys)
            if max_delta > float(config["distribution_change_threshold"]):
                changed = sorted(
                    ((k, base_dist.get(k, 0.0), cur_dist.get(k, 0.0)) for k in keys),
                    key=lambda x: abs(x[2] - x[1]), reverse=True
                )[:3]
                details = "; ".join(f"{k}: {b:.1%} -> {c:.1%}" for k, b, c in changed)
                alerts.append({
                    "alert_type": "distribution_change", "column_name": col, "severity": "warning",
                    "baseline_value": json.dumps(base_dist, sort_keys=True),
                    "current_value": json.dumps(cur_dist, sort_keys=True),
                    "explanation": f"{col} distribution changed; largest bucket changes: {details}."
                })
    return alerts


def _insert_alerts(conn: sqlite3.Connection, run_id: str, alerts: list[dict[str, str]], now: datetime) -> None:
    conn.executemany("""
        INSERT INTO quality_alerts(run_id, alert_type, column_name, severity,
            baseline_value, current_value, explanation, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (run_id, a["alert_type"], a.get("column_name") or None, a["severity"],
         a.get("baseline_value"), a.get("current_value"), a["explanation"], iso(now))
        for a in alerts
    ])


def process_batch(
    input_path: str | Path,
    customers_path: str | Path,
    db_path: str | Path,
    report_dir: str | Path,
    config: dict[str, Any] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Process one CSV batch. Same file content is safely skipped on rerun."""
    config = config or json.loads(Path("config.json").read_text(encoding="utf-8"))
    now = as_of or utc_now()
    now = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    run_id = "RUN_" + uuid.uuid4().hex[:12].upper()
    source = Path(input_path)
    source_name = source.name
    batch_hash = file_sha256(source)
    report_path = Path(report_dir)
    report_path.mkdir(parents=True, exist_ok=True)
    conn = connect_db(db_path)
    started = iso(now)
    logger = logging.getLogger("quality_monitor")
    started_clock = time.perf_counter()
    stage_results: list[dict[str, Any]] = []
    def stage(name: str, detail: str, status: str = "PASS") -> None:
        item = {"stage": name, "status": status, "detail": detail}
        stage_results.append(item)
        logger.info("[%s] %s - %s: %s", run_id, name, status, detail)
    logger.info("=" * 72)
    logger.info("DATA QUALITY PIPELINE | run_id=%s | source=%s", run_id, source_name)
    logger.info("=" * 72)
    conn.execute("""
        INSERT INTO batch_runs(run_id, source_file, batch_hash, started_at, status)
        VALUES (?, ?, ?, ?, 'RUNNING')
    """, (run_id, source_name, batch_hash, started))
    conn.commit()

    try:
        already = conn.execute(
            "SELECT first_run_id FROM processed_batches WHERE batch_hash=?", (batch_hash,)
        ).fetchone()
        if already:
            conn.execute("""
                UPDATE batch_runs SET ended_at=?, status='SKIPPED_ALREADY_PROCESSED',
                    failure_reason=? WHERE run_id=?
            """, (iso(now), f"Identical content already processed by {already['first_run_id']}", run_id))
            conn.commit()
            return {"run_id": run_id, "status": "SKIPPED_ALREADY_PROCESSED",
                    "input_count": 0, "valid_count": 0, "rejected_count": 0,
                    "output_count": 0, "alerts": [], "message": "Same file content was already processed."}

        rows, unexpected = read_batch(source)
        stage("01_INGESTION", f"Read {len(rows)} data rows from {source_name}")
        customer_ids = ensure_customers(conn, customers_path)
        stage("02_REFERENCE_LOAD", f"Loaded {len(customer_ids)} customer reference IDs")
        stage("03_SCHEMA_VALIDATION", f"Required columns present; unexpected columns={unexpected or 'none'}", "WARNING" if unexpected else "PASS")
        if unexpected:
            # Unexpected columns are observable schema drift; known columns are still processed.
            logger.warning("Unexpected columns in %s: %s", source_name, unexpected)

        existing_ids = {
            r[0] for r in conn.execute("SELECT application_id FROM loan_applications")
        }
        accepted: list[dict[str, Any]] = []
        rejects: list[tuple] = []
        seen: set[str] = set()
        for row_number, row in enumerate(rows, start=2):
            cleaned, reasons = validate_record(
                row, customer_ids, set(config["allowed_statuses"]),
                int(config["age_min"]), int(config["age_max"])
            )
            app_id = (row.get("application_id") or "").strip()
            if cleaned:
                if app_id in seen:
                    reasons.append("duplicate application_id within batch")
                    cleaned = None
                elif app_id in existing_ids:
                    reasons.append("application_id already exists in target")
                    cleaned = None
            if cleaned:
                seen.add(app_id)
                accepted.append(cleaned)
            else:
                rejects.append((
                    run_id, source_name, row_number, app_id or None,
                    json.dumps(row, sort_keys=True), "; ".join(reasons), iso(now)
                ))

        reject_reason_counts = Counter()
        for rejected_row in rejects:
            for reason in rejected_row[5].split("; "):
                if reason:
                    reject_reason_counts[reason] += 1
        validation_detail = (
            f"checked={len(rows)}, accepted={len(accepted)}, rejected={len(rejects)}, "
            f"reject_reasons={dict(reject_reason_counts)}"
        )
        stage("04_RECORD_VALIDATION", validation_detail, "WARNING" if rejects else "PASS")
        conn.executemany("""
            INSERT INTO rejected_records(run_id, source_file, source_row_number,
                application_id, raw_record, reject_reason, rejected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rejects)

        for rec in accepted:
            conn.execute("""
                INSERT INTO loan_applications(application_id, customer_id, age,
                    loan_amount, status, event_timestamp, source_file, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec["application_id"], rec["customer_id"], rec["age"],
                rec["loan_amount"], rec["status"], rec["event_timestamp"],
                source_name, run_id
            ))

        stage("05_TARGET_PUBLISH", f"Inserted {len(accepted)} accepted rows; quarantined {len(rejects)} rows", "WARNING" if rejects else "PASS")
        current_profiles = profile_records(accepted)
        stage("06_DATA_PROFILING", f"Created profiles for {len(current_profiles)} columns")
        conn.executemany("""
            INSERT INTO current_profiles(run_id, column_name, data_type, profile_json)
            VALUES (?, ?, ?, ?)
        """, [
            (run_id, col, "numeric" if col in NUMERIC_COLUMNS else "categorical",
             json.dumps(profile, sort_keys=True))
            for col, profile in current_profiles.items()
        ])

        baseline_rows = conn.execute(
            "SELECT column_name, profile_json FROM baseline_profiles"
        ).fetchall()
        baseline = {r["column_name"]: json.loads(r["profile_json"]) for r in baseline_rows}
        baseline_volume_row = conn.execute(
            "SELECT COUNT(*) AS n FROM loan_applications WHERE run_id=(SELECT established_from_run_id FROM baseline_profiles LIMIT 1)"
        ).fetchone()
        baseline_volume = int(baseline_volume_row["n"]) if baseline else 0

        alerts: list[dict[str, str]] = []
        if not baseline and accepted:
            conn.executemany("""
                INSERT INTO baseline_profiles(column_name, data_type, profile_json,
                    established_from_run_id, established_at)
                VALUES (?, ?, ?, ?, ?)
            """, [
                (col, "numeric" if col in NUMERIC_COLUMNS else "categorical",
                 json.dumps(profile, sort_keys=True), run_id, iso(now))
                for col, profile in current_profiles.items()
            ])
        elif baseline:
            alerts = compare_profiles(
                baseline, current_profiles, baseline_volume, len(accepted),
                config, run_id, now
            )

        # Schema drift is an explicit alert, not a silent pass.
        if unexpected:
            alerts.append({
                "alert_type": "schema_change", "column_name": "",
                "severity": "warning", "baseline_value": ",".join(APP_COLUMNS),
                "current_value": ",".join(APP_COLUMNS + unexpected),
                "explanation": f"Unexpected source columns detected: {', '.join(unexpected)}. They were ignored; review the producer schema."
            })

        # Freshness is measured from the newest valid event timestamp in this batch.
        valid_times = [parse_timestamp(r["event_timestamp"]) for r in accepted]
        if valid_times:
            newest = max(valid_times)
            lag_minutes = (now - newest).total_seconds() / 60
            max_lag = float(config["max_freshness_lag_minutes"])
            if lag_minutes > max_lag:
                alerts.append({
                    "alert_type": "freshness", "column_name": "event_timestamp",
                    "severity": "warning", "baseline_value": f"<= {max_lag:g} minutes",
                    "current_value": f"{lag_minutes:.1f} minutes",
                    "explanation": f"Newest valid event is {lag_minutes:.1f} minutes old; configured maximum lag is {max_lag:g} minutes."
                })

        for alert in alerts:
            logger.warning("ALERT | type=%s | column=%s | severity=%s | %s",
                           alert["alert_type"], alert.get("column_name") or "(batch)",
                           alert["severity"], alert["explanation"])
        stage("07_BASELINE_AND_MONITORING", f"Baseline {'established' if not baseline and accepted else 'compared'}; generated {len(alerts)} alerts", "WARNING" if alerts else "PASS")
        _insert_alerts(conn, run_id, alerts, now)
        output_count = conn.execute(
            "SELECT COUNT(*) FROM loan_applications WHERE run_id=?", (run_id,)
        ).fetchone()[0]
        status = "SUCCESS_WITH_REJECTS" if rejects else "SUCCESS"
        conn.execute("""
            INSERT INTO processed_batches(batch_hash, source_file, first_run_id, processed_at)
            VALUES (?, ?, ?, ?)
        """, (batch_hash, source_name, run_id, iso(now)))
        conn.execute("""
            UPDATE batch_runs SET ended_at=?, status=?, input_count=?, valid_count=?,
                rejected_count=?, output_count=? WHERE run_id=?
        """, (iso(now), status, len(rows), len(accepted), len(rejects), output_count, run_id))
        conn.commit()
        elapsed_seconds = round(time.perf_counter() - started_clock, 4)
        stage("08_RECONCILIATION", f"input={len(rows)} = accepted={len(accepted)} + rejected={len(rejects)}; target_output={output_count}", "PASS" if len(rows) == len(accepted) + len(rejects) and output_count == len(accepted) else "FAIL")
        stage("09_REPORTING", f"Run status={status}; elapsed={elapsed_seconds}s")

        report = {
            "run_id": run_id, "source_file": source_name, "status": status,
            "input_count": len(rows), "valid_count": len(accepted),
            "rejected_count": len(rejects), "output_count": output_count,
            "alerts": alerts, "unexpected_columns": unexpected,
            "baseline_established": bool(not baseline and accepted),
            "freshness_checked_at": iso(now),
            "duration_seconds": elapsed_seconds,
            "rows_per_second": round(len(rows) / elapsed_seconds, 2) if elapsed_seconds > 0 else None,
            "stage_results": stage_results,
            "validation_reject_reason_counts": dict(reject_reason_counts),
        }
        (report_path / f"{run_id.lower()}_report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        logger.info("PIPELINE COMPLETE | run_id=%s | status=%s | duration=%ss", run_id, status, elapsed_seconds)
        logger.info("=" * 72)
        return report
    except Exception as exc:
        conn.rollback()
        conn.execute("""
            UPDATE batch_runs SET ended_at=?, status='FAILED', failure_reason=?
            WHERE run_id=?
        """, (iso(now), str(exc), run_id))
        conn.commit()
        logger.exception("PIPELINE FAILED | run_id=%s | source=%s", run_id, source_name)
        raise
    finally:
        conn.close()


def reconciliation(db_path: str | Path, run_id: str) -> dict[str, int | bool]:
    conn = connect_db(db_path)
    row = conn.execute("""
        SELECT input_count, valid_count, rejected_count, output_count
        FROM batch_runs WHERE run_id=?
    """, (run_id,)).fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Unknown run_id: {run_id}")
    result = {
        "input_count": row["input_count"],
        "valid_count": row["valid_count"],
        "rejected_count": row["rejected_count"],
        "output_count": row["output_count"],
        "counts_reconcile": row["input_count"] == row["valid_count"] + row["rejected_count"]
            and row["valid_count"] == row["output_count"],
    }
    conn.close()
    return result
