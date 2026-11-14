from __future__ import annotations
import hashlib, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from itertools import islice
from .settings import ROOT

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def file_hash(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""): digest.update(chunk)
    return digest.hexdigest()

def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else ROOT / "database/lending_pipeline.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path); conn.execute("PRAGMA foreign_keys = ON")
    schema = (ROOT / "database/schema.sql").read_text(encoding="utf-8")
    conn.executescript(schema)
    return conn

def _batches(iterator, size):
    while True:
        batch = list(islice(iterator, size))
        if not batch: break
        yield batch

def _row_values(row, fields):
    values = []
    for field in fields:
        value = row[field]
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        values.append(value)
    return tuple(values)

def publish_spark_rows(conn: sqlite3.Connection, dataframe, table: str, fields: list[str], batch_size: int = 500) -> int:
    """Stream Spark rows to one SQLite writer in bounded executemany batches."""
    placeholders = ",".join("?" for _ in fields)
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(fields)}) VALUES ({placeholders})"
    total = 0
    for batch in _batches((_row_values(r, fields) for r in dataframe.toLocalIterator()), batch_size):
        conn.executemany(sql, batch); total += len(batch)
    return total

def publish_rejects(conn, dataframe, run_id, mode, batch_size=500):
    fields = ["entity_type","business_key","original_payload","reason_code","reason_message","source_file","source_row_number"]
    sql = "INSERT INTO rejected_records(entity_type,business_key,original_payload,reason_code,reason_message,run_id,processing_mode,source_file,source_row_number) VALUES (?,?,?,?,?,?,?,?,?)"
    total = 0
    def values():
        for r in dataframe.toLocalIterator():
            yield tuple(r[f] for f in fields[:5]) + (run_id, mode, r["source_file"], int(r["source_row_number"]))
    for batch in _batches(values(), batch_size): conn.executemany(sql, batch); total += len(batch)
    return total

def batch_already_processed(conn, digest, mode, version):
    return conn.execute("SELECT 1 FROM processed_batches WHERE batch_hash=? AND processing_mode=? AND transformation_version=?", (digest,mode,version)).fetchone() is not None

def start_run(conn, run_id, mode):
    conn.execute("INSERT INTO pipeline_runs(run_id,processing_mode,start_time,run_status) VALUES (?,?,?,?)", (run_id,mode,utc_now(),"RUNNING"))

def finish_run(conn, run_id, mode, metrics, spark_seconds, publish_seconds, status="SUCCESS", error=None):
    conn.execute("""UPDATE pipeline_runs SET end_time=?,run_status=?,input_customer_count=?,input_application_count=?,valid_customer_count=?,valid_application_count=?,rejected_count=?,output_count=?,spark_duration_seconds=?,publish_duration_seconds=?,error_message=? WHERE run_id=?""",
      (utc_now(),status,metrics.get("input_customer_count",0),metrics.get("input_application_count",0),metrics.get("valid_customer_count",0),metrics.get("valid_application_count",0),metrics.get("rejected_count",0),metrics.get("output_count",0),spark_seconds,publish_seconds,error,run_id))

def record_batch(conn, digest, mode, version, source_file, run_id):
    conn.execute("INSERT INTO processed_batches VALUES (?,?,?,?,?,?)", (digest,mode,version,source_file,run_id,utc_now()))
