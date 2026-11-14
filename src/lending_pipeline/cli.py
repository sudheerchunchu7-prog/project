from __future__ import annotations
import argparse, json, logging, time, uuid
from pathlib import Path
from .settings import ROOT, load_settings
from .spark_pipeline import run_spark_job
from .storage import connect, file_hash, batch_already_processed, start_run, finish_run, record_batch, publish_spark_rows, publish_rejects

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def run_pipeline(customers: Path, applications: Path, mode: str, db_path: Path | None = None) -> dict:
    settings = load_settings(); version = settings["transformation_version"]
    digest = file_hash(customers) + file_hash(applications)
    import hashlib
    digest = hashlib.sha256(digest.encode()).hexdigest()
    conn = connect(db_path); run_id = "RUN_" + uuid.uuid4().hex[:12].upper()
    if batch_already_processed(conn, digest, mode, version):
        conn.close(); return {"status":"SKIPPED_ALREADY_PROCESSED", "processing_mode":mode, "batch_hash":digest}
    start_run(conn, run_id, mode); conn.commit()
    job = None; metrics = {}; publish_seconds = 0.0
    try:
        job = run_spark_job(str(customers), str(applications), settings["max_loan_amount"], mode, run_id, settings["spark_shuffle_partitions"])
        metrics = job["metrics"]
        publish_start = time.perf_counter()
        conn.execute("BEGIN")
        customer_fields = ["customer_id","customer_name","date_of_birth","region","customer_segment","created_at","first_seen_run_id"]
        app_fields = ["application_id","customer_id","loan_amount","application_status","age_at_application","loan_amount_category","application_timestamp","source_system","updated_at","run_id","processing_mode","source_file","source_row_number"]
        publish_spark_rows(conn, job["customers"], "customers", customer_fields, settings["sqlite_batch_size"])
        published_apps = publish_spark_rows(conn, job["applications"], "loan_applications", app_fields, settings["sqlite_batch_size"])
        published_rejects = publish_rejects(conn, job["rejects"], run_id, mode, settings["sqlite_batch_size"])
        if published_apps != metrics["output_count"] or published_rejects != metrics["rejected_count"]:
            raise RuntimeError(f"Publish reconciliation failed: expected apps={metrics['output_count']}, rejects={metrics['rejected_count']}; published apps={published_apps}, rejects={published_rejects}")
        publish_seconds = time.perf_counter() - publish_start
        finish_run(conn, run_id, mode, metrics, job["spark_duration_seconds"], publish_seconds)
        record_batch(conn, digest, mode, version, f"{customers.name};{applications.name}", run_id)
        conn.commit()
        report = {"run_id":run_id,"status":"SUCCESS","processing_mode":mode,"metrics":metrics,
                  "published_application_rows":published_apps,"published_rejected_rows":published_rejects,"spark_duration_seconds":job["spark_duration_seconds"],
                  "sqlite_publish_duration_seconds":publish_seconds,"partition_count":job["partition_count"],
                  "physical_plan":job["physical_plan"]}
        out = ROOT / "reports/generated"; out.mkdir(parents=True, exist_ok=True)
        (out / f"{run_id.lower()}_{mode.lower()}.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        logging.info("Completed %s: %s", run_id, metrics)
        return report
    except Exception as exc:
        conn.rollback()
        try:
            finish_run(conn, run_id, mode, metrics, job["spark_duration_seconds"] if job else 0, publish_seconds, "FAILED", str(exc))
            conn.execute("INSERT INTO pipeline_errors(run_id,error_time,error_type,error_message) VALUES (?,?,?,?)", (run_id,__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),type(exc).__name__,str(exc)))
            conn.commit()
        except Exception: conn.rollback()
        raise
    finally:
        if job and job.get("spark"): job["spark"].stop()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Run the local PySpark lending pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate"); gen.add_argument("--customers",type=int,default=100); gen.add_argument("--applications",type=int,default=250); gen.add_argument("--seed",type=int,default=42); gen.add_argument("--out",default="data/sample")
    run = sub.add_parser("run"); run.add_argument("--customers",default="data/sample/customers.csv"); run.add_argument("--applications",default="data/sample/loan_applications.csv"); run.add_argument("--mode",choices=["BASELINE","OPTIMIZED"],default="BASELINE"); run.add_argument("--db",default=None)
    args = parser.parse_args()
    if args.command == "generate":
        from .generator import generate_data
        cp, ap = generate_data(ROOT / args.out,args.customers,args.applications,args.seed); print(f"Generated {cp} and {ap}")
    else:
        result = run_pipeline(ROOT / args.customers, ROOT / args.applications, args.mode, Path(args.db) if args.db else None)
        print(json.dumps(result, indent=2, default=str))
if __name__ == "__main__": main()
