"""Simple reproducible timing harness; run from the project root."""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    input_dir = ROOT / "benchmark" / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    customers = input_dir / "customers.csv"
    batch = input_dir / f"batch_{args.rows}.csv"
    db = ROOT / "benchmark" / "benchmark.db"
    reports = ROOT / "benchmark" / "reports"
    if db.exists():
        db.unlink()  # each benchmark starts with a clean local target
    env = dict(__import__("os").environ)
    env["PYTHONPATH"] = str(ROOT / "src")
    gen = [sys.executable, "-m", "quality_monitor.generator", "--customers", str(customers),
           "--output", str(batch), "--rows", str(args.rows), "--seed", "12345"]
    start = time.perf_counter()
    subprocess.run(gen, cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    generation_seconds = time.perf_counter() - start
    run = [sys.executable, "run.py", "--input", str(batch), "--customers", str(customers),
           "--db", str(db), "--reports", str(reports)]
    start = time.perf_counter()
    completed = subprocess.run(run, cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    processing_seconds = time.perf_counter() - start
    result = json.loads(completed.stdout.split("\nReconciliation:")[0])
    output = {"rows_requested": args.rows, "generation_seconds": round(generation_seconds, 4),
              "processing_seconds": round(processing_seconds, 4),
              "rows_per_second": round(args.rows / processing_seconds, 2) if processing_seconds else None,
              "pipeline_status": result["status"], "input_count": result["input_count"],
              "valid_count": result["valid_count"], "rejected_count": result["rejected_count"],
              "run_id": result["run_id"]}
    target = ROOT / "benchmark" / "benchmark_result.json"
    target.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps(output, indent=2))
    print(f"Saved: {target.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
