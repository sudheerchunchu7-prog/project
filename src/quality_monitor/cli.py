import argparse
import json
import logging
from pathlib import Path
from .pipeline import process_batch, reconciliation

def main():
    parser = argparse.ArgumentParser(description="Batch data-quality monitor")
    parser.add_argument("--input", required=True, help="Loan application CSV")
    parser.add_argument("--customers", default="data/input/customers.csv")
    parser.add_argument("--db", default="data/quality_monitor.db")
    parser.add_argument("--reports", default="reports")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    result = process_batch(args.input, args.customers, args.db, args.reports, config)
    print(json.dumps(result, indent=2))
    if result.get("run_id") and result.get("status") != "SKIPPED_ALREADY_PROCESSED":
        print("\nReconciliation:")
        print(json.dumps(reconciliation(args.db, result["run_id"]), indent=2))

if __name__ == "__main__":
    main()
