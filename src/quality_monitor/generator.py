"""Deterministic synthetic data generator for baseline and anomaly batches."""
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

def generate(customers_path: str, batch_path: str, rows: int = 1000,
             seed: int = 42, anomaly: bool = False, late: bool = False,
             edge_cases: bool = False) -> None:
    rng = random.Random(seed)
    customers_file = Path(customers_path)
    batch_file = Path(batch_path)
    customers_file.parent.mkdir(parents=True, exist_ok=True)
    batch_file.parent.mkdir(parents=True, exist_ok=True)
    customer_ids = [f"C{i:06d}" for i in range(1, max(101, rows // 2 + 2))]
    with customers_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name"])
        for cid in customer_ids:
            writer.writerow([cid, f"Customer {cid[-6:]}"])

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    expected_cases = []
    with batch_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["application_id", "customer_id", "age", "loan_amount", "status", "event_timestamp"])
        for i in range(rows):
            age = rng.randint(21, 65)
            amount = round(rng.uniform(10000, 100000), 2)
            status = rng.choices(["approved", "rejected", "pending"], weights=[0.65, 0.25, 0.10])[0]
            if anomaly and i % 4 == 0:
                age = ""
            if anomaly and i % 3 == 0:
                status = "rejected"
            event_time = now - timedelta(minutes=rng.randint(0, 45))
            if late:
                event_time = now - timedelta(hours=5, minutes=rng.randint(0, 30))
            # Optional, deliberately injected edge cases. Labels are written to a
            # separate manifest, never mixed into the source business columns.
            if edge_cases and i == 0:
                age = 18  # inclusive lower boundary: valid
                expected_cases.append({"row_number": 2, "case": "age_lower_boundary", "expected": "valid"})
            elif edge_cases and i == 1:
                age = 100  # inclusive upper boundary: valid
                expected_cases.append({"row_number": 3, "case": "age_upper_boundary", "expected": "valid"})
            elif edge_cases and i == 2:
                age = 150
                expected_cases.append({"row_number": 4, "case": "age_out_of_range", "expected": "rejected"})
            elif edge_cases and i == 3:
                amount = -1000
                expected_cases.append({"row_number": 5, "case": "negative_loan_amount", "expected": "rejected"})
            elif edge_cases and i == 4:
                age = ""
                expected_cases.append({"row_number": 6, "case": "missing_age", "expected": "valid_and_profiled_as_null"})
            elif edge_cases and i == 5:
                # Unknown reference key exercises referential-integrity rejection.
                customer_id = "C_UNKNOWN"
                expected_cases.append({"row_number": 7, "case": "unknown_customer_reference", "expected": "rejected"})
            if edge_cases and i == 6:
                application_id = f"APP_{seed}_00000000"  # duplicate of first row
                expected_cases.append({"row_number": 8, "case": "duplicate_application_id", "expected": "rejected"})
            writer.writerow([
                application_id if edge_cases and i == 6 else f"APP_{seed}_{i:08d}",
                customer_id if edge_cases and i == 5 else rng.choice(customer_ids),
                age,
                amount,
                status,
                event_time.isoformat()
            ])
    if edge_cases:
        manifest = batch_file.with_suffix(batch_file.suffix + ".manifest.json")
        import json
        manifest.write_text(json.dumps({"source_file": batch_file.name, "seed": seed,
                                        "injected_cases": expected_cases}, indent=2), encoding="utf-8")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--customers", default="data/input/customers.csv")
    p.add_argument("--output", default="data/input/batch.csv")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--anomaly", action="store_true")
    p.add_argument("--late", action="store_true")
    p.add_argument("--edge-cases", action="store_true", help="Inject labelled boundary, invalid, null, reference and duplicate cases; labels go to a sidecar manifest")
    a = p.parse_args()
    generate(a.customers, a.output, a.rows, a.seed, a.anomaly, a.late, a.edge_cases)
    print(f"Generated {a.rows} rows in {a.output}")
