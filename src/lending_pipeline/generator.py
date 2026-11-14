from __future__ import annotations
import csv, random, tempfile, shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

CUSTOMER_FIELDS = ["customer_id", "customer_name", "date_of_birth", "region", "customer_segment", "created_at"]
APPLICATION_FIELDS = ["application_id", "customer_id", "loan_amount", "application_status", "application_timestamp", "source_system", "updated_at"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
SEGMENTS = ["RETAIL", "SALARIED", "SELF_EMPLOYED"]
STATUSES = ["APPROVED", "REJECTED", "PENDING"]

def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)

def generate_data(output_dir: str | Path, customer_count: int = 100, application_count: int = 250, seed: int = 42) -> tuple[Path, Path]:
    """Generate repeatable customer and application CSVs."""
    if customer_count < 1 or application_count < 0:
        raise ValueError("customer_count must be >= 1 and application_count must be >= 0")
    rng = random.Random(seed); out = Path(output_dir)
    customers = []
    for i in range(1, customer_count + 1):
        born = date(1965, 1, 1) + timedelta(days=rng.randint(0, 18000))
        created = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=rng.randint(0, 700))
        customers.append({"customer_id": f"C{i:06d}", "customer_name": f"Customer {i}", "date_of_birth": born.isoformat(), "region": rng.choice(REGIONS), "customer_segment": rng.choice(SEGMENTS), "created_at": created.isoformat().replace("+00:00", "Z")})
    applications = []
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(1, application_count + 1):
        ts = base + timedelta(minutes=rng.randint(0, 525600))
        applications.append({"application_id": f"A{i:08d}", "customer_id": rng.choice(customers)["customer_id"], "loan_amount": f"{rng.randint(10000, 5000000):.2f}", "application_status": rng.choice(STATUSES), "application_timestamp": ts.isoformat().replace("+00:00", "Z"), "source_system": "LENDING_PORTAL", "updated_at": (ts + timedelta(hours=rng.randint(0, 72))).isoformat().replace("+00:00", "Z")})
    cp, ap = out / "customers.csv", out / "loan_applications.csv"
    _write_csv(cp, CUSTOMER_FIELDS, customers); _write_csv(ap, APPLICATION_FIELDS, applications)
    return cp, ap

def generate_negative_scenarios(output_dir: str | Path) -> None:
    """Create small deliberately invalid application/customer CSV fixtures."""
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        cp, ap = generate_data(Path(tmp), 3, 3, 7)
        customers = list(csv.DictReader(cp.open(encoding="utf-8")))
        apps = list(csv.DictReader(ap.open(encoding="utf-8")))
    customers.append(dict(customers[0]))  # duplicate customer key
    _write_csv(out / "duplicate_customers.csv", CUSTOMER_FIELDS, customers)
    apps[0]["loan_amount"] = "-10"; apps[1]["application_status"] = "UNKNOWN"; apps[2]["customer_id"] = "MISSING_CUSTOMER"
    _write_csv(out / "invalid_applications.csv", APPLICATION_FIELDS, apps)
    _write_csv(out / "empty_applications.csv", APPLICATION_FIELDS, [])
