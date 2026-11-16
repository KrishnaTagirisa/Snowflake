"""
generate_sample_data.py
------------------------------------------------------------------------------
Generates a synthetic "customers" dataset (ID, Name, Country, DOB) that
stands in for the real file a source system would drop into S3.

Usage:
    python scripts/generate_sample_data.py --rows 200 --output data/sample_customers.csv
    python scripts/generate_sample_data.py --rows 200 --upload --bucket my-bucket --key raw/customers/customers.csv

Design notes:
  * Uses Faker when it's installed (richer, more realistic names) and falls
    back to a small built-in name/country pool otherwise, so the script has
    zero hard dependencies and always runs.
  * A fixed random seed makes the output reproducible across runs, which is
    useful for demos and for unit tests that assert on row counts/shape.
  * Optionally uploads the generated file straight to S3 via boto3, mimicking
    how a real source system would land data for the pipeline to pick up.
------------------------------------------------------------------------------
"""

import argparse
import csv
import os
import random
from datetime import date, timedelta

# --- Optional dependency: Faker -------------------------------------------
try:
    from faker import Faker

    _faker = Faker()
    _HAS_FAKER = True
except ImportError:  # pragma: no cover - fallback path
    _HAS_FAKER = False

# Fallback name/country pools used only if Faker is unavailable.
_FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Aditi",
    "Wei", "Hiroshi", "Fatima", "Carlos", "Ingrid", "Liam", "Noor",
]
_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor",
    "Thomas", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
]

# Countries deliberately match the seed mapping used later in the dbt
# project (dbt_project/seeds/country_code_mapping.csv) so the demo data
# exercises that join.
_COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Germany", "France",
    "India", "Australia", "Brazil", "Japan", "China", "Mexico", "Italy",
    "Spain", "Netherlands", "South Africa",
]


def _random_dob(min_age: int = 18, max_age: int = 75) -> date:
    """Return a random date of birth for someone between min_age and max_age."""
    today = date.today()
    start = today - timedelta(days=max_age * 365)
    end = today - timedelta(days=min_age * 365)
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def _random_name() -> str:
    if _HAS_FAKER:
        return _faker.name()
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _random_country() -> str:
    if _HAS_FAKER:
        return random.choice(_COUNTRIES)  # keep bounded to our seed mapping
    return random.choice(_COUNTRIES)


def generate_rows(n_rows: int, seed: int = 42):
    """Yield n_rows of synthetic (id, name, country, dob) records."""
    random.seed(seed)
    if _HAS_FAKER:
        Faker.seed(seed)

    for customer_id in range(1, n_rows + 1):
        yield {
            "id": customer_id,
            "name": _random_name(),
            "country": _random_country(),
            "dob": _random_dob().isoformat(),
        }

    # Sprinkle in a couple of intentionally messy rows so downstream data
    # quality checks (Airflow validation + dbt tests) have something to
    # catch -- this is realistic: source systems are rarely perfectly clean.
    yield {"id": n_rows + 1, "name": "  Extra  Spaces  ", "country": "usa", "dob": "1990-01-15"}
    yield {"id": n_rows + 2, "name": "Duplicate Country Case", "country": "GERMANY", "dob": "1985-06-30"}


def write_csv(path: str, n_rows: int, seed: int = 42) -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    rows = list(generate_rows(n_rows, seed))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "country", "dob"])
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def upload_to_s3(path: str, bucket: str, key: str) -> None:
    """Upload the generated file to S3. Requires AWS credentials in the env."""
    import boto3  # imported lazily so boto3 isn't required unless --upload is used

    s3 = boto3.client("s3")
    s3.upload_file(path, bucket, key)
    print(f"Uploaded {path} -> s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic customer data.")
    parser.add_argument("--rows", type=int, default=200, help="Number of clean rows to generate.")
    parser.add_argument("--output", type=str, default="data/sample_customers.csv", help="Output CSV path.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--upload", action="store_true", help="Upload the generated file to S3.")
    parser.add_argument("--bucket", type=str, default=os.environ.get("S3_BUCKET_NAME"), help="Target S3 bucket.")
    parser.add_argument("--key", type=str, default=os.environ.get("S3_SOURCE_KEY"), help="Target S3 object key.")
    args = parser.parse_args()

    total = write_csv(args.output, args.rows, args.seed)
    print(f"Wrote {total} rows to {args.output} (Faker {'used' if _HAS_FAKER else 'unavailable, used fallback pool'})")

    if args.upload:
        if not args.bucket or not args.key:
            raise SystemExit("--upload requires --bucket and --key (or S3_BUCKET_NAME/S3_SOURCE_KEY env vars).")
        upload_to_s3(args.output, args.bucket, args.key)


if __name__ == "__main__":
    main()
