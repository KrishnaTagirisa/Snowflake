"""
Generate two days of sample OMS extracts that exercise the pipeline's edge cases:
  * day 2 changes a customer's city and segment      -> new SCD2 version in dim_customer
  * day 2 re-sends an order with a new status         -> stg_orders keeps latest version
  * day 2 amends an order and removes a line          -> delete+insert drops the stale line
  * a duplicate row inside one file                   -> deduplicated in silver
  * a messy price and blank fields                    -> TRY_ casts / NULL handling

Usage:
    python generate_sample_data.py                    # writes ./out/retail/...
    aws s3 sync out/retail s3://acme-retail-landing/retail/
"""
import csv
import json
from pathlib import Path

OUT = Path(__file__).parent / "out" / "retail"

CUSTOMER_HEADER = ["customer_id", "first_name", "last_name", "email", "phone", "city",
                   "country", "segment", "created_at", "updated_at"]
PRODUCT_HEADER = ["product_id", "product_name", "category", "sub_category", "brand",
                  "unit_price", "currency", "is_active", "updated_at"]

customers_d1 = [
    ["C001", "priya", "sharma", "Priya.Sharma@example.com", "07700 900001", "rugby", "gb", "consumer",
     "2025-03-01 09:00:00", "2025-03-01 09:00:00"],
    ["C002", "tom", "baker", "tom.baker@example.com", "", "london", "GB", "corporate",
     "2025-06-10 12:00:00", "2025-06-10 12:00:00"],
    ["C003", "aisha", "khan", "aisha.khan@example", "07700 900003", "leeds", "GB", "",
     "2026-01-15 08:30:00", "2026-01-15 08:30:00"],
    # exact duplicate inside the same file
    ["C002", "tom", "baker", "tom.baker@example.com", "", "london", "GB", "corporate",
     "2025-06-10 12:00:00", "2025-06-10 12:00:00"],
]
customers_d2 = [
    # C001 moved to Coventry and became a small business -> SCD2 change
    ["C001", "priya", "sharma", "priya.sharma@example.com", "07700 900001", "coventry", "GB",
     "small_business", "2025-03-01 09:00:00", "2026-11-11 14:05:00"],
    customers_d1[1],
    customers_d1[2],
    ["C004", "liam", "o'neil", "liam@example.co.uk", "+44 7700 900004", "manchester", "GB", "consumer",
     "2026-11-11 16:00:00", "2026-11-11 16:00:00"],
]

products = [
    ["P100", "Trail Running Shoe", "footwear", "running", "Stride", "89.99", "GBP", "true", "2026-01-01 00:00:00"],
    ["P200", "Waterproof Jacket", "apparel", "outerwear", "Northline", "1,249.00", "GBP", "true", "2026-01-01 00:00:00"],
    ["P300", "Insulated Bottle", "accessories", "", "Hydra", "24.50", "", "1", "2026-01-01 00:00:00"],
]


def order(oid, cust, ts, status, items, shipping, updated):
    total = round(sum(i["quantity"] * i["unit_price"] - i.get("discount_amount", 0) for i in items) + shipping, 2)
    return {"order_id": oid, "customer_id": cust, "order_ts": ts, "status": status, "channel": "web",
            "currency": "GBP", "shipping_amount": shipping, "order_total": total,
            "updated_at": updated, "items": items}


orders_d1 = [
    order("O9001", "C001", "2026-11-10 10:15:00", "PAID",
          [{"line_number": 1, "product_id": "P100", "quantity": 1, "unit_price": 89.99},
           {"line_number": 2, "product_id": "P300", "quantity": 2, "unit_price": 24.50, "discount_amount": 4.90}],
          4.99, "2026-11-10 10:16:00"),
    order("O9002", "C002", "2026-11-10 13:40:00", "PLACED",
          [{"line_number": 1, "product_id": "P200", "quantity": 1, "unit_price": 1249.00},
           {"line_number": 2, "product_id": "P300", "quantity": 1, "unit_price": 24.50}],
          0, "2026-11-10 13:40:00"),
]
orders_d2 = [
    # O9001 re-sent: shipped
    {**orders_d1[0], "status": "SHIPPED", "updated_at": "2026-11-11 09:00:00"},
    # O9002 amended: bottle removed -> line 2 must disappear downstream
    order("O9002", "C002", "2026-11-10 13:40:00", "PAID",
          [{"line_number": 1, "product_id": "P200", "quantity": 1, "unit_price": 1249.00}],
          0, "2026-11-11 08:00:00"),
    # new order from C001 AFTER the address change -> must join to the new SCD2 version
    order("O9003", "C001", "2026-11-11 15:30:00", "PAID",
          [{"line_number": 1, "product_id": "P200", "quantity": 1, "unit_price": 1249.00}],
          0, "2026-11-11 15:31:00"),
]


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)


def write_ndjson(path: Path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


for d, custs, ords in (("2026-11-10", customers_d1, orders_d1), ("2026-11-11", customers_d2, orders_d2)):
    tag = d.replace("-", "")
    write_csv(OUT / f"customers/load_date={d}/customers_{tag}.csv", CUSTOMER_HEADER, custs)
    write_csv(OUT / f"products/load_date={d}/products_{tag}.csv", PRODUCT_HEADER, products)
    write_ndjson(OUT / f"orders/load_date={d}/orders_{tag}.json", ords)
    (OUT / f"_control/load_date={d}").mkdir(parents=True, exist_ok=True)
    (OUT / f"_control/load_date={d}/_SUCCESS").touch()

print(f"Sample files written under {OUT}")
