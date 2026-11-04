"""Generate deterministic synthetic sales data so the pipeline runs with zero setup.

The last month deliberately contains dirty rows (duplicate order, negative
quantity, unparseable date, blank price) so the silver quarantine has something
real to catch on a first run.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = PROJECT_ROOT / "data" / "raw"

HEADER = [
    "order_id",
    "order_date",
    "store_id",
    "store_name",
    "region",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "currency",
]

STORES = [
    ("S01", "Austin Central", "South"),
    ("S02", "Denver North", "West"),
    ("S03", "Portland East", "West"),
    ("S04", "Chicago Loop", "Midwest"),
]

PRODUCTS = [
    ("P001", "Trail Runner 2", "Footwear"),
    ("P002", "City Walker", "Footwear"),
    ("P003", "Storm Shell", "Outerwear"),
    ("P004", "Merino Base Layer", "Apparel"),
    ("P005", "Summit Pack 30L", "Bags"),
    ("P006", "Trail Bottle 750ml", "Accessories"),
]


def _rows_for_day(day: date, rng: random.Random, counter: int) -> tuple[list[list[str]], int]:
    rows: list[list[str]] = []
    for _ in range(rng.randint(18, 28)):
        store_id, store_name, region = rng.choice(STORES)
        product_id, product_name, category = rng.choice(PRODUCTS)
        counter += 1
        rows.append(
            [
                f"ORD-{day:%Y%m%d}-{counter:05d}",
                day.isoformat(),
                store_id,
                store_name,
                region,
                product_id,
                product_name,
                category,
                str(rng.randint(1, 4)),
                f"{rng.uniform(15, 220):.2f}",
                "USD",
            ]
        )
    return rows, counter


def generate(out_dir: Path, seed: int = 42, days: int = 90) -> list[Path]:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = date(2024, 1, 1)
    by_month: dict[str, list[list[str]]] = {}
    counter = 0

    for offset in range(days):
        day = start + timedelta(days=offset)
        rows, counter = _rows_for_day(day, rng, counter)
        by_month.setdefault(day.strftime("%Y-%m"), []).extend(rows)

    last_month = sorted(by_month)[-1]
    sample = by_month[last_month][0]
    dirty_rows = [list(sample)]  # exact duplicate order_id
    dirty_rows.extend(
        [
            f"ORD-DIRTY-{i:03d}", "2024-03-15", "S01", "Austin Central", "South",
            "P001", "Trail Runner 2", "Footwear", qty, "120.00", "USD",
        ]
        for i, qty in enumerate(["-2", "-1", "0"])
    )
    dirty_rows.extend(
        [
            [
                "ORD-BADDATE-001", "31/02/2024", "S02", "Denver North", "West",
                "P003", "Storm Shell", "Outerwear", "1", "199.00", "USD",
            ],
            [
                "ORD-BLANKPRICE-001", "2024-03-16", "S03", "Portland East", "West",
                "P004", "Merino Base Layer", "Apparel", "2", "", "USD",
            ],
        ]
    )
    by_month[last_month].extend(dirty_rows)

    written: list[Path] = []
    for month, rows in sorted(by_month.items()):
        path = out_dir / f"orders_{month}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(HEADER)
            writer.writerows(rows)
        written.append(path)

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    files = generate(args.out, seed=args.seed, days=args.days)
    total = sum(len(p.read_text(encoding="utf-8").splitlines()) - 1 for p in files)
    print(f"wrote {len(files)} files, {total} rows to {args.out}")


if __name__ == "__main__":
    main()
