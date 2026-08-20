"""
Creates and populates the three ETL source/target databases on any Postgres
server — local, Azure Flexible Server, anything you can reach.

    python seed_databases.py "postgresql://user:pass@HOST:5432/postgres?sslmode=require"

Creates:
  customers_db  500 customers (ids 1-500, matching data/campaign_spend.csv)
  orders_db     ~3200 historic orders + 300 dated across the last 14 days
  warehouse_db  empty — the DAG's load task creates customer_revenue_summary

Safe to re-run: every insert is ON CONFLICT DO NOTHING and the databases are
only created if missing.
"""

from __future__ import annotations

import random
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

import psycopg2
from psycopg2 import sql

DATABASES = ("customers_db", "orders_db", "warehouse_db")

SEGMENTS = ["enterprise"] * 1 + ["mid_market"] * 3 + ["smb"] * 6
STATUSES = (
    ["completed"] * 12 + ["processing"] * 4 + ["pending"] * 2 + ["refunded"] + ["cancelled"]
)
FIRST = ["Alex", "Priya", "Sam", "Mei", "Omar", "Lena", "Kai", "Nora", "Tomas", "Aisha",
         "Ravi", "Elena", "Jonas", "Yuki", "Ibrahim", "Clara", "Diego", "Hana", "Noah", "Zara"]
LAST = ["Reyes", "Nakamura", "Okafor", "Lindqvist", "Haddad", "Moreau", "Silva", "Novak",
        "Ferrari", "Kowalski", "Abadi", "Bergstrom", "Ivanov", "Costa", "Tan", "Weber"]


def db_url(base: str, dbname: str) -> str:
    """Swap the database name on a connection URL, preserving ?sslmode=require."""
    parts = urlparse(base)
    return urlunparse(parts._replace(path=f"/{dbname}"))


def create_databases(base: str) -> None:
    # CREATE DATABASE cannot run inside a transaction block.
    conn = psycopg2.connect(base)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for name in DATABASES:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                if cur.fetchone():
                    print(f"  {name:<14} already exists")
                    continue
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                print(f"  {name:<14} created")
    finally:
        conn.close()


def seed_customers(base: str) -> None:
    now = datetime.now(timezone.utc)
    rows = []
    for cid in range(1, 501):
        rows.append((
            cid,
            f"{random.choice(FIRST)} {random.choice(LAST)}",
            random.choice(SEGMENTS),
            (now - timedelta(days=random.randint(30, 900))).date(),
            # ~10% churned, so the DAG's `WHERE is_active = true` filter is meaningful
            random.random() > 0.10,
        ))

    with psycopg2.connect(db_url(base, "customers_db")) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY,
                    full_name   TEXT NOT NULL,
                    segment     TEXT NOT NULL,
                    signup_date DATE NOT NULL,
                    is_active   BOOLEAN NOT NULL DEFAULT true
                )
            """)
            cur.executemany(
                "INSERT INTO customers (customer_id, full_name, segment, signup_date, is_active)"
                " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING",
                rows,
            )
            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_active) FROM customers")
            total, active = cur.fetchone()
        conn.commit()
    print(f"  customers      {total} rows ({active} active)")


def seed_orders(base: str) -> None:
    now = datetime.now(timezone.utc)
    rows = []
    seq = 1

    # Historic backfill: thin coverage over ~2.5 years, so the warehouse has depth.
    for _ in range(3200):
        ordered_at = now - timedelta(days=random.uniform(14, 900))
        rows.append((f"ORD-{seq:06d}", random.randint(1, 500),
                     round(random.uniform(15.0, 900.0), 2), "USD",
                     random.choice(STATUSES), ordered_at))
        seq += 1

    # Recent orders: without these a single nightly window catches almost nothing.
    for _ in range(300):
        ordered_at = now - timedelta(days=random.uniform(0, 14))
        rows.append((f"ORD-{seq:06d}", random.randint(1, 500),
                     round(random.uniform(15.0, 900.0), 2), "USD",
                     random.choice(STATUSES), ordered_at))
        seq += 1

    with psycopg2.connect(db_url(base, "orders_db")) as conn:
        with conn.cursor() as cur:
            # No FK to customers: it lives in a different database, and Postgres
            # cannot enforce foreign keys across databases.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id    TEXT PRIMARY KEY,
                    customer_id INTEGER NOT NULL,
                    amount      NUMERIC(10, 2) NOT NULL,
                    currency    TEXT NOT NULL DEFAULT 'USD',
                    status      TEXT NOT NULL,
                    ordered_at  TIMESTAMPTZ NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS orders_ordered_at_idx ON orders (ordered_at)")
            cur.executemany(
                "INSERT INTO orders (order_id, customer_id, amount, currency, status, ordered_at)"
                " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING",
                rows,
            )
            cur.execute("SELECT COUNT(*) FROM orders")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders WHERE ordered_at >= NOW() - INTERVAL '1 day'")
            last_day = cur.fetchone()[0]
        conn.commit()
    print(f"  orders         {total} rows ({last_day} in the last 24h)")


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    base = sys.argv[1]

    random.seed(20260820)  # reproducible across re-runs and machines

    print("Creating databases...")
    create_databases(base)
    print("Seeding...")
    seed_customers(base)
    seed_orders(base)
    print("  warehouse_db   left empty (the DAG's load task creates its table)")
    print("\nDone.")


if __name__ == "__main__":
    main()
