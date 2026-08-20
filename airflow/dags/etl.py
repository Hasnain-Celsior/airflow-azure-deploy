"""
Multi-Source ETL Pipeline for Apache Airflow
=============================================
A realistic pattern: pull from several different systems, reconcile
them into one clean dataset, and load into a central warehouse.

SOURCES
-------
1. customers_db   - Postgres OLTP database (your app's source of truth)
2. orders_api     - REST API (an orders/payments microservice)
3. campaign_spend - CSV partner/vendor feed, downloaded from the same local
                     source API and landed on disk before parsing

DESTINATION
-----------
warehouse_db - a central Postgres warehouse, loaded idempotently via
               an upsert (ON CONFLICT) so re-running a day is safe.

LOCAL SETUP
-----------
Airflow runs in Docker (docker-compose.yaml) while Postgres and the source
API run on the host, so every connection must use host.docker.internal
rather than localhost.

  1. Start the source API on the host:  python source_api.py
     (set ORDERS_DB_DSN first if your orders_db credentials differ)
  2. Create the three connections below in Admin -> Connections.

REQUIRED AIRFLOW CONNECTIONS  (Admin -> Connections in the UI, or via CLI)
---------------------------------------------------------------------
  conn_id            type      notes
  ----------------------------------------------------------------
  customers_db       Postgres  host.docker.internal:5432 / customers_db
  warehouse_db       Postgres  host.docker.internal:5432 / warehouse_db
  orders_api         HTTP      http://host.docker.internal:8000
                                (serves both /v1/orders and the CSV feed)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CUSTOMERS_DB_CONN_ID = "customers_db"
WAREHOUSE_DB_CONN_ID = "warehouse_db"
ORDERS_API_CONN_ID = "orders_api"

MARKETING_SPEND_ENDPOINT = "/v1/feeds/campaign_spend.csv"   # partner feed, served by the source API
MARKETING_SPEND_LOCAL_PATH = "/tmp/campaign_spend.csv"      # where the fetched feed is landed before parsing

WAREHOUSE_TABLE = "customer_revenue_summary"

def resolve_window(data_interval_start, data_interval_end) -> tuple[datetime, datetime]:
    """Return the [start, end) window this run should extract.

    Airflow 3 gives manually triggered runs a NULL logical_date and NULL data
    interval unless one is passed explicitly, so fall back to the last 24 hours
    for ad-hoc runs rather than blowing up on None.
    """
    if data_interval_start is not None and data_interval_end is not None:
        return data_interval_start, data_interval_end

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=1)
    logger.warning(
        "No data interval on this run (manual trigger?) — defaulting to the last 24h: %s to %s",
        start.isoformat(), end.isoformat(),
    )
    return start, end


default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}


@dag(
    dag_id="multi_source_etl",
    description="Reconciles customers (Postgres), orders (REST API), and "
                 "marketing spend (CSV) into a central warehouse table.",
    # NOTE: a bare cron string in Airflow 3 builds a CronTriggerTimetable, whose
    # data_interval_start == data_interval_end (zero width) — which would make the
    # incremental orders extract below return nothing. CronDataIntervalTimetable
    # gives the [previous_run, this_run] window this pipeline actually needs.
    schedule=CronDataIntervalTimetable("0 3 * * *", timezone="UTC"),  # 3 AM daily, after upstream batch jobs finish
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,             # avoid overlapping runs against the same warehouse table
    default_args=default_args,
    tags=["etl", "warehouse", "multi-source"],
)
def multi_source_etl():

    # -----------------------------------------------------------------
    # EXTRACT — one task per source, grouped for readability in the UI
    # -----------------------------------------------------------------
    @task_group(group_id="extract")
    def extract_group():

        @task
        def extract_customers() -> list[dict]:
            """Pull active customers from the OLTP Postgres database."""
            hook = PostgresHook(postgres_conn_id=CUSTOMERS_DB_CONN_ID)
            sql = """
                SELECT customer_id, full_name, segment, signup_date
                FROM customers
                WHERE is_active = true
            """
            df = hook.get_pandas_df(sql)
            logger.info("Extracted %d customers", len(df))
            if df.empty:
                raise AirflowException("customers_db returned zero rows — refusing to continue")
            return df.to_dict(orient="records")

        @task
        def extract_orders(data_interval_start=None, data_interval_end=None) -> list[dict]:
            """Pull the window's orders from the internal REST API, paginated."""
            hook = HttpHook(method="GET", http_conn_id=ORDERS_API_CONN_ID)
            window_start, window_end = resolve_window(data_interval_start, data_interval_end)

            all_orders = []
            page = 1
            while True:
                response = hook.run(
                    endpoint="/v1/orders",
                    data={
                        "start": window_start.isoformat(),
                        "end": window_end.isoformat(),
                        "page": page,
                        "page_size": 500,
                    },
                )
                payload = response.json()
                orders = payload.get("results", [])
                if not orders:
                    break
                all_orders.extend(orders)
                if not payload.get("has_next"):
                    break
                page += 1

            logger.info("Extracted %d orders across %d page(s)", len(all_orders), page)
            return all_orders

        @task
        def extract_marketing_spend() -> list[dict]:
            """Download the partner-supplied marketing spend CSV feed, then parse it."""
            hook = HttpHook(method="GET", http_conn_id=ORDERS_API_CONN_ID)
            try:
                response = hook.run(endpoint=MARKETING_SPEND_ENDPOINT)
            except AirflowException:
                # Partner feeds sometimes arrive late (the endpoint 404s) — don't hard-fail
                # the whole DAG, downstream transform will treat spend as zero for these customers.
                logger.warning("Marketing spend feed unavailable at %s, continuing with empty spend",
                                MARKETING_SPEND_ENDPOINT)
                return []

            # Land the feed on disk first, mirroring an SFTP/S3 drop, then parse it
            with open(MARKETING_SPEND_LOCAL_PATH, "wb") as fh:
                fh.write(response.content)
            df = pd.read_csv(MARKETING_SPEND_LOCAL_PATH)

            logger.info("Extracted %d marketing spend rows", len(df))
            return df.to_dict(orient="records")

        return {
            "customers": extract_customers(),
            "orders": extract_orders(),
            "marketing_spend": extract_marketing_spend(),
        }

    # -----------------------------------------------------------------
    # TRANSFORM — join and aggregate the three sources
    # -----------------------------------------------------------------
    @task
    def transform(sources: dict, data_interval_start=None, data_interval_end=None) -> list[dict]:
        customers = pd.DataFrame(sources["customers"])
        orders = pd.DataFrame(sources["orders"])
        spend = pd.DataFrame(sources["marketing_spend"])

        # Normalize types across sources — a common real-world pain point
        customers["customer_id"] = customers["customer_id"].astype(str)
        if not orders.empty:
            orders["customer_id"] = orders["customer_id"].astype(str)
            orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce").fillna(0)
        if not spend.empty:
            spend["customer_id"] = spend["customer_id"].astype(str)

        # Aggregate orders per customer
        if not orders.empty:
            revenue = orders.groupby("customer_id", as_index=False).agg(
                order_count=("order_id", "count"),
                total_revenue=("amount", "sum"),
            )
        else:
            revenue = pd.DataFrame(columns=["customer_id", "order_count", "total_revenue"])

        # Aggregate spend per customer (if the feed attributes spend to a customer)
        if not spend.empty:
            spend_agg = spend.groupby("customer_id", as_index=False).agg(
                marketing_spend=("spend", "sum")
            )
        else:
            spend_agg = pd.DataFrame(columns=["customer_id", "marketing_spend"])

        # Left-join everything onto the customer master list so every active
        # customer appears even with zero orders/spend
        result = (
            customers
            .merge(revenue, on="customer_id", how="left")
            .merge(spend_agg, on="customer_id", how="left")
        )
        result[["order_count", "total_revenue", "marketing_spend"]] = (
            result[["order_count", "total_revenue", "marketing_spend"]].fillna(0)
        )
        result["net_margin"] = result["total_revenue"] - result["marketing_spend"]

        # Stamp the row with the day being processed, NOT the day the task happens to
        # run. Re-running a failed day tomorrow must overwrite that day's rows, so this
        # has to come from the data interval for the upsert below to be idempotent.
        window_start, _ = resolve_window(data_interval_start, data_interval_end)
        result["load_date"] = window_start.date().isoformat()

        logger.info("Transformed dataset: %d customer rows", len(result))
        return result.to_dict(orient="records")

    # -----------------------------------------------------------------
    # LOAD — idempotent upsert into the central warehouse
    # -----------------------------------------------------------------
    @task
    def load(rows: list[dict]) -> int:
        if not rows:
            raise AirflowException("Transform produced zero rows — refusing to load")

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_DB_CONN_ID)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {WAREHOUSE_TABLE} (
                customer_id     TEXT,
                full_name       TEXT,
                segment         TEXT,
                signup_date     DATE,
                order_count     INTEGER,
                total_revenue   NUMERIC,
                marketing_spend NUMERIC,
                net_margin      NUMERIC,
                load_date       DATE,
                -- one row per customer per day: keeps daily history, and lets a
                -- re-run of a failed day overwrite only that day
                PRIMARY KEY (customer_id, load_date)
            )
        """
        hook.run(create_sql)

        upsert_sql = f"""
            INSERT INTO {WAREHOUSE_TABLE}
                (customer_id, full_name, segment, signup_date,
                 order_count, total_revenue, marketing_spend, net_margin, load_date)
            VALUES (%(customer_id)s, %(full_name)s, %(segment)s, %(signup_date)s,
                    %(order_count)s, %(total_revenue)s, %(marketing_spend)s,
                    %(net_margin)s, %(load_date)s)
            ON CONFLICT (customer_id, load_date) DO UPDATE SET
                full_name       = EXCLUDED.full_name,
                segment         = EXCLUDED.segment,
                signup_date     = EXCLUDED.signup_date,
                order_count     = EXCLUDED.order_count,
                total_revenue   = EXCLUDED.total_revenue,
                marketing_spend = EXCLUDED.marketing_spend,
                net_margin      = EXCLUDED.net_margin
        """
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.executemany(upsert_sql, rows)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        logger.info("Upserted %d rows into %s", len(rows), WAREHOUSE_TABLE)
        return len(rows)

    # -----------------------------------------------------------------
    # DATA QUALITY CHECK — cheap sanity check before calling the run a success
    # -----------------------------------------------------------------
    @task
    def validate_load(row_count: int):
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_DB_CONN_ID)
        db_count = hook.get_first(f"SELECT COUNT(*) FROM {WAREHOUSE_TABLE}")[0]
        if db_count < row_count:
            raise AirflowException(
                f"Data quality check failed: warehouse has {db_count} rows, "
                f"expected at least {row_count}"
            )
        logger.info("Validation passed: %d rows in %s", db_count, WAREHOUSE_TABLE)

    # -----------------------------------------------------------------
    # Wire the pipeline together
    # -----------------------------------------------------------------
    extracted = extract_group()
    transformed = transform(extracted)
    loaded_count = load(transformed)
    validate_load(loaded_count)


multi_source_etl()