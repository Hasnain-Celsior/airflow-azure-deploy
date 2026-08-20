"""
Local stand-in for the two non-database sources used by the multi_source_etl DAG.

  GET /v1/orders                    paginated JSON, read from orders_db
  GET /v1/feeds/campaign_spend.csv  the partner CSV feed, served as text/csv
  POST /admin/feed/{state}          flip the feed to "missing"/"available" to
                                    demo the late-partner-feed path

Run on the host (Airflow reaches it at http://host.docker.internal:8000):

    set ORDERS_DB_DSN=postgresql://user:pass@localhost:5432/orders_db
    python source_api.py
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

# No default: this service must never silently fall back to a local database
# when the env var is missing in a deployed environment.
ORDERS_DB_DSN = os.environ["ORDERS_DB_DSN"]
CSV_PATH = Path(__file__).parent / "data" / "campaign_spend.csv"

app = FastAPI(title="ETL source systems (local mock)")

# lets you demo the "partner feed hasn't landed yet" branch without deleting the file
feed_available = True


@app.get("/v1/orders")
def get_orders(
    start: str | None = None,
    end: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=5000),
):
    """Orders in the requested window, paginated the way the DAG expects."""
    where = []
    params: list[object] = []
    if start:
        where.append("ordered_at >= %s")
        params.append(start)
    if end:
        where.append("ordered_at < %s")
        params.append(end)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    offset = (page - 1) * page_size

    with psycopg2.connect(ORDERS_DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM orders {where_sql}", params)
            total = cur.fetchone()["n"]

            cur.execute(
                f"""
                SELECT order_id, customer_id, amount, currency, status, ordered_at
                FROM orders
                {where_sql}
                ORDER BY ordered_at, order_id
                LIMIT %s OFFSET %s
                """,
                [*params, page_size, offset],
            )
            rows = cur.fetchall()

    for row in rows:
        row["amount"] = float(row["amount"])
        row["ordered_at"] = row["ordered_at"].isoformat()

    return {
        "results": rows,
        "page": page,
        "page_size": page_size,
        "has_next": offset + len(rows) < total,
        "total": total,
    }


@app.get("/v1/feeds/campaign_spend.csv")
def get_campaign_spend():
    """The partner marketing-spend feed. 404 mimics a feed that hasn't landed."""
    if not feed_available:
        raise HTTPException(status_code=404, detail="Feed has not landed yet")
    if not CSV_PATH.exists():
        raise HTTPException(status_code=404, detail=f"No such feed file: {CSV_PATH.name}")
    return FileResponse(CSV_PATH, media_type="text/csv", filename=CSV_PATH.name)


@app.post("/admin/feed/{state}")
def set_feed_state(state: str):
    """state=missing makes the feed 404; state=available restores it."""
    global feed_available
    if state not in ("missing", "available"):
        raise HTTPException(status_code=400, detail="state must be 'missing' or 'available'")
    feed_available = state == "available"
    return {"feed_available": feed_available}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))