# Airflow ETL on Azure

Azure Container Apps + one Azure Database for PostgreSQL Flexible Server.

## Layout

    airflow/        Dockerfile (apache/airflow:3.3.1) + dags/ + entrypoint
    source-api/     Dockerfile + FastAPI service + data/campaign_spend.csv
    seed_databases.py   creates & populates the three ETL databases
    deploy.sh       provisions everything in one run

## Prerequisites

    az login
    az extension add --name containerapp --upgrade
    az provider register --namespace Microsoft.App
    az provider register --namespace Microsoft.OperationalInsights

`deploy.sh` builds images in Azure Container Registry via `az containerapp up
--source`, so Docker is not needed locally.

## Deploy

    ./deploy.sh

It prints the Airflow UI URL and the generated passwords at the end. They are
not stored anywhere else — save them.

## Architecture notes

**One Postgres server, four databases** (`airflow`, `customers_db`,
`orders_db`, `warehouse_db`). Separate servers would cost 4x and buy nothing;
Postgres already isolates databases. There are deliberately no foreign keys
between `customers` and `orders` — Postgres cannot enforce them across
databases.

**One Airflow container** runs api-server, scheduler, and dag-processor with
`LocalExecutor`. In Airflow 3 the scheduler reaches the api-server over the
Task Execution API; keeping them co-located makes that a localhost call and
avoids paying for a second always-on replica. If task volume outgrows this,
split into separate container apps and set
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` to the api-server's internal FQDN.

**`--min-replicas 1` is load-bearing.** Container Apps scales to zero by
default. A scheduler that gets scaled away silently stops running DAGs while
the UI still looks healthy.

**Azure Postgres requires TLS**, so every connection URL carries
`?sslmode=require`. Generated passwords are alphanumeric only, because they are
embedded in URLs and special characters would need percent-encoding in each one.

## Connection URLs

All of them are Container App secrets, set by `deploy.sh` — nothing is
committed. Same server and credentials throughout; only the database name at
the end of the URL differs.

| Where | Variable | Database |
|---|---|---|
| `airflow` app | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `airflow` |
| `airflow` app | `AIRFLOW_CONN_CUSTOMERS_DB` | `customers_db` |
| `airflow` app | `AIRFLOW_CONN_WAREHOUSE_DB` | `warehouse_db` |
| `source-api` app | `ORDERS_DB_DSN` | `orders_db` |
| your laptop, once | `seed_databases.py <url>` | `postgres` |

`AIRFLOW_CONN_ORDERS_API` is not a database — it points at the `source-api`
container app's internal FQDN.

## Migrating existing data instead of seeding

`deploy.sh` calls `seed_databases.py`, which generates data from scratch. To
move real data from a local Postgres instead, replace that step with:

    for db in customers_db orders_db warehouse_db; do
      pg_dump "postgresql://postgres:PASS@localhost:5432/$db" \
        | psql "postgresql://pgadmin:PASS@HOST:5432/$db?sslmode=require"
    done

## Teardown

    az group delete -n rg-airflow-etl --yes
