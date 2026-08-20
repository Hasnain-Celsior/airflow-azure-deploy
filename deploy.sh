#!/usr/bin/env bash
# One-shot Azure deployment: Postgres Flexible Server + two Container Apps.
#
#   az login
#   ./deploy.sh
#
# Re-running is safe — every create is guarded, and the container apps are
# updated in place rather than recreated.
set -euo pipefail

# ---------------------------------------------------------------------------
# Settings — change these, then run.
# ---------------------------------------------------------------------------
RG="rg-airflow-etl"
LOCATION="centralindia"
ENV_NAME="cae-airflow"
PG_SERVER="pg-airflow-etl-$RANDOM"   # must be globally unique
PG_ADMIN="pgadmin"
PG_SKU="Standard_B1ms"               # burstable, the cheapest tier that works
PG_STORAGE=32

# Alphanumeric only: these end up inside connection URLs, and special
# characters would need percent-encoding in every one of them.
gen_pw() { openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 24; }

PG_PASSWORD="$(gen_pw)"
AIRFLOW_ADMIN_PASSWORD="$(gen_pw)"
FERNET_KEY="$(openssl rand -base64 32)"
JWT_SECRET="$(openssl rand -base64 32)"

# ---------------------------------------------------------------------------
echo "==> [1/6] Resource group"
az group create -n "$RG" -l "$LOCATION" -o none

echo "==> [2/6] Postgres Flexible Server ($PG_SERVER) — takes ~5 min"
az postgres flexible-server create \
  --resource-group "$RG" --name "$PG_SERVER" --location "$LOCATION" \
  --admin-user "$PG_ADMIN" --admin-password "$PG_PASSWORD" \
  --sku-name "$PG_SKU" --tier Burstable --storage-size "$PG_STORAGE" \
  --version 16 --yes -o none

# 0.0.0.0 is the special rule meaning "allow other Azure services" — this is
# what lets the Container Apps reach the server.
az postgres flexible-server firewall-rule create \
  --resource-group "$RG" --name "$PG_SERVER" \
  --rule-name allow-azure --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none

# Your own IP, so you can run seed_databases.py from this machine.
MY_IP="$(curl -s https://api.ipify.org)"
az postgres flexible-server firewall-rule create \
  --resource-group "$RG" --name "$PG_SERVER" \
  --rule-name my-laptop --start-ip-address "$MY_IP" --end-ip-address "$MY_IP" -o none

PG_HOST="${PG_SERVER}.postgres.database.azure.com"

echo "==> [3/6] Databases"
for db in airflow customers_db orders_db warehouse_db; do
  az postgres flexible-server db create \
    --resource-group "$RG" --server-name "$PG_SERVER" --database-name "$db" -o none
  echo "    $db"
done

BASE="postgresql://${PG_ADMIN}:${PG_PASSWORD}@${PG_HOST}:5432"
SSL="?sslmode=require"

echo "==> [4/6] Seeding source data"
python seed_databases.py "${BASE}/postgres${SSL}"

echo "==> [5/6] Container Apps environment + source-api"
az containerapp env create -n "$ENV_NAME" -g "$RG" -l "$LOCATION" -o none

az containerapp up \
  --name source-api --resource-group "$RG" --environment "$ENV_NAME" \
  --source ./source-api --target-port 8000 --ingress internal -o none

az containerapp secret set -n source-api -g "$RG" \
  --secrets orders-dsn="${BASE}/orders_db${SSL}" -o none
az containerapp update -n source-api -g "$RG" \
  --set-env-vars ORDERS_DB_DSN=secretref:orders-dsn -o none

# Internal ingress terminates on port 80 and forwards to the target port.
API_FQDN="$(az containerapp show -n source-api -g "$RG" \
  --query properties.configuration.ingress.fqdn -o tsv)"
echo "    source-api reachable in-cluster at http://${API_FQDN}"

echo "==> [6/6] Airflow"
az containerapp up \
  --name airflow --resource-group "$RG" --environment "$ENV_NAME" \
  --source ./airflow --target-port 8080 --ingress external -o none

az containerapp secret set -n airflow -g "$RG" --secrets \
  metadata-conn="postgresql+psycopg2://${PG_ADMIN}:${PG_PASSWORD}@${PG_HOST}:5432/airflow${SSL}" \
  customers-conn="${BASE}/customers_db${SSL}" \
  warehouse-conn="${BASE}/warehouse_db${SSL}" \
  fernet-key="${FERNET_KEY}" \
  jwt-secret="${JWT_SECRET}" \
  admin-password="${AIRFLOW_ADMIN_PASSWORD}" -o none

# minReplicas=1 is not optional: Container Apps scales to zero by default, and
# a scheduler that gets scaled away stops running your DAGs.
az containerapp update -n airflow -g "$RG" \
  --min-replicas 1 --max-replicas 1 \
  --cpu 1.0 --memory 2.0Gi \
  --set-env-vars \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=secretref:metadata-conn \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__FERNET_KEY=secretref:fernet-key \
    AIRFLOW__API_AUTH__JWT_SECRET=secretref:jwt-secret \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True \
    AIRFLOW_CONN_CUSTOMERS_DB=secretref:customers-conn \
    AIRFLOW_CONN_WAREHOUSE_DB=secretref:warehouse-conn \
    AIRFLOW_CONN_ORDERS_API="http://${API_FQDN}" \
    _AIRFLOW_WWW_USER_USERNAME=admin \
    _AIRFLOW_WWW_USER_PASSWORD=secretref:admin-password \
  -o none

UI="$(az containerapp show -n airflow -g "$RG" \
  --query properties.configuration.ingress.fqdn -o tsv)"

cat <<EOF

=====================================================================
  Airflow UI   https://${UI}
  Username     admin
  Password     ${AIRFLOW_ADMIN_PASSWORD}

  Postgres     ${PG_HOST}
  Admin user   ${PG_ADMIN}
  Password     ${PG_PASSWORD}
=====================================================================
Save these now — the passwords are generated per run and not stored
anywhere outside this output.

Tear everything down with:  az group delete -n ${RG} --yes
EOF
