#!/usr/bin/env bash
# Runs the whole Airflow control plane in one container: api-server, scheduler,
# and dag-processor. Fine for LocalExecutor at this scale, and it keeps the
# Task Execution API call from the scheduler on localhost instead of crossing
# the Container Apps network.
set -euo pipefail

echo "==> Running metadata DB migrations"
airflow db migrate

# Idempotent: 'users create' errors if the user already exists, so don't let a
# restart of an existing deployment fail here.
if [[ -n "${_AIRFLOW_WWW_USER_USERNAME:-}" ]]; then
  echo "==> Ensuring admin user '${_AIRFLOW_WWW_USER_USERNAME}' exists"
  airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
    --firstname Admin --lastname User --role Admin \
    --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" || true
fi

# Container Apps injects PORT; ingress targetPort must match it.
PORT="${PORT:-8080}"

echo "==> Starting dag-processor"
airflow dag-processor &

echo "==> Starting scheduler"
airflow scheduler &

echo "==> Starting api-server on 0.0.0.0:${PORT}"
airflow api-server --host 0.0.0.0 --port "${PORT}" &

# If ANY of the three dies, kill the container so Container Apps restarts the
# replica. Without this a dead scheduler leaves a healthy-looking web UI.
wait -n
echo "!! An Airflow process exited — shutting down the replica"
exit 1
