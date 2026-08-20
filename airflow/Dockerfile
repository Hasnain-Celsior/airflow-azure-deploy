FROM apache/airflow:3.3.1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dags/ ${AIRFLOW_HOME}/dags/
COPY scripts/ /opt/scripts/

USER root
RUN chmod +x /opt/scripts/*.sh
USER airflow

ENTRYPOINT ["/opt/scripts/entrypoint.sh"]
