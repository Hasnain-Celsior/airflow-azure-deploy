from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="deductible_master",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger-only
    catchup=False,
)
def deductible_master_dag():

    @task
    def extract():
        print("deductible_master ran")

    extract()

deductible_master_dag()
