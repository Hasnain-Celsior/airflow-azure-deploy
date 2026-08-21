from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="underwriting_master",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger-only
    catchup=False,
)
def underwriting_master_dag():

    @task
    def extract():
        print("underwriting_master ran")

    extract()

underwriting_master_dag()
