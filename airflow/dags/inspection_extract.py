from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="inspection_extract",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger-only
    catchup=False,
)
def inspection_extract_dag():

    @task
    def extract():
        print("inspection_extract ran")

    extract()

inspection_extract_dag()
