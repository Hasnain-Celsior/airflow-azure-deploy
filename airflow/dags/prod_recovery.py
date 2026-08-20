from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="prod_recovery",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
def hello_world_dag():

    @task
    def hello_world():
        print("Hello, World!")

    hello_world()

hello_world_dag()