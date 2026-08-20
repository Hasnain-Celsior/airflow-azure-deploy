from datetime import timedelta

import pendulum
from airflow.sdk import DAG, TaskGroup, task
from airflow.task.trigger_rule import TriggerRule


local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}


with DAG(
    dag_id="example_best_practices",
    description="Small DAG showing common scheduling, reliability, workflow, and resource settings.",
    schedule=None,  # Daily at 09:00 in the DAG timezone. Presets like "@daily" also work.
    start_date=pendulum.datetime(2026, 8, 13, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example", "best-practices"],
) as dag:

    @task(pool="default_pool")
    def extract():
        print("Extracting data")
        return {"records": 100}

    extract_result = extract()

    with TaskGroup(group_id="transform_steps") as transform_steps:

        @task(pool="default_pool")
        def clean(data):
            print(f"Cleaning {data['records']} records")
            return data

        @task(pool="default_pool")
        def validate(data):
            print("Validating data")
            raise RuntimeError("Forced failure for restart-on-failure testing")

        clean_result = clean(extract_result)
        validate_result = validate(clean_result)

    @task(pool="default_pool")
    def load(data):
        print(f"Loading {data['records']} records")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify():
        print("Notify runs after upstream tasks finish, even if one fails.")

    load_task = load(validate_result)

    extract_result >> transform_steps >> load_task >> notify()

'''
┌──────── Minute (0-59)
│ ┌────── Hour (0-23)
│ │ ┌──── Day of Month (1-31)
│ │ │ ┌── Month (1-12)
│ │ │ │ ┌─ Day of Week (0-6 or Sun-Sat)
│ │ │ │ │
* * * * *

'''