from airflow import DAG
from airflow.decorators import task
from include.datasets import MY_FILE
from datetime import datetime



with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023,1,1),
    tags=['producer'],
    catchup=False
):

    @task(outlets=[MY_FILE])
    def update_my_file():
        with open(MY_FILE.uri,"a+") as f:
            f.write("producer update")

    update_my_file()
