from airflow import DAG,Dataset
from airflow.decorators import task
from include.datasets import MY_FILE
from datetime import datetime



with DAG(
    dag_id="consumer",
    schedule=[MY_FILE],
    start_date=datetime(2023,1,1),
    tags=["consumer"],
    catchup=False
):

    @task()
    def read_my_file():
        with open(MY_FILE.uri,"r") as f:
            print(f.read())

    read_my_file()



