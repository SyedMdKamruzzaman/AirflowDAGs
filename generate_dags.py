import datetime
from airflow.decorators import dag,task
import requests
import pathlib

#list of cities
cities = ["chicago", "paris", "london"]

#list of european countries
european_cities = ["paris", "london"]

# create weather update folder if it does not exist
report_location =  pathlib.Path(__file__).parent.parent / "weather_updates"
if not report_location.exists():
    report_location.mkdir()

# define the list of airflow tasks
@task
def generate_report(city):
    response = requests.get(f"http://wttr.in/{city}")
    if response.ok:
        return response.text
    return None

@task
def write_report(city,report_data):
    report_file = report_location / f"{city}.txt"
    print(f"writing data to {report_file}...")
    with open(report_file,"w") as f:
        f.write(report_data)

@task
def print_report(city, report_data):
    print(f"This is the weather report for: {city}")
    print(report_data)

# iterate over each city
for city in cities:
    # set the dag id
    dag_id = f"{city}_weather_update"
    # define the dag
    @dag(dag_id=dag_id, start_date=datetime.datetime(2022, 1, 1), schedule_interval=None)
    def my_dag():
        # call the generate report task
        report_data = generate_report(city)
        # if the city is European, call write report
        if city in european_cities:
            write_report(city, report_data)
        else:
            # otherwise call print report
            print_report(city, report_data)

    # save the dag in the globals list
    # this enables airflow to see the DAG and add it to the
    # list of tracked DAGs
    globals()[dag_id] = my_dag()
