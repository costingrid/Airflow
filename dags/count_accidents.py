from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

CONTAINER_PATH = Variable.get("CONTAINER_PATH")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 15),
}


def process_output():
    with open(f"{CONTAINER_PATH}/data/count.txt", "r") as file:
        data = file.read()
        print(f"Count of accidents: {data}")


with DAG('count_accidents', default_args=default_args, schedule_interval='@daily') as dag:
    log_start = BashOperator(
        task_id="log_start",
        bash_command="echo 'Start processing file'",
    )

    count_rows = SparkSubmitOperator(
        task_id="count_rows",
        application=f"{CONTAINER_PATH}/spark/count_rows.py",
        conn_id="spark",
    )

    get_count = PythonOperator(
        task_id="get_count",
        python_callable=process_output,
    )

    log_start >> count_rows >> get_count
