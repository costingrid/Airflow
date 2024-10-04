from datetime import datetime
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

config = {
    'dag_id_1': {'schedule_interval': '0 0 * * *', 'start_date': datetime(2024, 9, 1),
                 'table_name': 'table_1'},
    'dag_id_2': {'schedule_interval': '@hourly', 'start_date': datetime(2024, 9, 2),
                 'table_name': 'table_2'},
    'dag_id_3': {'schedule_interval': '@daily', 'start_date': datetime(2024, 9, 3),
                 'table_name': 'table_3'},
}


def branch_condition():
    a = random.randint(1, 2)
    if a == 1:
        return "create_table"
    else:
        return "insert_new_row"


def print_params_fn(**kwargs):
    import logging
    logging.info(f'{kwargs["dag_id"]} started processing tables in database: {kwargs["database"]}')
    return None
