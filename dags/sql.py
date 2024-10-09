import subprocess

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from PostgreSQLCountRows import PostgreSQLCountRowsOperator
from jobs_dag import *


def check_table_exists(table_name):
    hook = PostgresHook(postgres_conn_id="airflow_postgres_conn")
    sql = f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'"
    query = hook.get_first(sql)
    if query:
        return "insert_new_row"
    return "create_table"


def get_current_user(ti):
    user = subprocess.check_output("whoami").decode("utf-8").strip()
    ti.xcom_push(key="user", value=user)


with DAG(
        dag_id="dag_sql",
        start_date=config["dag_id_2"]["start_date"],
        schedule=config["dag_id_2"]["schedule_interval"]

) as dag:
    log_start = BashOperator(
        task_id="log_start",
        bash_command="echo 'Start processing SQL files'",
        dag=dag
    )
    get_current_user = PythonOperator(
        task_id="get_current_user",
        python_callable=get_current_user,
        provide_context=True,
        dag=dag
    )
    check_table_exists = BranchPythonOperator(
        task_id="check_table_exists",
        python_callable=check_table_exists,
        op_args=["table_name"],
        dag=dag
    )
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="airflow_postgres_conn",
        sql="""CREATE TABLE table_name(custom_id integer NOT NULL,
                    user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);""",
        dag=dag
    )
    insert_new_row = SQLExecuteQueryOperator(
        task_id="insert_new_row",
        conn_id="airflow_postgres_conn",
        trigger_rule="none_failed",
        sql="""INSERT INTO table_name(custom_id, user_name, timestamp)
                    VALUES (1, 'user1', '2024-09-02 00:00:00');""",
        dag=dag
    )
    count_rows = PostgreSQLCountRowsOperator(
        task_id="count_rows",
        conn_id="airflow_postgres_conn",
        table_name="table_name",
        dag=dag
    )

    log_start >> get_current_user >> check_table_exists
    check_table_exists >> create_table >> insert_new_row >> count_rows
    check_table_exists >> insert_new_row >> count_rows
