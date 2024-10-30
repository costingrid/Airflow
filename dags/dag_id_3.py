from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from jobs_dag import *

with DAG(
    dag_id="dag_id_3",
    start_date=config["dag_id_3"]["start_date"],
    schedule=config["dag_id_3"]["schedule_interval"]
) as dag:
    task_log = PythonOperator(
        task_id="log_database_connection",
        op_kwargs={"dag_id": f"{dag.dag_id}", "database": "airflow"},
        python_callable=print_params_fn,
        dag=dag
    )
    task_bash = BashOperator(
        task_id="get_current_user",
        bash_command="whoami",
        dag=dag
    )
    task_branch = BranchPythonOperator(
        task_id="branch_condition",
        python_callable=branch_condition,
        dag=dag
    )
    task_create = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres-conn",
        trigger_rule=TriggerRule.NONE_FAILED,
        sql="CREATE TABLE IF NOT EXISTS table_3 (id SERIAL PRIMARY KEY, name VARCHAR(50))",
        dag=dag
    )
    task_insert_row = SQLExecuteQueryOperator(
        task_id="insert_new_row",
        trigger_rule=TriggerRule.NONE_FAILED,
        sql="INSERT INTO table_3 (name) VALUES ('John')",
        dag=dag
    )
    task_query = SQLExecuteQueryOperator(
        task_id="query_table",
        trigger_rule=TriggerRule.NONE_FAILED,
        sql="SELECT * FROM table_3",
        dag=dag
    )
    task_log >> task_bash
    task_bash >> task_branch
    task_branch >> task_create
    task_branch >> task_insert_row
    task_create >> task_insert_row
    task_insert_row >> task_query

