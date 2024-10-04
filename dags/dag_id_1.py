from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

from jobs_dag import *

with DAG(
    dag_id="dag_id_1",
    start_date=config["dag_id_1"]["start_date"],
    schedule=config["dag_id_1"]["schedule_interval"]
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
    task_create = EmptyOperator(
        task_id="create_table",
        dag=dag
    )
    task_insert_row = EmptyOperator(
        task_id="insert_new_row",
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )
    task_query = EmptyOperator(
        task_id="query_table",
        dag=dag
    )
    task_log >> task_bash
    task_bash >> task_branch
    task_branch >> task_create
    task_branch >> task_insert_row
    task_create >> task_insert_row
    task_insert_row >> task_query
