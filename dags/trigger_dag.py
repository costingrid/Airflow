import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable, DagRun
from airflow.utils.task_group import TaskGroup

CONTAINER_PATH = Variable.get("CONTAINER_PATH")
path = Variable.get('run_path', default_var='/data/newdata')


def print_result(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info(f"Result: {kwargs['ti'].xcom_pull(task_ids='external_file_sensor')}")


def get_most_recent_execution_date(dt):
    dag_runs = DagRun.find(dag_id='dag_id_3')
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date
    else:
        raise Exception("No DagRun found")


with DAG(
        dag_id="trigger_dag",
        start_date=datetime(2024, 9, 1),
        schedule='@daily'
) as dag:
    task_sensor = FileSensor(
        task_id="file_sensor",
        filepath=f"{CONTAINER_PATH}/data/run",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=3000,
        dag=dag
    )
    with TaskGroup(group_id="bash_tasks") as bash_tasks:
        task_check_sensor = ExternalTaskSensor(
            task_id="wait_for_dag",
            allowed_states=['success'],
            failed_states=['failed', 'skipped', 'upstream_failed'],
            external_dag_id="dag_id_3",
            execution_date_fn=get_most_recent_execution_date,
            mode='reschedule',
            external_task_id='query_table',
            executor="CeleryExecutor",
        )
        print_result = PythonOperator(
            task_id="print_result",
            python_callable=print_result,
            provide_context=True
        )
        remove_file = BashOperator(
            task_id="remove_file",
            bash_command=f"rm {CONTAINER_PATH}{path}/run",
            dag=dag
        )
        create_finished_file = BashOperator(
            task_id="create_finished_file",
            bash_command=f"touch {CONTAINER_PATH}{path}" + '/finished_#{{ ts_nodash }}.txt',
            dag=dag
        )

        task_check_sensor >> print_result >> remove_file >> create_finished_file

    task_trigger_dag = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="dag_id_3",
        dag=dag
    )

    task_sensor >> task_trigger_dag >> bash_tasks
