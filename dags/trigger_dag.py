from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

CONTAINER_PATH = Variable.get("CONTAINER_PATH")
path = Variable.get('run_path', default_var='/data/run')

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
    task_trigger_dag = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="dag_id_1",
        conf={"table_name": "table_1"},
        dag=dag
    )
    with TaskGroup("bash_tasks") as bash_tasks:
        task_check_sensor = ExternalTaskSensor(
            task_id="external_file_sensor",
            allowed_states=["success"],
            external_task_id=None,
            mode="poke",
            poke_interval=30,
            timeout=600,
        )

    task_sensor >> task_trigger_dag >> task_bash
