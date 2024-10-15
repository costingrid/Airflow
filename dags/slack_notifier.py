from airflow import DAG

from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from jobs_dag import *

SLACK_CHANNEL = "airflow"
SLACK_MESSAGE = """
Hello, you have a new message from Apache Airflow.
Dag: {{ dag }}
Task: {{ task }}
Execution Time: {{ ds }}
:tada:
"""
webhook_url = Variable.get("webhook_url")

with DAG(
        dag_id="slack_notifier",
        start_date=datetime(2024, 10, 10),
        schedule_interval="@daily"
) as dag:
    empty = EmptyOperator(
        task_id="empty",
        dag=dag
    )
    slack_notification = SlackWebhookOperator(
        task_id="slack_notification",
        message=SLACK_MESSAGE,
        slack_webhook_conn_id="slack_webhook_conn",
        dag=dag
    )
    empty2 = EmptyOperator(
        task_id="empty2",
        dag=dag
    )

    empty >> slack_notification >> empty2
