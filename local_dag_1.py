from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['example'],
)
def start_dag():
    # TriggerDagRunOperator(
    #     task_id="test_trigger_dagrun",
    #     trigger_dag_id="target_dag",  # Ensure this equals the dag_id of the DAG to trigger
    #     conf={"message": "my_info"},
    # )
    @task(outlets=[Dataset('start_task', extra={'load_date': 'date'})])
    def start_task():
        return 'start_task'

    start_task()
    # bash_task = BashOperator(
    #     task_id="bash_task",
    #     bash_command='echo "Here is the message: $message"',
    #     env={'message': '{{ dag_run.conf.get("message") }}'},
    # )


start_dag()
