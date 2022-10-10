from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.taskinstance import TaskInstance
from airflow.utils.db import provide_session
from airflow.models import XCom
import logging

log = logging.getLogger(__name__)


@task
def print_value(value):
    """Empty function"""
    # conf = {"message": "Hello World"},
    log.info("The knights of Ni", value, )


@task
def pull_data(ti=None):
    # value = context['ti'].xcom_pull(task_ids='push_data')
    value = ti.xcom_pull(task_ids="start_task", key='pipeline_outlets')
    print('xcom_pull_my', value)
    # return value


@task
def get_ti(ti=None):
    print(ti)


@task
def get_context(**context):
    print(context)


@task
def run_this_func(dag_run=None,):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dag_run: The DagRun object
    """
    print(dag_run.conf, )
    # print({{dag_run.conf}})

    # print(f"Remotely received value of {dag_run.conf.get('message')} for key=message")


# {"sasa": "okkkk"}
@task
def start_task(params=None):
    print(params)


@dag(
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=[Dataset('start_task')],
    tags=['example'],
)
def target_dag():
    # run_this_func()
    # run_this_func2()
    # push_data()
    run_this_func()
    start_task()
    get_ti()
    get_context()
    # pull_data()
    # print_value()

    # bash_task = BashOperator(
    #     task_id="bash_task",
    #     bash_command='echo "Here is the message: $message"',
    #     env={'message': '{{ dag_run.conf.get("message") }}'},
    # )


target_dag()
