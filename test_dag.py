import os
# import pendulum
from airflow.decorators import dag, task

from airflow import DAG, Dataset
from airflow.models import Variable
from datetime import datetime, date
from airflow.models.xcom import XCom


from airflow.models import TaskInstance


@dag(
    default_args={'owner': 'zirui', },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据运维', '测试用例']
)
def dag_info():
    @task
    def task_info(msg):
        print(msg)
        return {'msg': msg}

    task_info('start')


def get_context(context):
    return context['task_instance']


@dag(
    default_args={'owner': 'zirui', },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据运维', '测试用例']
)
def dag_get():

    @task
    def task_get(para):

        print('获取', para)

    task_get(para=get_context)


d1 = dag_info()

d2 = dag_get()
