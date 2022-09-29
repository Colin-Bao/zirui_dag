# @Time      :2022-09-28 15:49:50
# @Author    :Colin
# @Note      :新的合并算法
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import dag, task
from airflow.models import Variable


# Define datasets

#

# TODO 已经内置了邮件处理函数
# TODO 动态映射,自定义task,自定义dag
@dag(
    default_args={'owner': 'zirui'},
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=['数据运维', ],

)
def csc_ops_load():
    @task
    def out():
        print('ok')

    out()

# TODO 生成多个DAG用于处理
