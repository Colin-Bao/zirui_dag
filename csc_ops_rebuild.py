# @Time      :2022-09-28 15:49:50
# @Author    :Colin
# @Note      :删除并覆盖历史文件
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import dag, task
# Define datasets
dag1_dataset = Dataset('load_data')
dag2_dataset = Dataset('transform_data')


@dag(
    default_args={'owner': 'zirui'},
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=['数据运维', '1期项目'],

)
def csc_ops_load():
    @task(outlets=[dag1_dataset])
    def out():
        print('ok')
    out()


@dag(
    default_args={'owner': 'zirui'},
    schedule=[dag1_dataset],
    tags=['数据运维', '1期项目'],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # outlets=[dag2_dataset]

)
def csc_ops_transform():
    @task(outlets=[dag2_dataset])
    def out():
        pass
    out()


dag1 = csc_ops_load()
dag2 = csc_ops_transform()
