# @Time      :2022-09-28 15:49:50
# @Author    :Colin
# @Note      :删除并覆盖历史文件
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
# Define datasets


@dag(
    default_args={'owner': 'zirui'},
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=['数据运维', '1期项目'],

)
def csc_ops_load():
    @task(outlets=[Dataset(Variable.get("csc_load_path")+'load.csv')])
    def out():
        print('ok')
    out()


@dag(
    default_args={'owner': 'zirui'},
    schedule=[Dataset(Variable.get("csc_load_path")+'load.csv')],
    tags=['数据运维', '1期项目'],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # outlets=[dag2_dataset]

)
def csc_ops_transform():
    @task(outlets=[Dataset(Variable.get("csc_load_path")+'trans.csv')])
    def out():
        pass
    out()
