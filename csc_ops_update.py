import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.datasets import Dataset
from airflow.models.param import Param


def get_context(context):
    return context['task_instance']


@task
def get_dagrun_conf(dag_run=None,):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dag_run: The DagRun object
    {"date":"20220101"}
    """
    # print(dag_run.conf)
    return dag_run.conf


def print_x(**context):
    print(context["params"])


@dag(
    default_args={'owner': 'zirui', },
    params={"msg": Param("Please Use Upper Table Name", type="string"),
            "start_date": Param(20220301, type="integer", minimum=20211231, maximum=20221231),
            "end_date": Param(20220303, type="integer", minimum=20211231, maximum=20221231),
            "table_name": Param(
                "ASHAREBALANCESHEET",
                type="string",
                minLength=5,
                maxLength=255,
    )},
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据更新', '参数触发']
)
def csc_ops_update():

    @task
    def get_params(params=None) -> dict:
        # print('task获取params参数', params, params['table_name'])
        start_date = params['start_date']
        end_date = params['end_date']
        # date_list = [str(date) for date in range(start_date, end_date)]
        return {'table_name': params['table_name'], 'start_date': start_date, 'end_date': end_date, }

    @task
    def check(param):
        print(param)
    # 任务流
    res_value = get_params()
    table_name = res_value['table_name']
    start_date = res_value['start_date']
    end_date = res_value['end_date']
    print(end_date)
    # date_list = [str(date) for date in range(start_date, end_date)]
    # print(date_list)
    # date_list = [i for i in res_value['date_list']]
    # res = get_dagrun_conf()

    # print(f"the value is {xcomarg}")
    # check(date_list)
    # print(date_list)
    # print(get_dagrun_conf())

    # import sys
    # sys.path.append('/home/lianghua/rtt/soft/airflow/dags/zirui_dag')
    # from csc_ops_load import extract_sql_by_table, load_sql_query
    # load_sql_query.expand(data_dict=extract_sql_by_table.partial(table_name=table_name).expand(
    #     load_date=['2020202'])
    # )


d1 = csc_ops_update()
