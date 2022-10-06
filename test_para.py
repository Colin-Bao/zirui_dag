import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.datasets import Dataset
from airflow.models.param import Param


@dag(
    default_args={'owner': 'zirui', },
    params={"msg": Param("Please Use Upper Table Name", type="string"),
            "start_date": Param(20220301, type="integer", minimum=20211231, maximum=20221231),
            "end_date": Param(20220304, type="integer", minimum=20211231, maximum=20221231),
            "table_name": Param(
                "ASHAREBALANCESHEET",
                type="string",
                minLength=5,
                maxLength=255,
            ),
            "table_list": Param(
                ["ASHAREBALANCESHEET", "ASHAREBALANCESHEETB"],
                type="array",
                items={"type": "string"},
                minLength=1,
                maxLength=10,
            ),

            },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据更新', '参数触发']
)
def test_para():
    @task
    def get_data_list(params=None):
        date_list = [str(i) for i in range(params['start_date'], params['end_date'])]
        return date_list

    @task
    def get_table_name(params=None):
        return params['table_name']

    @task
    def get_table_list(params=None):
        return params['table_list']

    @task
    def do_task(table_name, op_date):
        import pandas as pd
        pd.DataFrame().to_csv(f"{table_name}_{op_date}.csv")

    # date_list = get_data_list()
    do_task.expand(table_name=get_table_list(), op_date=get_data_list())
    #
    from airflow.models.xcom_arg import XComArg
    # a = get_params()
    # res = pull_function()
    # print(str(a))
    # la = [i for i in a]

    # print(get_context())


test_para()
