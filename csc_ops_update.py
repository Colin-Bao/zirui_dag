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
def csc_ops_update():
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

    # 任务流

    import sys
    sys.path.append('/home/lianghua/rtt/soft/airflow/dags/zirui_dag')
    from csc_ops_load import extract_sql_by_table, load_sql_query
    load_sql_query.expand(data_dict=extract_sql_by_table.expand(table_name=get_table_list(),
                                                                load_date=get_data_list())
                          )


d1 = csc_ops_update()
