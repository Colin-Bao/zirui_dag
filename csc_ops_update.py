import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.datasets import Dataset
from airflow.models.param import Param


@task
def get_dagrun_conf(dag_run=None,):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dag_run: The DagRun object
    {"date":"20220101"}
    """
    # print({{dag_run.conf}})
    return dag_run.conf


@dag(
    default_args={'owner': 'zirui', },
    params={"msg": Param("Please Use Upper Table Name", type="string"),
            "start_date": Param(20220301, type="integer", minimum=20000101, maximum=20221231),
            "end_date": Param(20220301, type="integer", minimum=20000101, maximum=20221231),
            "table_name": Param(
                "ASHAREBALANCESHEET",
                type="string",
                minLength=5,
                maxLength=255,
    )},
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据更新', '手动触发']
)
def csc_ops_update():
    import sys
    sys.path.append('/home/lianghua/rtt/soft/airflow/dags/zirui_dag')
    from csc_ops_load import extract_sql_by_table

    @task
    def get_params(params=None):
        print(f"params参数{{params}}")
        # print('task获取params参数', params)
        return params

    # 任务流
    get_params()
    print(f"{{params}}")
    # params_dict = get_params()
    # print(params_dict['table_name'])
    # table_name = params_dict['table_name']
    # # load_date = [str(date) for date in range(
    # #     params_dict['start_date'], params_dict['start_date'])]
    # extract_sql_by_table.override(
    #     task_id='U_'+table_name, outlets=[Dataset('U_'+table_name)]).partial(table_name=table_name).expand(load_date=[1, 2, 3])


d1 = csc_ops_update()
