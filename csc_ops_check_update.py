import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.models.param import Param


@dag(
    default_args={'owner': 'zirui',
                  'email': ['523393445@qq.com', ],  # '821538716@qq.com'
                  'email_on_failure': True,
                  'email_on_retry': True,
                  #   'retries': 1,
                  "retry_delay": timedelta(minutes=1), },
    schedule="0/60 1-6 * * 1-7",
    start_date=pendulum.datetime(2022, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['数据更新']
)
def csc_ops_check_update():
    # 任务流
    import sys
    import json
    sys.path.append(Variable.get('csc_zirui_dag'))  # 导入包
    from csc_ops_load import extract_sql_by_table, load_sql_query
    # 获取更新表
    with open(Variable.get("csc_input_table")) as j:
        table_list = json.load(j)['need_tables']
    # 获取更新日期
    # table_list = ['DER_FOCUS_STK','DER_PROB_EXCESS_STOCK', 'AINDEXFREEWEIGHT']
    date_list = [(date.today() + timedelta(-i-1)).strftime('%Y%m%d')
                 for i in list(range(3))]

    # 遍历

    for table in table_list:
        # 获取更新表
        # path = f'/home/lianghua/rtt/mountdir/data/load/{table}/config.json'
        # congfig_path = path if os.path.exists(path) else ''
        # if congfig_path != "":
        #     with open(congfig_path) as j:
        #         dynamic = json.load(j)['dynamic']
        # else:
        #     dynamic = False
        with open(f'/home/lianghua/rtt/mountdir/data/load_new/{table}/config.json') as j:
            dynamic = json.load(j)['dynamic']
        # 开始任务
        if not dynamic:
            load_sql_query.override(task_id='L_' + table, )(extract_sql_by_table.override(task_id='E_' + table, )(
                table_name=table, load_date=''))
        else:
            load_sql_query.override(task_id='L_' + table, ).expand(xcom_dict=extract_sql_by_table.override(task_id='E_' + table, ).partial(table_name=table).expand(
                load_date=date_list)
            )


csc_ops_check_update()
