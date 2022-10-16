import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.models.param import Param


@dag(
    default_args={'owner': 'zirui', },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据更新', '参数触发']
)
def csc_ops_check_update():
    # 任务流
    import sys
    import json
    import pandas as pd
    sys.path.append(Variable.get('csc_zirui_dag'))  # 导入包
    from csc_ops_load import extract_sql_by_table, load_sql_query, get_config
    # 获取更新表
    with open(Variable.get("csc_input_table")) as j:
        table_list = json.load(j)['need_tables']
    # 获取更新日期
    date_list = [
        str(i).replace('-', '').split(' ')[0]
        for i in pd.date_range('20220101', '20220110')
    ]

    load_sql_query.expand(xcom_dict=extract_sql_by_table.expand(table_name=table_list,
                                                                load_date=date_list)
                          )


csc_ops_check_update()
