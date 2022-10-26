import os
from airflow.models import Variable
from datetime import timedelta, date, datetime
import json
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from datetime import timedelta, date, datetime

with open(Variable.get("db_sql_dict")) as j:
    DB_SQL_DICT = json.load(j)  # 依赖的SQL语句

LOAD_PATH = '/home/lianghua/rtt/mountdir/data/load/'
LOAD_PATH_OP = '/home/lianghua/rtt/mountdir/data/load_op/'
LOG_PATH = '/home/lianghua/rtt/mountdir/data/log/'


@task
def check_date(select_table, load_date):
    """
    检查- 输出日志
    """
    import os
    import pandas as pd
    import datacompy
    # --------------找信息--------------#
    # 找到PK
    df_info = DB_SQL_DICT[select_table]
    pk_column = df_info['primary_key']

    path_load = LOAD_PATH+f'{select_table}/{load_date}.parquet'
    path_op = LOAD_PATH_OP+f'{select_table}/{load_date}.parquet'

    if (not os.path.exists(path_load)) or (not os.path.exists(path_op)):
        return

    df_old = pd.read_parquet(LOAD_PATH+f'{select_table}/{load_date}.parquet')
    df_new = pd.read_parquet(
        LOAD_PATH_OP+f'{select_table}/{load_date}.parquet')

    # print(df_new.columns, df_old.columns)
    # print(pk_column, select_table)

    # --------------日志文件路径--------------#
    _ = os.mkdir(LOG_PATH+select_table) if not os.path.exists(
        LOG_PATH+select_table) else None
    LOG_OUTPUT_PATH = LOG_PATH + f'{select_table}/{load_date}'

    # --------------输出日志文件--------------#
    import logging
    from datacompy.core import LOG
    fh = logging.FileHandler(f'{LOG_OUTPUT_PATH}.log', encoding='utf-8')
    fh_formatter = logging.Formatter(
        '%(asctime)s %(module)s-%(lineno)d [%(levelname)s]:%(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    fh.setFormatter(fh_formatter)
    LOG.addHandler(fh)

    # --------------比较--------------#
    compare_log = datacompy.Compare(
        df_old, df_new, df1_name='df_load', df2_name='df_opdate', join_columns=pk_column)

    # --------------输出差异比较--------------#
    with open(f'{LOG_OUTPUT_PATH}.txt', 'w') as f:
        f.write(compare_log.report())


@dag(
    default_args={'owner': 'zirui',
                  # '821538716@qq.com'
                  'email': ['523393445@qq.com', '821538716@qq.com', ],
                  'email_on_failure': True,
                  'email_on_retry': True,
                  #   'retries': 1,
                  "retry_delay": timedelta(minutes=1), },
    schedule="30 3 * * 1-7",
    start_date=pendulum.datetime(2022, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['日期检查'],
)
def csc_check_date():
    import os
    table_list = [table for table in os.listdir(LOAD_PATH_OP)]
    load_date = (date.today() + timedelta(-1)).strftime('%Y%m%d')
    for table in table_list:
        check_date.override(task_id='C_' + table, )(table, load_date)


csc_check_date()
