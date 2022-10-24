from airflow.models import Variable
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, date, datetime
from retry import retry
import os
import pandas as pd
# -----------------加载运行依赖的配置信息----------------- #
with open(Variable.get("csc_input_table")) as j:
    TABLE_LIST = json.load(j)['need_tables']
with open(Variable.get("db_sql_dict")) as j:
    DB_SQL_DICT = json.load(j)  # 依赖的SQL语句
TABLE_LIST = [
    'CON_FORECAST_ROLL_STK'
]
CONFIG_PATH = '/home/lianghua/ZIRUI/rely_files/test_type_df_and_parquet/'  # 转换依赖的CONFIG
LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load/'  # 输出路径
DATE_LIST = [
    str(i).replace('-', '').split(' ')[0]
    for i in pd.date_range('20220104', '20220105')
]
MODE = 0  # 0空值保存


def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句
    :return:( connector_id, return_sql, table_name, load_date)
    """
    # 去数据字典文件中找信息
    table_info = DB_SQL_DICT[table_name]

    # 处理增量表
    query_sql = table_info['sql'] % (
        load_date) if table_info['dynamic'] else table_info['sql']

    if table_info['data_base'] == 'wind':
        query_sql = query_sql.replace(
            table_info['date_where']+' =', ' convert(varchar(100), OPDATE, 112) = ')
    else:
        query_sql = query_sql.replace(
            table_info['date_where']+' =', ' ENTRYTIME = ')
    print(query_sql)
    return {
        'connector_id': table_info['data_base'] + '_af_connector',
        'query_sql': query_sql,
        'real_table': table_info['oraignal_table'],
        'select_table': table_name,
        'load_date': load_date,
        'dynamic': table_info['dynamic'],
        'date_column': table_info['date_where'],
        'all_columns': table_info['all_columns']
    }


def load_sql_query(xcom_dict: dict) -> dict:
    """
    根据sql查询语句下载数据到本地
    :return:xcom_dict
    """
    # -----------------参数传递----------------- #
    connector_id = xcom_dict['connector_id']
    query_sql = xcom_dict['query_sql']
    select_table = xcom_dict['select_table']
    load_date = xcom_dict['load_date']
    dynamic = xcom_dict['dynamic']
    date_column = xcom_dict['date_column']

    # -----------------数据库接口---------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # ----------------------------------大表分片保存---------------------------------- #
    # get_pandas_df   连接suntime会报错
    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks(
            query_sql,
            chunksize=1000000,
    ):
        # 无数据跳出
        if df_chunk.empty:
            LOAD_PATH = ''
            if MODE == 0:
                pass
            else:
                break
        print(df_chunk)


@retry(exceptions=Exception, tries=10, delay=1)
def download_by_table(table_name):
    """
    按照表名下载 
    """
    # 按日期下载
    for load_date in DATE_LIST:
        # LOAD_PATH = LOAD_PATH_ROOT + table_name + f'/{load_date}.parquet'
        # if os.path.exists(LOAD_PATH):  # 判断是否存在
        #     continue
        extract = extract_sql_by_table(table_name, load_date)
        # if not extract['dynamic']:
        #     # pass
        #     break

        load_date = load_sql_query(extract)  # 开始下载

    return f'task {table_name} ok'


def start_task_mul():
    """
    多进程用于下载
    """

    with ThreadPoolExecutor(max_workers=10) as executor:
        result = {
            executor.submit(download_by_table, table): table
            for table in TABLE_LIST
        }
        for future in as_completed(result):
            try:
                print(future.result())
            except Exception as e:
                print(e)


def start_task_one():
    """
    单线程用于测试
    """
    len_t = len(TABLE_LIST)
    for i, table in enumerate(TABLE_LIST):
        print('------------------------------')
        print(f'{table} {i}/{len_t}')
        download_by_table(table)


start_task_one()
