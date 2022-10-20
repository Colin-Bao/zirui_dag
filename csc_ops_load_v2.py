import os
import time
from airflow.models import Variable
from datetime import timedelta, date, datetime
from retry import retry
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq
import numpy as np

import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import timedelta, date, datetime

# -----------------加载运行依赖的配置信息----------------- #
# 去数据字典文件中找信息
with open(Variable.get("db_sql_dict")) as j:
    DB_SQL_DICT = json.load(j)

CONFIG_PATH = '/home/lianghua/ZIRUI/rely_files/test_type_df_and_parquet'  # 转换依赖的CONFIG
LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load_test/'  # 输出路径


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


def trans_before_astype(df_chunk, dtype_config: dict):
    """
    转换dtypes前的步骤
    """
    # -----------------转换dtype前要加的步骤---------------- #
    # 非字符串处理
    float_columns = [k for k, v in dtype_config.items() if v == 'float64']
    df_chunk[float_columns] = df_chunk[float_columns].replace(
        [None, 'None'], np.float64(0))
    # 日期处理
    int_columns = [k for k, v in dtype_config.items() if v == 'int64']
    df_chunk[int_columns] = df_chunk[int_columns].replace([None, 'None'],
                                                          np.int64(0))
    # 日期处理，有的传回来的值是一个Timestamp对象
    for i in int_columns:
        df_chunk[i] = df_chunk[i].apply(lambda x: x.timestamp()
                                        if type(x) == pd.Timestamp else x)

    return df_chunk


def trans_before_hdf(df_chunk):
    """
    转换hdf前的步骤,按照鹏队的需求把str的None转为''
    """
    obj_columns = list(df_chunk.select_dtypes(
        include=['object', 'str']).columns.values)
    df_chunk[obj_columns] = df_chunk[obj_columns].replace([None, 'None'],
                                                          '')
    return df_chunk


def trans_before_schema(df_chunk):
    """
    转换schema前的步骤
    """
    return df_chunk


def load_sql_query(xcom_dict: dict) -> dict:
    """
    根据sql查询语句下载数据到本地
    :return:xcom_dict
    """
    # -----------------参数传递----------------- #
    connector_id = xcom_dict['connector_id']
    query_sql = xcom_dict['query_sql']
    real_table = xcom_dict['real_table']
    select_table = xcom_dict['select_table']
    load_date = xcom_dict['load_date']
    dynamic = xcom_dict['dynamic']
    date_column = xcom_dict['date_column']

    # -----------------数据库接口---------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------去字典文件中找config信息----------------- #
    with open(f'{CONFIG_PATH}{select_table}.json') as j:
        config_dict = json.load(j)
    dtype_config = {k: v['pandas_type'] for k, v in config_dict.items()}

    # ----------------------------------大表分片保存---------------------------------- #
    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks(
            query_sql,
            chunksize=1000000,
    ):
        # 无数据跳出
        if df_chunk.empty:
            break
        # -----------------转换dtype前要加的步骤---------------- #
        df_chunk = trans_before_astype(df_chunk, dtype_config)

        # ----------------- 转换dtype----------------- #

        df_chunk = df_chunk.astype(dtype=dtype_config)

        # ----------------- 转换dtype后要加的步骤----------------- #
        df_chunk = trans_before_hdf(df_chunk)

        # ----------------- 命名----------------- #
        if dynamic:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{real_table}/{load_date}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{real_table}/{load_date}_{chunk_count}.parquet'
        else:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{real_table}/{real_table}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{real_table}/{real_table}_{chunk_count}.parquet'

        # -----------------2.修改schema 输出文件----------------- #
        import pyarrow as pa
        import os
        schema_list = [(column_name, types['parquet_type'])
                       for column_name, types in config_dict.items()
                       if column_name in df_chunk.columns.to_list()]
        pa_table = pa.Table.from_pandas(df_chunk,
                                        schema=pa.schema(schema_list))

        _ = os.mkdir(LOAD_PATH_ROOT +
                     real_table) if not os.path.exists(LOAD_PATH_ROOT +
                                                       real_table) else None
        pq.write_table(pa_table, LOAD_PATH)
        chunk_count += 1
