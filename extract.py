import os
import time
from tkinter import N
from airflow.models import Variable
from datetime import timedelta, date, datetime
from retry import retry
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq

LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load_new/'


def get_config_from_df(df_chunk: pd.DataFrame, real_table: str,
                       select_table: str):
    """
    跑一遍,获取所有的信息
    """
    import pyarrow as pa
    config_dict = {}
    db_from_df = pd.read_csv(
        f'/home/lianghua/ZIRUI/rely_files/info_from_db/{real_table}.csv',
        usecols=['COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH'],
        index_col='COLUMN_NAME').to_dict()
    dtype_from_df = df_chunk.dtypes.to_dict()
    schema_from_df = pa.Schema.from_pandas(df_chunk)

    # -----------------遍历判断类型---------------- #
    for k, v in dtype_from_df.items():

        # 取出信息
        database_type = db_from_df['DATA_TYPE'][k]
        pandas_type = str(v)
        parquet_type = str(schema_from_df.field_by_name(k).type)

        # -----------------判断类型：1.数据库---------------- #
        # TODO 结合长度判断类型？
        if database_type in ['varchar', 'text', 'VARCHAR2', 'CLOB']:  # 字符串类型
            pandas_type = 'str'
            parquet_type = 'string'
        elif database_type in ['numeric', 'NUMBER', 'FLOAT']:  # TODO 根据长度去定义一下
            # database_len = db_from_df['DATA_LENGTH'][k]
            pandas_type = 'float64'
            parquet_type = 'double'
        elif database_type in ['datetime', 'DATE']:
            # database_len = db_from_df['DATA_LENGTH'][k]
            pandas_type = 'int'
            parquet_type = 'int64'
            # pandas_type = 'str'
            # parquet_type = 'string'
        else:
            raise Exception(
                f'定义类型{database_type}, {pandas_type}, {parquet_type}')

        # -----------------判断类型：2.日期---------------- #
        if ('DATE' in k) | ('date' in k) | (k in ['TRADE_DT', 'ANN_DT']):
            pandas_type = 'int'
            parquet_type = 'int64'

        # -----------------更新config---------------- #
        config_dict.update({
            k: {
                'database_type': database_type,
                'pandas_type': pandas_type,
                'parquet_type': parquet_type
            }
        })
    # -----------------输出config---------------- #
    with open(
            f"/home/lianghua/ZIRUI/rely_files/type_df_and_parquet/{select_table}.json",
            'w') as f:
        json.dump(config_dict, f)


def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句
    :return:( connector_id, return_sql, table_name, load_date)
    """
    # 去数据字典文件中找信息
    with open(Variable.get("db_sql_dict")) as j:
        table_info = json.load(j)[table_name]

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
    all_columns = xcom_dict['all_columns']

    # -----------------数据库接口---------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # # -----------------先跑一遍----------------- #
    # for df_chunk in sql_hook.get_pandas_df_by_chunks(query_sql,
    #                                                  chunksize=1000):
    #     # 转为str：orcale有问题
    #     if connector_id == 'suntime_af_connector':
    #         str_column = df_chunk.select_dtypes(include='object').columns
    #         df_chunk[str_column] = df_chunk[str_column].astype('str')

    #     get_config_from_df(df_chunk, real_table, select_table)

    #     return None
    # return None
    # -----------------去字典文件中找config信息----------------- #
    with open(
            f'/home/lianghua/ZIRUI/rely_files/type_df_and_parquet/{select_table}.json'
    ) as j:
        table_info = json.load(j)
    all_columns = table_info
    dtype_config = {k: v['pandas_type'] for k, v in table_info.items()}

    # ----------------------------------大表分片保存---------------------------------- #
    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks_zirui(query_sql,
                                                           chunksize=500000,
                                                           dtype=dtype_config):
        # 无数据跳出
        if df_chunk.empty:
            break
        # ----------------- 命名----------------- #
        if dynamic:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{real_table}/{load_date}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{real_table}/{load_date}_{chunk_count}.parquet'
        else:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{real_table}/{real_table}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{real_table}/{real_table}_{chunk_count}.parquet'

        # ----------------- 1.读取schema----------------- #
        schema_list = [(column_name, types['parquet_type'])
                       for column_name, types in all_columns.items()
                       if column_name in df_chunk.columns.to_list()]

        # -----------------2.修改schema 输出文件----------------- #
        import pyarrow as pa
        import os
        # print(df_chunk)
        pa_table = pa.Table.from_pandas(df_chunk,
                                        schema=pa.schema(schema_list))

        _ = os.mkdir(LOAD_PATH_ROOT +
                     real_table) if not os.path.exists(LOAD_PATH_ROOT +
                                                       real_table) else None
        pq.write_table(pa_table, LOAD_PATH)
        chunk_count += 1


# @retry(exceptions=Exception, tries=10, delay=1)
def download_by_table(table_name):

    date_list = [
        str(i).replace('-', '').split(' ')[0]
        for i in pd.date_range('20220101', '20221015')
    ]
    # 按日期下载
    for load_date in date_list:
        LOAD_PATH = LOAD_PATH_ROOT + table_name + f'/{load_date}.parquet'
        # 判断是否存在
        if os.path.exists(LOAD_PATH):
            # pass
            continue
        extract = extract_sql_by_table(table_name, load_date)
        # 开始下载
        if not extract['dynamic']:
            break

        load_date = load_sql_query(extract)

    return f'task {table_name} ok'


def start_task_one():
    with open(Variable.get("csc_input_table")) as j:
        table_list = json.load(j)['need_tables']
    len_t = len(table_list)
    # table_list = ['ASHARETRADINGSUSPENSION']
    for i, table in enumerate(table_list):
        # print(i, table)
        # 判断是否存在
        # if os.path.exists(LOAD_PATH_ROOT + table):
        #     # pass
        #     continue

        # download_by_table(table)

        print('------------------------------')
        print(f'{table} {i}/{len_t}')
        download_by_table(table)


def start_task_mul():
    with open(Variable.get("csc_input_table")) as j:
        table_list = json.load(j)['need_tables']
    with ThreadPoolExecutor(max_workers=10) as executor:
        result = {
            executor.submit(download_by_table, table): table
            for table in table_list
        }
        for future in as_completed(result):
            try:
                print(future.result())
            except Exception as e:
                print(e)


st = time.time()

start_task_one()
# check_dataset_byx_table('ASHARECONSENSUSROLLINGDATA')

print(time.time() - st)
