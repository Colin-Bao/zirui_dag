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
# -----------------运行参数----------------- #
LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load_test/'
CONFIG_PATH = '/home/lianghua/ZIRUI/rely_files/test_type_df_and_parquet/'
MODE_LIST = ['GET_CONFIG', 'DOWN_BY_ONE', "GET_CONFIG_AND_DOWN_BY_ONE"]
MODE_NUM = 2
DATE_LIST = [
    str(i).replace('-', '').split(' ')[0]
    for i in pd.date_range('20100101', '20221015')
]
os.environ['NUMEXPR_MAX_THREADS'] = '12'

with open(Variable.get("csc_input_table")) as j:
    TABLE_LIST = json.load(j)['need_tables']
# TABLE_LIST = ['ASHAREDIVIDEND']
DATE_NAME = []
# -----------------运行参数----------------- #


def get_config_from_df(
    connector_id,
    sql_hook,
    query_sql: str,
    real_table: str,
    select_table: str,
):
    """
    跑一遍,获取所有的信息
    """
    # -----------------得到df----------------- #
    df_chunk = pd.DataFrame()
    for chunk in sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=100):
        # 转为str：orcale有问题
        if connector_id == 'suntime_af_connector':
            str_column = chunk.select_dtypes(include='object').columns
            chunk[str_column] = chunk[str_column].astype('str')
        if chunk.empty:
            return None
        else:
            df_chunk = chunk
            break

    # -----------------三种config---------------- #
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
            pandas_type = 'int64'
            parquet_type = 'int64'
            # pandas_type = 'str'
            # parquet_type = 'string'
        else:
            raise Exception(
                f'定义类型{database_type}, {pandas_type}, {parquet_type}')

        # -----------------判断类型：2.日期---------------- # ('DATE' in k) | ('date' in k) |
        if (k in ['TRADE_DT', 'ANN_DT', 'REPORT_PERIOD'
                  ]) | ('_DT' in k) | ('date' in k) | ('DATE' in k):
            DATE_NAME.append(k)
            pandas_type = 'int64'
            parquet_type = 'int64'
        # print(k[-4:])

        # -----------------更新config---------------- #

        config_dict.update({
            k: {
                'database_type': database_type,
                'pandas_type': pandas_type,
                'parquet_type': parquet_type
            }
        })

    # -----------------测试config---------------- #
    dtype_config = {k: v['pandas_type'] for k, v in config_dict.items()}
    schema_list = [(column_name, types['parquet_type'])
                   for column_name, types in config_dict.items()
                   if column_name in df_chunk.columns.to_list()]

    # -----------------转换dtype前要加的步骤---------------- #

    # 非字符串处理
    float_columns = [k for k, v in dtype_config.items() if v == 'float64']
    df_chunk[float_columns] = df_chunk[float_columns].replace([None, 'None'],
                                                              np.float64(0))
    # 日期处理
    int_columns = [k for k, v in dtype_config.items() if v == 'int64']
    df_chunk[int_columns] = df_chunk[int_columns].replace([None, 'None'],
                                                          np.int64(0))
    # 日期处理
    for i in int_columns:
        df_chunk[i] = df_chunk[i].apply(lambda x: x.timestamp()
                                        if type(x) == pd.Timestamp else x)

    # -----------------排查出错的字段---------------- #
    try:
        df_chunk = df_chunk.astype(dtype=dtype_config)
    except Exception as e:

        print(f'# -----------------{select_table}---------------- #')
        # 逐字段排查
        for i in df_chunk.columns.to_list():
            print(
                df_chunk[i].to_list(),
                '\n-----------------\n',
                dtype_config[i],
            )
            df_chunk[i] = df_chunk[i].astype(dtype_config[i])

    # -----------------转换dtype后要加的步骤---------------- #
    # 把str中的空值替换
    obj_columns = list(
        df_chunk.select_dtypes(include=['object']).columns.values)
    df_chunk[obj_columns] = df_chunk[obj_columns].replace([None, 'None'], '')

    # 测试pa
    pa_table = pa.Table.from_pandas(df_chunk, schema=pa.schema(schema_list))
    # print(f'{select_table} success')

    # -----------------输出config---------------- #
    with open(f"{CONFIG_PATH}{select_table}.json", 'w') as f:
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

    # -----------------数据库接口---------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------获取config信息----------------- #
    if MODE_LIST[MODE_NUM] == 'GET_CONFIG':
        get_config_from_df(connector_id, sql_hook, query_sql, real_table,
                           select_table)
        return MODE_LIST[MODE_NUM]

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

        # 非字符串处理
        float_columns = [k for k, v in dtype_config.items() if v == 'float64']
        df_chunk[float_columns] = df_chunk[float_columns].replace(
            [None, 'None'], np.float64(0))
        # 日期处理
        int_columns = [k for k, v in dtype_config.items() if v == 'int64']
        df_chunk[int_columns] = df_chunk[int_columns].replace([None, 'None'],
                                                              np.int64(0))
        # 日期处理
        for i in int_columns:
            df_chunk[i] = df_chunk[i].apply(lambda x: x.timestamp()
                                            if type(x) == pd.Timestamp else x)

        # ----------------- 转换dtype----------------- #
        df_chunk = df_chunk.astype(dtype=dtype_config)

        # ----------------- 转换dtype后要加的步骤----------------- #
        obj_columns = list(
            df_chunk.select_dtypes(include=['object']).columns.values)
        df_chunk[obj_columns] = df_chunk[obj_columns].replace([None, 'None'],
                                                              '')
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


@retry(exceptions=Exception, tries=10, delay=1)
def download_by_table(table_name):
    """
    按照表名下载 
    """
    # 按日期下载
    for load_date in DATE_LIST:
        LOAD_PATH = LOAD_PATH_ROOT + table_name + f'/{load_date}.parquet'
        if os.path.exists(LOAD_PATH):  # 判断是否存在
            continue
        extract = extract_sql_by_table(table_name, load_date)
        if not extract['dynamic']:
            break

        load_date = load_sql_query(extract)  # 开始下载

    return f'task {table_name} ok'


def start_task_one():
    """
    单线程用于测试
    """
    len_t = len(TABLE_LIST)
    for i, table in enumerate(TABLE_LIST):
        print('------------------------------')
        print(f'{table} {i}/{len_t}')
        download_by_table(table)


def start_task_mul():

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


st = time.time()

if MODE_NUM <= 1:
    start_task_one()
elif MODE_NUM == 2:
    start_task_mul()
print(time.time() - st)
# print(DATE_NAME)
# print(list(set(DATE_NAME)).sort())
