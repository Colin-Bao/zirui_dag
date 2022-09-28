import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG, Dataset
from airflow.models import Variable
from datetime import datetime, timedelta

OUTPUT_PATH = Variable.get("csc_load_path")


def get_sql_by_table(table_name: str, load_date: str) -> tuple:
    """
        根据表名和日期返回sql查询语句
        :return:( connector_id, table_name, sql, )
    """
    AF_CONN = '_af_connector'  # 数据库连接器名称
    import json
    # 查找属于何种数据源
    with open('sql_files/all_table_db' + '.json') as f:  # 去数据字典文件中寻找
        db = json.load(f)[table_name]

    # 不同数据源操作
    if db == 'wind':
        from sql_files.wind_sql import sql_sentence
        wind_sql_dict = {k.upper(): v.replace('\n      ', '').replace(
            '\n', '') for k, v in sql_sentence.items()}  # 转成大写
        return_sql = wind_sql_dict[table_name] % f"\'{load_date}\'"

    elif db == 'suntime':
        # TODO 没有写增量表，需要增加逻辑判断
        import json
        with open('sql_files/suntime_sql_merge' + '.json') as f:
            suntime_sql = json.load(f)[table_name]['sql']  # 去数据字典文件中寻找

        return_sql = suntime_sql % (
            'zyyx.'+table_name, f"{load_date}") if suntime_sql else None

    else:
        raise Exception
    return (db + AF_CONN, return_sql)


def load_sql_query(table_name: str, load_date: str):
    # ----------------- 生成查询语句----------------- #
    connector_id, query_sql = get_sql_by_table(table_name, load_date)

    # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------df执行sql查询,保存文件----------------- #
    if not os.path.exists(OUTPUT_PATH + table_name):
        os.mkdir(OUTPUT_PATH + table_name)
    # import pandas as pd  # 文档说不要写在外面，影响性能
    chunk_count = 0
    # 防止服务器内存占用过大
    for df_chunk in sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=1000):
        if chunk_count == 0:
            path = OUTPUT_PATH + table_name + f'/{load_date}.csv'
            df_chunk.to_csv(path, index=False)
            break
        else:
            # TODO 只保存chunksize行，如果超过chunksize行要分片保存再合并
            break
        chunk_count += 1


table_list = [
    'FIN_BALANCE_SHEET_GEN', 'ASHAREBALANCESHEET', 'ASHARECASHFLOW', 'FIN_CASH_FLOW_GEN',
    'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
    'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
    'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION']

for i in table_list:
    load_sql_query(i, '20220101')
