#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/24 09:02
# @Author    :Colin
# @Note      :None
# -----------------airflow数据库接口----------------- #
from airflow.providers.common.sql.hooks.sql import BaseHook  # 通用数据库接口
from zirui_dag.map_table import *
import pandas as pd
AF_CONN = '_af_connector'


def extract_sql(connector_id, table_name, column, date_column, start_date, end_date) -> pd.DataFrame:
    sql_hook = BaseHook.get_connection(connector_id).get_hook()
    query_sql = f"""SELECT {column} FROM {table_name} WHERE {date_column} = '{start_date}'  """
    # print(query_sql)
    df_extract = sql_hook.get_pandas_df(query_sql)  # 从Airflow的接口返回df格式
    return df_extract


def select_sql(connector_id, query_sql):
    sql_hook = BaseHook.get_connection(connector_id).get_hook()
    df_extract = sql_hook.get_pandas_df(query_sql)  # 从Airflow的接口返回df格式
    return df_extract


def load(table_name):
    app = MapCsc(table_name)

    for i in app.get_map_tables():
        # print(i[0], i[1], i[2], i[3], 20220101, 20220101)
        table_name_new = str(i[1]).replace(
            '[',  '').replace(']',  '').split('.')[-1]
        if i[0] == 'wind_af_connector':
            res = extract_sql(i[0], i[1], i[2], i[3], '20210101', '20210201')
            res.to_csv(
                f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/{table_name_new}.csv', index=False)

        elif i[0] == 'suntime_af_connector':
            res = extract_sql(
                i[0], i[1], i[2], i[3], "to_date('20210101','yyyy-MM-dd')", "to_date('20210201','yyyy-MM-dd')")
            res.to_csv(
                f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/{table_name_new}.csv', index=False)

            # print(res)

# connector_id, table_name, column, date_column, start_date, end_date)


# 检查sql语法 2022年09月27日
# 已经测试通过，遇到的问题主要是，orcle中的lob字段关闭连接后无法转换，用一边取一边存解决
def check_tables():
    app = MapCsc()
    wind_tables = [(j, i['wind'][j]['target_column'], i['wind'][j]['date_column'])
                   for i in app.MAP_DICT.values() for j in i['wind'].keys()]
    zyyx_tables = [(j, i['suntime'][j]['target_column'], i['suntime'][j]['date_column'])
                   for i in app.MAP_DICT.values() for j in i['suntime'].keys()]

    wind_sql = []
    for i in wind_tables:
        break

        table_name = f'[wande].[dbo].[{i[0]}]'
        target_column = ','.join([f'[{i}]' for i in i[1]])
        date_column = f'[{i[2]}]'
        query_sql = f"""SELECT {target_column} FROM {table_name} WHERE {date_column} = '20210301'  """
        wind_sql += [query_sql]
        sql_hook = BaseHook.get_connection('wind_test_af_connector').get_hook()
        df_extract = sql_hook.get_pandas_df_by_chunks(
            query_sql, chunksize=10000)
        # print(table_name, '\n', df_extract)

    suntime_sql = []
    for i in zyyx_tables:

        table_name = f'ZYYX.{i[0]}'
        if table_name != 'ZYYX.FIN_PERFORMANCE_FORECAST':
            continue
        target_column = ','.join([i for i in i[1]])
        date_column = i[2]
        date = '20210301'
        query_sql = f"""SELECT {target_column} FROM {table_name} WHERE {date_column} = to_date('{date}','yyyy-MM-dd') """
        suntime_sql += [query_sql]

        # ZYYX.FIN_PERFORMANCE_FORECAST
        sql_hook = BaseHook.get_connection(
            'suntime_af_connector').get_hook()
        print('checkpoint', table_name, query_sql)
        # from airflow.providers.oracle.hooks.oracle import OracleHook
        # a = OracleHook.get_pandas_df_by_chunks('s')
        # chunk_list = list()
        # res = sql_hook.get_records(query_sql)

        # chunk_list = pd.DataFrame()

        folder_path = '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/'
        sql_chunk = sql_hook.get_pandas_df_by_chunks(
            query_sql, chunksize=10000)
        print(len(sql_chunk))
        # for i, chunk in enumerate(sql_chunk):
        # chunk.to_csv(folder_path + f'{date}_{i}.csv')

        # df_i.to_csv(
        # '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/FIN_PERFORMANCE_FORECAST.csv')
        # chunk_list = chunk_list.append(df_i)
        # chunk_list = pd.concat(
        # [chunk_list, df_i], axis=0, ignore_index=False)
        # df_list = pd.concat(chunk_list, axis=0, ignore_index=False)

        # print(pd.DataFrame(res))
        # break

        # print(i)
    _ = [print(i, '\n') for i in wind_sql]
    _ = [print(i, '\n') for i in suntime_sql]
    # print(wind_sql, suntime_sql)


def csv_test():
    df = pd.read_csv(
        '//home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/ASHAREBALANCESHEET.csv',)
    df.reset_index(inplace=True)

    df.set_index(['WIND_CODE', 'STATEMENT_TYPE', 'ANN_DT'], inplace=True)

    # print(df.index)
    new_index = []
    for i in list(df.index):
        new_index += [(i[0].split('.')[0], i[1], i[2])]
        # print(df.index[i])
        # df.index[i] = (1, 1, 1)

        # print(list(df.index))
    df.index = pd.MultiIndex.from_tuples(new_index, names=df.index.names)
    print(df.index)


def unit_merge(table_name):
    load(table_name)
    MAP = MapCsc(table_name)
    for i in MAP.get_map_tables():
        db_name = i[0].split('_')[0]
        table_name_new = str(i[1]).replace(
            '[',  '').replace(']',  '').split('.')[-1]
        last_name = db_name + '_' + table_name_new  # 数据源+表名.split('.')[-1]

        # 更新数据
        MAP.update_multi_data(db_name, table_name_new, '')
    MAP.merge_multi_data_v2()


def test_failed():
    import os
    OUTPUT_PATH = '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/'

    def extract_sql(connector_id, table_name, column, date_column, start_date):
        from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
        sql_hook = BaseHook.get_connection(connector_id).get_hook()
        if connector_id == 'wind_af_connector':
            query_sql = f"""SELECT {column} FROM {table_name} WHERE [{date_column}] ='{start_date}' """

        elif connector_id == 'suntime_af_connector':
            query_sql = f"""SELECT {column} FROM {table_name} WHERE {date_column} = to_date({start_date},'yyyy-MM-dd')  """
        else:
            raise Exception('未注册的连接名', 200)
        table_filename = table_name.replace(
            '[', '').replace(']', '').split('.')[-1]
        if not os.path.exists(OUTPUT_PATH + table_filename):
            os.mkdir(OUTPUT_PATH + table_filename)

        # 防止服务器内存占用过大

        chunk_count = 0
        print(query_sql)
        chunks = sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=10000)
        for df_chunk in chunks:
            if chunk_count == 0:
                df_chunk.to_csv(OUTPUT_PATH + table_filename +
                                f'/{start_date}.csv', index=False)
            else:
                df_chunk.to_csv(OUTPUT_PATH + table_filename +
                                f'/{start_date}_{chunk_count}.csv', index=False)
            chunk_count += 1

    table_list = list(MapCsc().MAP_DICT.keys())
    # print(table_list)

    for i in table_list:
        MAP = MapCsc(i)
        for connector_id, table_name, columns, date_column, _, _ in MAP.get_map_tables():
            db_name = connector_id.split('_')[0]
            table_file_name = table_name.replace(
                '[', '').replace(']', '').split('.')[-1]
            res = extract_sql(connector_id, table_name,
                              columns, date_column, '20210301')
        break


# test_failed()

# 合并算法 测试转换

def test_trans(csc_table):
    from map_table import MapCsc
    app = MapCsc(csc_table)
    app.update_multi_data('wind', 'ASHAREEODPRICES',
                          '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/ASHAREEODDERIVATIVEINDICATOR/20210301.csv')
    app.update_multi_data('suntime', 'QT_STK_DAILY',
                          '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/QT_STK_DAILY/20210301.csv')
    res = app.transform_df()

    app2 = MapCsc(csc_table)
    app2.init_multi_data(res)
    res2 = app2.merge_df()

    app3 = MapCsc(csc_table)
    res3 = app3.check_df(res2['table_path'])
    print()


# 测试新的静态文件
def test_sql_byfile():
    from map_table import MapCsc
    app = MapCsc('CSC_Prices')
    for conn, table, sql in app.get_map_tables_by_date('20220101'):
        print(conn, table, sql)

    # test_trans('CSC_Prices')
# test_sql_byfile()


def get_sql_by_table(table_name: str, load_date: str) -> tuple:
    """
    根据表名和日期返回sql查询语句
    :return:( connector_id, table_name, sql, )
    """
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
            suntime_sql_dict = json.load(f)  # 去数据字典文件中寻找
        return_sql = suntime_sql_dict[table_name]['sql'] % (
            table_name, f"{load_date}")

    else:
        raise Exception

    return (db + AF_CONN, table_name, return_sql)
