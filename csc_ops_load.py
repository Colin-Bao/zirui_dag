#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None

import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import timedelta, date, datetime


@task
def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句
    :return:( connector_id, return_sql, table_name, load_date)
    """
    # 查找属于何种数据源
    import json
    db = json.loads(Variable.get("csc_table_db"))[table_name]  # 去数据字典文件中寻找
    # 不同数据源操作
    if db == 'wind':
        wind_sql = json.loads(Variable.get("csc_wind_sql"))
        return_sql = wind_sql[table_name] % f"\'{load_date}\'"

    elif db == 'suntime':
        # TODO 没有写增量表，需要增加逻辑判断
        suntime_sql = json.loads(Variable.get("csc_suntime_sql"))[
            table_name]['sql']  # 去数据字典文件中寻找
        return_sql = suntime_sql % (
            'zyyx.' + table_name, f"{load_date}") if suntime_sql else None
    else:
        raise Exception
    return {'connector_id': db + '_af_connector', 'query_sql': return_sql, 'table_name': table_name,
            'load_date': load_date}


@task
def load_sql_query(xcom_dict: dict, load_path: str = "csc_load_path") -> dict:
    """
     根据sql查询语句下载数据到本地
    :return:
    """
    # -----------------参数传递----------------- #
    connector_id = xcom_dict['connector_id']
    query_sql = xcom_dict['query_sql']
    table_name = xcom_dict['table_name']
    load_date = xcom_dict['load_date']

    # -----------------输出文件的路径----------------- #
    LOAD_PATH = Variable.get(load_path) + table_name

    # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------df执行sql查询,保存文件----------------- #
    if not os.path.exists(LOAD_PATH):
        os.mkdir(LOAD_PATH)

    # 防止服务器内存占用过大

    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=10000):
        if chunk_count == 0:
            table_path = LOAD_PATH + f'/{load_date}.parquet'
            df_chunk.to_parquet(table_path, index=False)
            return {'table_path': table_path, 'table_name': table_name, 'table_empty': df_chunk.empty, 'query_sql': query_sql}
        else:
            # TODO 只保存chunksize行，如果超过chunksize行要分片保存再合并
            break
        chunk_count += 1


@task
def check_load(xcom_dict: dict) -> dict:
    """
    检查保存后的文件,并输出字段类型与含义
    :param data_dict:
    :return:
    """
    # 传参数
    table_empty = xcom_dict['table_empty']
    table_path = xcom_dict['table_path']
    table_name = xcom_dict['table_name']

    if table_empty:
        return xcom_dict

    # 文件目录
    DICT_DATA_PATH = Variable.get('csc_data_dict_path') + table_name+'.csv'
    LOAD_TYPE_PATH = Variable.get('csc_load_path') + table_name+'/' + table_path.split(
        '/')[-1].split('.')[0]+'_type.parquet'

    # 读取load的文件
    import pandas as pd
    df_check = pd.read_csv(table_path)
    df_info = pd.DataFrame(
        {"column": df_check.columns,
         "null_ratio": df_check.isnull().sum() / df_check.count(),
         "type": df_check.dtypes})

    # 读取字典文件
    df_dict = pd.read_csv(DICT_DATA_PATH)

    # 合并
    df_merge = pd.merge(df_info, df_dict, how='left',
                        left_on='column', right_on='fieldName')
    df_merge.to_csv(LOAD_TYPE_PATH, index=False)

    return xcom_dict


@task
def send_info(xcom_dict: dict):
    """
    发送邮件
    """
    # 解析load的内容
    table_empty = xcom_dict['table_empty']
    # 跳过非空值
    if not table_empty:
        # TODO 如果不为空的处理
        return
    # 跳过时间段
    if datetime.now().hour != 7:
        return

    table_name = xcom_dict['table_name']
    query_sql = xcom_dict['query_sql']

    email_content = {'subject': 'Airflow 警告',
                     'html_content':
                     f""" 
                     <h3>{table_name} 是空的</h3> 
                     详细SQL语法为:{query_sql}
                     """}

    # 发送邮件
    # TODO 如果过了指定时间
    #
    from airflow.utils.email import send_email
    send_email(to=['523393445@qq.com'],
               subject=email_content['subject'],
               html_content=email_content['html_content'],)


# [START DAG] 实例化一个DAG


@dag(
    default_args={'owner': 'zirui',
                  'email': ['523393445@qq.com', ],  # '821538716@qq.com'
                  'email_on_failure': True,
                  'email_on_retry': True,
                  #   'retries': 1,
                  "retry_delay": timedelta(minutes=1), },
    schedule="0/30 1-6 * * 1-7",
    start_date=pendulum.datetime(2022, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['数据加载'],
)
# 在DAG中定义任务
def csc_ops_load():
    # [START main_flow]
    def start_tasks(table_name: str):
        """
        任务流控制函数，用于被多进程调用，每张表下载都是一个并行的进程
        :return:
        """

        # 下载昨天的数据
        load_date = (date.today() + timedelta(-1)
                     ).strftime('%Y%m%d')
        # load_date = '20220102'
        # ETL
        load_return = load_sql_query.override(
            task_id='L_' + table_name, outlets=[Dataset('L_' + table_name, extra={'load_date': load_date})])(
            extract_sql_by_table.override(task_id='E_' + table_name, )(table_name, load_date))

        # 根据load的结果是否为空，进行告警或者下一步动作
        # send_info(check_load(load_return))

    # 多进程异步执行
    test_list = ['ASHAREBALANCESHEET']
    table_list = [
        'FIN_BALANCE_SHEET_GEN', 'ASHAREBALANCESHEET', 'ASHARECASHFLOW', 'FIN_CASH_FLOW_GEN',
        'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
        'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
        'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION']
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as executor:
        _ = {executor.submit(start_tasks, table): table for table in test_list}

    # [END main_flow]


# [END DAG]

# [START dag_invocation]
dag = csc_ops_load()
# [END dag_invocation]
