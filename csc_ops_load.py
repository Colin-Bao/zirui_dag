#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None
from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import datetime, timedelta, date


# TODO load 后的类型检查
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
def load_sql_query(data_dict: dict, load_path: str = "csc_load_path") -> dict:
    """
     根据sql查询语句下载数据到本地
    :return:
    """
    # -----------------参数传递----------------- #
    connector_id = data_dict['connector_id']
    query_sql = data_dict['query_sql']
    table_name = data_dict['table_name']
    load_date = data_dict['load_date']

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
    for df_chunk in sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=1000):
        if chunk_count == 0:
            file_path = LOAD_PATH + f'/{load_date}.csv'
            df_chunk.to_csv(file_path, index=False)
            return {'file_path': file_path}
        else:
            # TODO 只保存chunksize行，如果超过chunksize行要分片保存再合并
            break
        chunk_count += 1


@task
def check_load(data_dict: dict) -> dict:
    """
    检查保存后的文件
    :param data_dict:
    :return:
    """
    file_path = data_dict['file_path']
    import pandas as pd
    check_df = pd.read_csv(file_path)

    return data_dict


# [START DAG] 实例化一个DAG


@dag(
    default_args={'owner': 'zirui', },
    schedule="0 17 * * 1-7",
    start_date=pendulum.datetime(2022, 9, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['数据运维', '数据加载'],
)
# 在DAG中定义任务
def csc_ops_load():
    # [START main_flow]
    def start_tasks(table_name: str):
        """
        任务流控制函数，用于被多进程调用，每张表下载都是一个并行的进程
        :return:
        """
        # extract_sql_by_table()
        load_date = (date.today() + timedelta(-1)).strftime('%Y%m%d')  # 下载昨天的数据
        load_sql_query.override(
            task_id='L_' + table_name, outlets=[Dataset('L_' + table_name, extra={'load_date': load_date})])(
            extract_sql_by_table.override(task_id='E_' + table_name, )(table_name, load_date))
        # check_load(load_return)

    # 多进程异步执行
    # start_tasks('FIN_BALANCE_SHEET_GEN')
    table_list = [
        'FIN_BALANCE_SHEET_GEN', 'ASHAREBALANCESHEET', 'ASHARECASHFLOW', 'FIN_CASH_FLOW_GEN',
        'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
        'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
        'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION']
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as executor:
        _ = {executor.submit(start_tasks, table): table for table in table_list}

    # [END main_flow]


# [END DAG]

# [START dag_invocation]
dag = csc_ops_load()
# [END dag_invocation]

# kill -9 2819187
# airflow webserver --port 8081
# kill -9 $(lsof -i:8081 -t) 2> /dev/null
# kill -9 $(lsof -i:8793 -t) 2> /dev/null
# P@ssw0rd
# sudo fuser - k 8793/tcp
# sudo fuser - k 8081/tcp
# airflow scheduler
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-webserver.pid)
# sudo lsof -i: 8081 | grep - v "PID" | awk '{print "kill -9",$2}' | sh
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)
