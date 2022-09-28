#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None
import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG, Dataset
from airflow.models import Variable
from datetime import datetime, timedelta


# -----------------输出文件的路径----------------- #
DAG_PATH = Variable.get("csc_dag_path")
OUTPUT_PATH = DAG_PATH+'output/'


# [START DAG] 实例化一个DAG
@dag(
    default_args={'owner': 'zirui', },
    schedule="0 0 * * 1-5",
    start_date=pendulum.datetime(2022, 9, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['数据运维', '1期项目'],
)
# 在DAG中定义任务
def csc_ops_load():

    def get_sql_by_table(table_name: str, load_date: str) -> tuple:
        """
        根据表名和日期返回sql查询语句
        :return:( connector_id, table_name, sql, )
        """
        AF_CONN = '_af_connector'  # 数据库连接器名称
        import json
        # 查找属于何种数据源
        with open(DAG_PATH+'sql_files/all_table_db' + '.json') as f:  # 去数据字典文件中寻找
            db = json.load(f)[table_name]

        # 不同数据源操作
        if db == 'wind':
            from zirui_dag.sql_files.wind_sql import sql_sentence
            wind_sql_dict = {k.upper(): v.replace('\n      ', '').replace(
                '\n', '') for k, v in sql_sentence.items()}  # 转成大写
            return_sql = wind_sql_dict[table_name] % f"\'{load_date}\'"

        elif db == 'suntime':
            # TODO 没有写增量表，需要增加逻辑判断
            import json
            with open(DAG_PATH+'sql_files/suntime_sql_merge' + '.json') as f:
                suntime_sql_dict = json.load(f)  # 去数据字典文件中寻找
            return_sql = suntime_sql_dict[table_name]['sql'] % (
                table_name, f"{load_date}")
        else:
            raise Exception
        return (db + AF_CONN, return_sql)

    # 提取-> 从数据库按照日期提取需要的表
    @task()
    def load_sql_query(table_name: str, load_date: str):
        # ----------------- 生成查询语句----------------- #
        connector_id, query_sql = get_sql_by_table(table_name, load_date)

        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
        sql_hook = BaseHook.get_connection(connector_id).get_hook()

        # -----------------df执行sql查询,保存文件----------------- #
        if not os.path.exists(OUTPUT_PATH + table_name):
            os.mkdir(OUTPUT_PATH + table_name)

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

    # [START main_flow]  API 2.0 会根据任务流调用自动生成依赖项,不需要定义依赖
    def start_tasks(table_name: str, load_date: str):  # 按照规则运行所有任务流
        load_sql_query.override(task_id='L_'+table_name, outlets=['L' +
                                                                  OUTPUT_PATH+table_name])(table_name, load_date)

    # 多进程异步执行,submit()中填写函数和形参
    start_tasks('ASHAREBALANCESHEET', '20220301')

    # [END main_flow]


# [END DAG]

# [START dag_invocation]
dag = csc_ops_load()
# [END dag_invocation]
