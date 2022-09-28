#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None
import os
from tabnanny import check
import pendulum
from airflow.decorators import dag, task
from datetime import datetime, timedelta
# import logging

# -----------------处理数据源合并----------------- #
# from zirui_dag.map_table import *

# -----------------输出文件的路径----------------- #
OUTPUT_PATH = '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/'
# if not os.path.exists(OUTPUT_PATH):
# os.mkdir(OUTPUT_PATH)

# -----------------配置本地日志信息----------------- #
# logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
#                     filename=f'{OUTPUT_PATH}csc.log',
#                     filemode='a',  # 模式，有w和a，w就是写模式，a是追加模式，
#                     format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'  # 日志格式
#                     )
# fh = logging.FileHandler(f'{OUTPUT_PATH}csc.log', encoding='utf8', mode='w')
# fh.setFormatter(
# logging.Formatter('%(asctime)s %(filename)s %(funcName)s [line:%(lineno)d] %(levelname)s %(message)s'))
# logging.getLogger().addHandler(fh)


# 获得当前信息
def task_info(context):
    return f"{context['task_instance_key_str']}"


# 定义失败提醒
def task_failure_alert(context):
    from msg.MailMsg import MailMsg
    with MailMsg() as msg:
        msg.send_msg('任务失败',
                     f"任务失败了!, 失败任务ID是: {context['task_instance_key_str']}")
        # TODO airflow的自己的日志覆盖了，需要重写一个日志提示
        # logging.error(f"任务失败了!, 失败任务ID是: {context['task_instance_key_str']}")


# 定义成功提醒
def dag_success_alert(context):
    pass
    # TODO airflow的自己的日志覆盖了，需要重写一个日志提示
    # logging.info(f"DAG has succeeded, run_id: {context['run_id']}")


# [START DAG] 实例化一个DAG
@dag(
    default_args={'owner': 'ZIRUI', },
    schedule_interval=None,
    # schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 9, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    on_success_callback=dag_success_alert,
    tags=['数据运维'],
)
# 在DAG中定义任务
def csc_database_etl():
    # 提取-> 从数据库按照日期提取需要的表
    @task(on_failure_callback=task_failure_alert)
    def extract_sql(connector_id, table_name, column, date_column, start_date) -> dict:
        import pandas as pd  # 文档说不要写在外面，影响性能
        table_filename = table_name.replace(
            '[', '').replace(']', '').split('.')[-1]
        # return {'table_name': table_filename, 'table_path': OUTPUT_PATH + table_filename}
        # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
        from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
        sql_hook = BaseHook.get_connection(connector_id).get_hook()
        if connector_id == 'wind_af_connector':
            query_sql = f"""SELECT {column} FROM {table_name} WHERE [{date_column}] ='{start_date}' """
        elif connector_id == 'suntime_af_connector':
            query_sql = f"""SELECT {column} FROM {table_name} WHERE {date_column} = to_date('{start_date}','yyyy-MM-dd')  """
        else:
            raise Exception('未注册的连接名', 200)
        # -----------------df执行sql查询----------------- #
        # from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # 数据库接口
        table_filename = table_name.replace(
            '[', '').replace(']', '').split('.')[-1]
        if not os.path.exists(OUTPUT_PATH + table_filename):
            os.mkdir(OUTPUT_PATH + table_filename)

        # 防止服务器内存占用过大
        chunk_count = 0
        chunks = sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=10000)
        path = ''
        for df_chunk in chunks:
            if chunk_count == 0:
                path = OUTPUT_PATH + table_filename + f'/{start_date}.csv'
                df_chunk.to_csv(path, index=False)
            else:
                # TODO 解决合并问题
                path = OUTPUT_PATH + table_filename + \
                    f'/{start_date}_{chunk_count}.csv'
                df_chunk.to_csv(path, index=False)
            chunk_count += 1
        # 传递到下一个子任务
        return {'table_db': connector_id.split('_')[0], 'table_name': table_filename, 'table_path': path}

    # 变换 -> 对DataFrame进行格式转换
    @task(on_failure_callback=task_failure_alert)
    def transform(MERGE_TABLE, MULTI_TABLE_DICT: dict) -> dict:
        from zirui_dag.map_table import MapCsc
        MAP = MapCsc(MERGE_TABLE)
        MAP.init_multi_data(MULTI_TABLE_DICT)
        df_dict = MAP.transform_df()
        return df_dict

    # 合并->数据合并
    @task
    def merge(MERGE_TABLE, MULTI_TABLE_DICT: dict) -> dict:
        from zirui_dag.map_table import MapCsc
        MAP = MapCsc(MERGE_TABLE)
        MAP.init_multi_data(MULTI_TABLE_DICT)
        df_dict = MAP.merge_df()
        return df_dict

    # 检查-> 合并后的信息检查
    @task
    def check(MERGE_TABLE, PATH: str) -> dict:
        from zirui_dag.map_table import MapCsc
        return MapCsc(MERGE_TABLE).check_df(PATH)

    # [START main_flow]  API 2.0 会根据任务流调用自动生成依赖项,不需要定义依赖
    def start_tasks(csc_merge_table):  # 按照规则运行所有任务流
        from zirui_dag.map_table import MapCsc
        # 1.实例化用于合并的APP
        MAP = MapCsc(csc_merge_table)

        for connector_id, table_name, columns, date_column, _, _ in MAP.get_map_tables():
            db_name = connector_id.split('_')[0]
            table_file_name = table_name.replace(
                '[', '').replace(']', '').split('.')[-1]

            # TODO 把日期写成参数的形式
            res = extract_sql.override(task_id='L_' + table_file_name)(
                connector_id, table_name, columns, date_column, '20220927')

            # 更新数据
            MAP.update_multi_data(db_name, table_file_name, res['table_path'])

        # 2.生成转换表
        dict_t = transform.override(
            task_id='T_' + csc_merge_table)(csc_merge_table, MAP.MULTI_DB_DICT)

        # 3.合并
        dict_m = merge.override(
            task_id='M_' + csc_merge_table)(csc_merge_table, dict_t)

        # 4.检查数据准确性
        check.override(
            task_id='C_' + csc_merge_table)(csc_merge_table, dict_m['table_path'])

    # 多进程异步执行,submit()中填写函数和形参
    start_tasks('CSC_Income')

    # from concurrent.futures import ThreadPoolExecutor  # 多进程并行
    # from zirui_dag.map_table import MapCsc
    # table_list = list(MapCsc().MAP_DICT.keys())
    # with ThreadPoolExecutor(max_workers=1) as executor:
    #     _ = {executor.submit(start_tasks, table): table for table in [
    #         'CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income', 'CSC_Prices', 'CSC_Profit_Notice', 'CSC_Profit_Express']}
    # [END main_flow]


# [END DAG]


# [START dag_invocation]
dag = csc_database_etl()
# [END dag_invocation]


# -----------------调试命令----------------- #
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)
# kill $(cat airflow-scheduler.pid)
# lsof -i :8080|grep -v "PID"|awk '{print "kill -9",$2}'|sh
# source activate zirui_env 要在我的虚拟环境中执行
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-webserver.pid)
# airflow webserver --port 8080
# ps - ef | grep 8080 | grep - v grep | cut - c 9-15 | xargs kill - 9
# airflow scheduler
# airflow users create \
#           --username zirui \
#           --firstname FIRST_NAME \
#           --lastname LAST_NAME \
#           --role Admin \
#           --email admin@example.org
