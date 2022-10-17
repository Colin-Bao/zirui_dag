#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :merge_table.py
# @Time      :2022/9/7 15:16
# @Author    :Colin
# @Note      :None

import os
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import timedelta, date, datetime


def get_type_from_df(table_name: str, df_by_sql) -> dict:
    import pyarrow as pa
    import pandas as pd
    # df_by_sql = df_by_sql.copy()
    # print(df_by_sql.reset_index().columns)
    pa_schema = pa.Table.from_pandas(df_by_sql).schema  # 取出schema
    # print(, pa_schema)
    # 生成数据结构字典
    type_config = {}

    # 遍历spa_schema
    for field in pa_schema:
        # 对应好类型
        pyarrow_type = str(field.type)
        pandas_type = str(df_by_sql[field.name].dtype)
        trans_schema_type = pyarrow_type  # 自定义转换类型

        # 修改类型 主要出错在null上
        if trans_schema_type == 'null':
            df_info = pd.read_csv(
                Variable.get('csc_table_info') + f'{table_name}.csv')
            if df_info.empty:
                # TODO 有的没有,查回来是空的
                break
            else:
                db_type = df_info[df_info['COLUMN_NAME']
                                  == field.name]['DATA_TYPE'].iloc[0]

            if db_type == 'varchar':
                trans_schema_type = 'string'
            elif db_type == 'numeric':
                trans_schema_type = 'double'
            elif db_type == 'NUMBER':
                trans_schema_type = 'double'
            elif db_type == 'VARCHAR2':
                trans_schema_type = 'string'

        type_config.update({
            field.name: {
                'pandas_type': pandas_type,
                'pyarrow_type': pyarrow_type,
                'parquet_type': trans_schema_type
            }
        })

    # 导出config
    import json
    CONFIG_PATH = Variable.get('csc_parquet_schema') + f'{table_name}.json'
    with open(CONFIG_PATH, 'w') as f:
        json.dump(type_config, f)

    # 进行转换


def check_update(table_name, table_path, df_chunk, load_date, dynamic):
    """
    检查- 输出日志
    """
    import pandas as pd
    import datacompy
    # --------------找信息--------------#
    # 找到PK
    df_info = pd.read_csv(Variable.get(
        'csc_table_info') + f'{table_name}.csv')

    df_mask = (df_info['CONSTRAINT_TYPE']
               == 'PK') | (df_info['CONSTRAINT_TYPE'] == 'P')

    # 有的是空的
    pk_column = df_info[df_mask]['COLUMN_NAME'].to_list()
    df_old = pd.read_parquet(table_path)
    df_new = df_chunk.copy()

    # --------------日志文件路径--------------#
    today_date = date.today().strftime('%Y%m%d')
    LOG_PATH = Variable.get(
        'csc_log_path') + f"{table_name}/"
    _ = os.mkdir(LOG_PATH) if not os.path.exists(
        LOG_PATH) else None
    LOG_OUTPUT_PATH = LOG_PATH + load_date if dynamic else LOG_PATH+today_date

    # --------------输出日志文件--------------#
    import logging
    from datacompy.core import LOG
    fh = logging.FileHandler(f'{LOG_OUTPUT_PATH}.log', encoding='utf-8')
    fh_formatter = logging.Formatter(
        '%(asctime)s %(module)s-%(lineno)d [%(levelname)s]:%(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    fh.setFormatter(fh_formatter)
    LOG.addHandler(fh)

    # --------------比较--------------#
    if pk_column:  # 不是空
        # TODO 临时修复 保存的没有obj id
        # print(df_old.columns, df_new.columns)
        if pk_column in df_old.columns.to_list() and pk_column in df_new.columns.to_list():
            compare_log = datacompy.Compare(
                df_old, df_new, df1_name='df_old', df2_name='df_new', join_columns=pk_column)
        else:
            compare_log = datacompy.Compare(
                df_old, df_new, df1_name='df_old', df2_name='df_new', on_index=True)
    else:
        # TODO 已经采用临时手段修复
        compare_log = datacompy.Compare(
            df_old, df_new, df1_name='df_old', df2_name='df_new', on_index=True)

    # --------------输出差异比较--------------#
    with open(f'{LOG_OUTPUT_PATH}.txt', 'w') as f:
        f.write(compare_log.report())


@task
def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句
    :return:( connector_id, return_sql, table_name, load_date)
    """
    # 查找属于何种数据源
    import json
    # 查找属于何种数据源
    with open(Variable.get("csc_table_db")) as j:
        db = json.load(j)[table_name]  # 去数据字典文件中寻找

    # 不同数据源操作
    if db == 'wind':
        wind_sql = json.loads(Variable.get("csc_wind_sql"))[table_name]
        dynamic = True if wind_sql.upper().count('WHERE') == 1 else False
        return_sql = wind_sql % f"\'{load_date}\'" if dynamic else wind_sql
    elif db == 'suntime':
        suntime_sql_dict = json.loads(Variable.get("csc_suntime_sql"))
        suntime_sql = suntime_sql_dict[table_name][
            'sql'] if table_name in suntime_sql_dict.keys(
        ) else "SELECT * FROM %s"
        suntime_sql = "SELECT * FROM %s" if suntime_sql == "" else suntime_sql  # suntime_sql可能为空
        dynamic = True if suntime_sql.upper().count('WHERE') == 1 else False
        return_sql = suntime_sql % ('zyyx.' + table_name,
                                    load_date) if dynamic else suntime_sql % (
                                        'zyyx.' + table_name)

    # 日期
    select_date = return_sql.upper().split('WHERE')[-1].split(
        '=')[0].strip() if dynamic else ""

    return {
        'connector_id': db + '_af_connector',
        'query_sql': return_sql,
        'table_name': table_name,
        'load_date': load_date,
        'dynamic': dynamic,
        'date_column': select_date
    }


@task
def load_sql_query(xcom_dict: dict, load_path: str = "csc_load_path") -> dict:
    """
    根据sql查询语句下载数据到本地
    :return:xcom_dict
    """
    # -----------------参数传递----------------- #
    connector_id = xcom_dict['connector_id']
    query_sql = xcom_dict['query_sql']
    table_name = xcom_dict['table_name']
    load_date = xcom_dict['load_date']
    dynamic = xcom_dict['dynamic']
    date_column = xcom_dict['date_column']

    # -----------------输出文件的路径----------------- #
    LOAD_PATH = Variable.get(load_path) + table_name

    # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------df执行sql查询,保存文件----------------- #
    import os
    _ = os.mkdir(LOAD_PATH) if not os.path.exists(LOAD_PATH) else None
    # 防止服务器内存占用过大

    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks(query_sql, chunksize=10000):
        if chunk_count == 0:
            table_path = LOAD_PATH + \
                f'/{load_date}.parquet' if dynamic else f'{LOAD_PATH}/{table_name}.parquet'

            # 转为str：orcale有问题
            if connector_id == 'suntime_af_connector':
                str_column = df_chunk.select_dtypes(
                    include='object').columns
                df_chunk[str_column] = df_chunk[str_column].astype('str')

            # ----------------保存检查点:保存规则----------------- #
            if not df_chunk.empty:

                # 0. 传出格式 第一次运行的时候要用
                get_type_from_df(table_name, df_chunk)
                # _ = get_type_from_df(table_name, df_chunk) if not os.path.exists(
                # Variable.get('csc_parquet_schema') + f'{table_name}.json') else None

                # 1.读取csc_parquet_schema
                import json
                import pyarrow as pa
                with open(Variable.get(
                        'csc_parquet_schema') + f'{table_name}.json') as json_file:
                    schema_list = [(column_name, types['parquet_type'])
                                   for column_name, types in json.load(json_file).items()]

                # ----------------更新检测----------------- #
                if os.path.exists(table_path):  # 如果旧文件存在
                    check_update(table_name, table_path,
                                 df_chunk, load_date, dynamic)
                    # pass

                # 2.修改schema 输出文件
                df_chunk.to_parquet(
                    table_path, engine='pyarrow', schema=pa.schema(schema_list))

            return {'table_path': table_path, 'table_name': table_name,
                    'table_empty': df_chunk.empty, 'dynamic': dynamic, 'date_column': date_column, }
        else:
            # TODO 只保存chunksize行，如果超过chunksize行要分片保存再合并
            break


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
def get_config(xcom_dict: dict) -> dict:
    """
    得到鹏队要的config表
    """
    import json
    from datetime import datetime
    import pandas as pd
    if xcom_dict['table_empty']:
        return xcom_dict
    table_name = xcom_dict['table_name']
    dynamic = xcom_dict['dynamic']
    date_column = xcom_dict['date_column']

    # 1.csc_table_info文件路径
    df_table_info = pd.read_csv(Variable.get('csc_table_info') +
                                f'{table_name}.csv',
                                index_col='COLUMN_NAME',
                                usecols=[
                                    'COLUMN_NAME',
                                    'CONSTRAINT_TYPE',
                                    'DATA_TYPE',
                                    'DATA_LENGTH',
    ])
    # 2.csc_parquet_schema文件路径
    df_schema_info = pd.read_json(
        Variable.get('csc_parquet_schema') +
        f'{table_name}.json').transpose()[['parquet_type']]

    # 3.连接
    df_config = df_schema_info.join(df_table_info, how='left')
    df_config['DATA_TYPE'] = df_config['DATA_TYPE'].astype('str').str.upper(
    ) + ' ' + df_config['DATA_LENGTH'].astype('str').str.upper()

    # 4.取出主键
    pk_list = [
        k for k, v in df_config[['CONSTRAINT_TYPE'
                                 ]].transpose().to_dict().items()
        if v['CONSTRAINT_TYPE'] == 'PK' or v['CONSTRAINT_TYPE'] == 'P'
    ]

    # 5.取出columns_config
    df_config.rename(columns={'DATA_TYPE': 'original_type'}, inplace=True)
    columns_config = df_config[['parquet_type',
                                'original_type']].transpose().to_dict()

    # column_dict={for i in df_config}
    # 6.合并config
    config_dict = {
        "primary_key": pk_list,
        "dynamic": dynamic,
        "date_column": date_column,
        "last_update": str(datetime.now()),
        "all_columns": columns_config
    }

    # 输出config
    with open(
            Variable.get('csc_load_path') + f'{table_name}/config.json',
            'w') as f:
        json.dump(config_dict, f)

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
        load_date = (date.today() + timedelta(-1)).strftime('%Y%m%d')
        # load_date = '20221015'
        # load_date = '20221014'
        load_date = '20220105'

        # ETL
        load_return = load_sql_query.override(
            task_id='L_' + table_name, )(extract_sql_by_table.override(task_id='E_' + table_name, )(table_name, load_date))
        # config
        get_config.override(task_id='C_' + table_name)(load_return)
        # transform_schema.override(task_id='T_' + table_name)(load_return)
        # outlets=[Dataset('L_' + table_name, extra={'load_date': load_date})]
        # 根据load的结果是否为空，进行告警或者下一步动作
        # send_info(check_load(load_return))

    # 多进程异步执行
    import json
    with open(Variable.get("csc_input_table")) as j:
        table_list = json.load(j)['need_tables']
    test_list = ['ASHAREAUDITOPINION', ]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = {executor.submit(start_tasks, table): table for table in table_list}

    # [END main_flow]


# [END DAG]

# [START dag_invocation]
csc_ops_load()
# [END dag_invocation]
