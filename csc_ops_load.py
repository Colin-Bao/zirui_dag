from airflow.models import Variable
from datetime import timedelta, date, datetime
import json
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
# from airflow.datasets import Dataset
from datetime import timedelta, date, datetime

# -----------------加载运行依赖的配置信息----------------- #
with open(Variable.get("csc_input_table")) as j:
    TABLE_LIST = json.load(j)['need_tables']
with open(Variable.get("db_sql_dict")) as j:
    DB_SQL_DICT = json.load(j)  # 依赖的SQL语句

CONFIG_PATH = '/home/lianghua/ZIRUI/rely_files/test_type_df_and_parquet/'  # 转换依赖的CONFIG
LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load/'  # 输出路径


def get_new_pk(select_table):
    """
    鹏队临时加的需求,改config里面的主键
    """
    PrimaryKeys = {
        'aindexcsi500weight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexeodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'aindexfreeweight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexhs300closeweight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexhs300freeweight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexmembers': ['S_INFO_WINDCODE', 'S_CON_WINDCODE'],
        'aindexmemberscitics': ['S_INFO_WINDCODE', 'S_CON_WINDCODE'],
        'ASHAREPLANTRADE': ['S_INFO_WINDCODE', 'ANN_DT', 'ANN_DT_NEW'],
        'ashareannfinancialindicator': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareauditopinion': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'asharebalancesheet': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareblocktrade': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'asharecalendar': ['S_INFO_EXCHMARKET'],
        'asharecashflow': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareconsensusdata': ['S_INFO_WINDCODE', 'EST_DT', 'EST_REPORT_DT', 'CONSEN_DATA_CYCLE_TYP'],
        'ashareconsensusrollingdata': ['S_INFO_WINDCODE', 'EST_DT', 'ROLLING_TYPE'],
        'ashareconseption': ['S_INFO_WINDCODE', 'WIND_SEC_CODE'],
        'asharedescription': ['S_INFO_WINDCODE'],
        'asharedividend': ['S_INFO_WINDCODE', 'S_DIV_PROGRESS', 'ANN_DT'],
        'ashareearningest': ['S_INFO_WINDCODE', 'WIND_CODE'],
        'ashareeodderivativeindicator': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ashareeodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'asharefinancialderivative': ['S_INFO_COMPCODE', 'BEGINDATE', 'ENDDATE'],
        'asharefinancialindicator': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareincome': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareipo': ['S_INFO_WINDCODE'],
        'ashareisactivity': ['S_INFO_WINDCODE', 'S_SURVEYDATE', 'S_SURVEYTIME', 'S_ACTIVITIESTYPE'],
        'asharemajorevent': ['S_INFO_WINDCODE', 'S_EVENT_CATEGORYCODE', 'S_EVENT_ANNCEDATE'],
        'asharemanagementholdreward': ['S_INFO_WINDCODE', 'ANN_DATE'],
        'asharemoneyflow': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ashareprofitexpress': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareprofitnotice': ['S_INFO_WINDCODE', 'S_PROFITNOTICE_DATE', 'S_PROFITNOTICE_PERIOD'],
        'asharereginv': ['S_INFO_WINDCODE', 'STR_DATE'],
        'asharesalessegment': ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_SEGMENT_ITEM'],
        'asharest': ['S_INFO_WINDCODE', 'ANN_DT'],
        'asharetradingsuspension': ['S_INFO_WINDCODE', 'S_DQ_SUSPENDDATE'],
        'ccommodityfutureseodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ccommodityfuturespositions': ['S_INFO_WINDCODE', 'TRADE_DT', 'S_INFO_COMPCODE', 'FS_INFO_TYPE'],
        'cfuturescalendar': [],
        'cfuturescontpro': ['S_INFO_WINDCODE'],
        'cfuturescontprochange': ['S_INFO_WINDCODE'],
        'cfuturesdescription': ['S_INFO_WINDCODE'],
        'cfuturesmarginratio': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'chinamutualfundstockportfolio': ['S_INFO_WINDCODE', 'ANN_DATE'],
        'con_forecast_roll_stk': ['stock_code', 'con_date'],
        'con_forecast_stk': ['stock_code', 'con_date'],
        'con_rating_stk': ['stock_code', 'con_date'],
        'con_target_price_stk': ['stock_code', 'con_date'],
        'der_conf_stk': ['stock_code', 'con_date', 'con_year'],
        'der_con_dev_roll_stk': ['stock_code', 'con_date'],
        'der_diver_stk': ['stock_code', 'con_date', 'con_year'],
        'der_excess_stock': ['stock_code', 'declare_date', 'report_year'],
        'der_focus_stk': ['stock_code', 'con_date'],
        'der_forecast_adjust_num': ['stock_code', 'con_date', 'con_year'],
        'der_prob_below_stock': ['stock_code', 'report_year', 'declare_date'],
        'der_prob_excess_stock': ['stock_code', 'report_year', 'report_quarter', 'declare_date'],
        'der_rating_adjust_num': ['stock_code', 'con_date'],
        'der_report_num': ['stock_code', 'con_date'],
        'rpt_earnings_adjust': ['stock_code', 'current_create_date', 'report_year'],
        'rpt_forecast_stk': ['stock_code', 'create_date', 'report_year'],
        'rpt_gogoal_rating': ['gg_rating_code'],
        'rpt_rating_adjust': ['stock_code', ''],
        'rpt_target_price_adjust': ['stock_code', 'updatetime', 'organ_id'],
        'shscchannelholdings': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'shscmembers': ['S_INFO_WINDCODE'],
        'szscmembers': ['S_INFO_WINDCODE']
    }
    PrimaryKeys = {k.upper(): v for k, v in PrimaryKeys.items()}
    return PrimaryKeys[select_table]


# TODO DB_SQL_DICT删除 demo
@task
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


def transform(df_chunk, select_table):
    """
    转换3次,输出config
    """
    import pandas as pd
    import numpy as np

    # -----------------去字典文件中找config信息----------------- #
    with open(f'{CONFIG_PATH}{select_table}.json') as j:
        config_dict = json.load(j)  # 转换type的config

    def trans_dtype(df_trans):
        """
        转换dtypes前的步骤
        """

        dtype_config = {k: v['pandas_type'] for k, v in config_dict.items()}
        # -----------------转换dtype前要加的步骤---------------- #
        # 非字符串处理
        float_columns = [k for k, v in dtype_config.items() if v == 'float64']
        df_trans[float_columns] = df_trans[float_columns].replace(
            [None, 'None', np.nan], np.float64(0))
        # 日期处理
        int_columns = [k for k, v in dtype_config.items() if v == 'int64']
        df_trans[int_columns] = df_trans[int_columns].replace([None, 'None', np.nan],
                                                              np.int64(0))
        # 日期处理，有的传回来的值是一个Timestamp对象

        for i in int_columns:
            df_trans[i] = df_trans[i].apply(lambda x: x.timestamp()
                                            if type(x) == pd.Timestamp else x)
        # -----------------转换dtype---------------- #
        df_trans = df_trans.astype(dtype=dtype_config)
        return df_trans

    def trans_hdf(df_trans):
        """
        转换hdf前的步骤,按照鹏队的需求把str的None转为''
        """
        obj_columns = list(df_trans.select_dtypes(
            include=['object']).columns.values)
        df_trans[obj_columns] = df_trans[obj_columns].replace([None, 'None'],
                                                              '')
        return df_trans

    def trans_schema(df_trans):
        """
        修改统一的schema
        """
        import pyarrow as pa
        schema_list = [(column_name, types['parquet_type'])
                       for column_name, types in config_dict.items()
                       if column_name in df_trans.columns.to_list()]
        pa_table = pa.Table.from_pandas(df_trans,
                                        schema=pa.schema(schema_list))
        return pa_table

    def get_config():
        """
        按表输出config.json文件
        """
        table_info = DB_SQL_DICT[select_table]
        # -----------------cols----------------- #
        type_config = {
            k: {
                'original_type': v['database_type'],
                'parquet_type': v['parquet_type']
            }
            for k, v in config_dict.items()
        }

        # -----------------date----------------- #
        table_info.update({"all_columns": type_config})
        # ---------------输出----------------- #

        out_put_config = {
            "primary_key": get_new_pk(select_table),
            "original_table": table_info['oraignal_table'],
            'dynamic': table_info['dynamic'],
            'date_column': table_info['date_where'],
            'last_update': str(datetime.now()),
            'all_cols': table_info['all_columns']
        }
        OUT_PUT_PATH = f'{LOAD_PATH_ROOT}{select_table}/config.json'
        with open(OUT_PUT_PATH, 'w') as f:
            json.dump(out_put_config, f)
    # -----------------config---------------- #
    get_config()
    # -----------------transform---------------- #
    return trans_schema(trans_hdf(trans_dtype(df_chunk)))


@task
def load_sql_query(xcom_dict: dict) -> dict:
    """
    根据sql查询语句下载数据到本地
    :return:xcom_dict
    """
    # -----------------参数传递----------------- #
    print(xcom_dict)
    print('\n# -----------------参数传递----------------- #\n')
    connector_id = xcom_dict['connector_id']
    query_sql = xcom_dict['query_sql']
    select_table = xcom_dict['select_table']
    load_date = xcom_dict['load_date']
    dynamic = xcom_dict['dynamic']
    date_column = xcom_dict['date_column']

    # -----------------数据库接口---------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # ----------------------------------大表分片保存---------------------------------- #
    # get_pandas_df   连接suntime会报错
    chunk_count = 0
    for df_chunk in sql_hook.get_pandas_df_by_chunks(
            query_sql,
            chunksize=1000000,
    ):
        # 无数据跳出
        if df_chunk.empty:
            LOAD_PATH = ''
            break

        # ----------------- 命名----------------- #
        if dynamic:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{select_table}/{load_date}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{select_table}/{load_date}_{chunk_count}.parquet'
        else:
            LOAD_PATH = LOAD_PATH_ROOT + \
                f'{select_table}/{select_table}.parquet' if chunk_count == 0 else LOAD_PATH_ROOT + \
                f'{select_table}/{select_table}_{chunk_count}.parquet'

        # -----------------输出文件----------------- #
        import os
        import pyarrow.parquet as pq
        _ = os.mkdir(LOAD_PATH_ROOT +
                     select_table) if not os.path.exists(LOAD_PATH_ROOT +
                                                         select_table) else None
        # -----------------转换---------------- #
        pa_table = transform(df_chunk, select_table)
        pq.write_table(pa_table, LOAD_PATH)
        chunk_count += 1
    return {'table_path': LOAD_PATH, 'select_table': select_table,
            'table_empty': df_chunk.empty, 'dynamic': dynamic, 'date_column': date_column, }


@dag(
    default_args={'owner': 'zirui',
                  # '821538716@qq.com'
                  'email': ['523393445@qq.com', '821538716@qq.com', ],
                  'email_on_failure': True,
                  'email_on_retry': True,
                  #   'retries': 1,
                  "retry_delay": timedelta(minutes=1), },
    schedule="0/30 2-4 * * 1-7",
    start_date=pendulum.datetime(2022, 9, 1, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['数据加载'],
)
def csc_data_load():
    def start_tasks(table_name: str):
        """
        任务流控制函数，用于被多进程调用，每张表下载都是一个并行的进程
        :return:
        """
        # 下载昨天的数据
        load_date = (date.today() + timedelta(-1)).strftime('%Y%m%d')

        # ETL
        load_sql_query.override(
            task_id='L_' + table_name, )(
                extract_sql_by_table.override(task_id='E_' + table_name, )(table_name, load_date))

    # 多进程异步执行
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        _ = {executor.submit(start_tasks, table)             : table for table in TABLE_LIST}


csc_data_load()
