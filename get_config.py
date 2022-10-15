from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from airflow.models import Variable
import pandas as pd
import os
from retry import retry


def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句,没有where就跳过
    :return:( connector_id, return_sql, table_name, load_date)
    """

    import json
    # 查找属于何种数据源
    db = json.loads(Variable.get("csc_table_db"))[table_name]  # 去数据字典文件中寻找

    # 不同数据源操作
    if db == 'wind':
        wind_sql = json.loads(Variable.get("csc_wind_sql"))[table_name]
        dynamic = True if wind_sql.count('where') == 1 else False
        return_sql = wind_sql % f"\'{load_date}\'" if dynamic else wind_sql
    elif db == 'suntime':
        suntime_sql_dict = json.loads(Variable.get("csc_suntime_sql"))
        suntime_sql = suntime_sql_dict[table_name]['sql'] if table_name in suntime_sql_dict.keys(
        ) else "SELECT * FROM %s"
        suntime_sql = "SELECT * FROM %s" if suntime_sql == "" else suntime_sql  # suntime_sql可能为空
        dynamic = True if suntime_sql.count('WHERE') == 1 else False
        return_sql = suntime_sql % (
            'zyyx.' + table_name, load_date) if dynamic else suntime_sql % ('zyyx.' + table_name)

    return {'connector_id': db + '_af_connector', 'query_sql': return_sql, 'table_name': table_name,
            'load_date': load_date, 'dynamic': dynamic}


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

    # -----------------输出文件的路径----------------- #
    LOAD_PATH = Variable.get(load_path) + table_name

    # ----------------- 从Airflow保存的connection获取多数据源连接----------------- #
    from airflow.providers.common.sql.hooks.sql import BaseHook  # airflow通用数据库接口
    sql_hook = BaseHook.get_connection(connector_id).get_hook()

    # -----------------df执行sql查询,保存文件----------------- #
    import os
    if not os.path.exists(LOAD_PATH):
        os.mkdir(LOAD_PATH)

    #
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

            # 保存
            if not df_chunk.empty:
                df_chunk.to_parquet(table_path, engine='pyarrow')
                # 传出格式 第一次运行的时候要用
                # get_type_from_df(table_name, df_chunk, table_path)

            return {'table_path': table_path, 'table_name': table_name,
                    'table_empty': df_chunk.empty, 'query_sql': query_sql,
                    'type_dict_path': LOAD_PATH+f'/type_config.json', 'dynamic': dynamic}
        else:
            # TODO 只保存chunksize行，如果超过chunksize行要分片保存再合并
            print(table_name, "超过chunksize行")
            break
        # chunk_count += 1

# 取出来df的类型


def get_type_from_df(table_name: str, df_by_sql: pd.DataFrame, parquet_path: str) -> dict:
    import pyarrow.parquet as pq
    pa_schema = pq.read_table(parquet_path).schema  # 取出schema

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
            if pandas_type == 'object':
                trans_schema_type = 'string'
            elif pandas_type == 'float64':
                trans_schema_type = 'double'

            else:  # 没有手动处理的 datetime64[ns]
                raise Exception('trans_schema_type is null')
        else:  # 不为空的自定义转换
            if pandas_type == 'datetime64[ns]':
                trans_schema_type = 'string'

        type_config.update(
            {field.name: {'pandas_type': pandas_type,
                          'pyarrow_type': pyarrow_type,
                          'parquet_type': trans_schema_type}})

    # 导出config
    import json
    CONFIG_PATH = Variable.get('csc_type_config_path')+f'{table_name}.json'
    with open(CONFIG_PATH, 'w') as f:
        json.dump(type_config, f)

    return type_config
    # 进行转换


def transform_type(xcom_dict: dict) -> dict:
    """
    根据数据类型字典转换parquet
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import json

    # 解析参数
    table_empty = xcom_dict['table_empty']
    if table_empty:
        return False
    parquet_path = xcom_dict['table_path']
    table_name = xcom_dict['table_name']
    pa_table = pq.read_table(parquet_path)  # 读取原始parquet

    # 读取type_dict_path
    CONFIG_PATH = Variable.get('csc_type_config_path')+f'{table_name}.json'
    with open(CONFIG_PATH) as json_file:
        type_dict = json.load(json_file)

    # 从type_dict_path修改schema类型
    schema_list = []
    for column_name, types in type_dict.items():
        schema_list += [(column_name, types['parquet_type'])]

    # 修改pa_table.schema
    schema_from_dict = pa.schema(schema_list)

    # print(len(schema_from_dict))
    # print(pa_table.schema)
    pa_table = pa_table.cast(schema_from_dict)

    # 输出文件 覆盖
    pq.write_table(pa_table, parquet_path)

    # 读取文件 已验证pandas和pyarrow读取方式
    # import pandas as pd
    # pd.read_parquet(parquet_path)
    # new_table = pq.read_table(parquet_path)
    # print(new_table)
    # print(pd.read_parquet(parquet_path))
    # 输出日志
    # get_col_from_dict(xcom_dict['table_name'])

    # 返回值
    return xcom_dict


def get_col_from_dict(table_name):
    """
    得到鹏队要的config表
    """
    import json
    from datetime import datetime
    # 字典文件路径
    data_dict = Variable.get('csc_data_dict_path') + table_name+'.csv'
    if not os.path.exists(data_dict):  # 没有数据字典的处理
        return
    else:
        df_dict = pd.read_csv(Variable.get(
            'csc_data_dict_path') + table_name+'.csv')[['fieldName', 'fieldType']]

    # 读取type_dict_path
    CONFIG_PATH = Variable.get('csc_type_config_path')+f'{table_name}.json'
    with open(CONFIG_PATH) as json_file:
        type_dict = json.load(json_file)

    for col in type_dict.keys():

        target_column = df_dict[df_dict['fieldName'] == col]
        if target_column.empty:
            original_type = 'VARCHAR(30)'
        else:
            original_type = target_column['fieldType'].iloc[0]

        type_dict[col].update({'original_type': original_type})
        del type_dict[col]['pandas_type'], type_dict[col]['pyarrow_type']
    # 自定义增加
    config_dict = {}
    config_dict.update({"primary_key": [],
                        "dynamic": True,
                        "date_column": "ANN_DT",
                        "last_update": str(datetime.now()),
                        "all_columns": type_dict})
    # 输出config
    with open(Variable.get('csc_load_path')+f'{table_name}/config.json', 'w') as f:
        json.dump(config_dict, f)


def start_tasks(table_name):

    import warnings
    warnings.filterwarnings('ignore')  # 忽略警告
    """
    任务流验证
    """
    pd_dates = [str(i).replace('-', '').split(' ')[0]
                for i in pd.date_range('20220101', '20221015')]

    @retry(exceptions=Exception, tries=10, delay=1)
    def down_by_date(down_date):
        # print(table_name)
        res_extract = extract_sql_by_table(table_name, down_date)
        # print(res_extract, date)
        transform_type(load_sql_query(res_extract))

    # get_col_from_dict(xcom_dict['table_name'])
    for date in pd_dates:
        table_path = Variable.get(
            'csc_load_path')+f'{table_name}/{date}.parquet'
        table_path_2 = Variable.get(
            'csc_load_path')+f'{table_name}/{table_name}.parquet'
        if os.path.exists(table_path):
            # print('exists table_path_1')
            continue
        elif os.path.exists(table_path_2):
            # print('exists table_path_2')
            break
        down_by_date(date)  # 某一天出错重试
    # 所有日期下载完以后
    get_col_from_dict(table_name)

    return table_name, 'task ok'


def download_by_multi():
    """
    多进程
    """
    table_list = [
        'FIN_BALANCE_SHEET_GEN', 'ASHAREBALANCESHEET', 'ASHARECASHFLOW', 'FIN_CASH_FLOW_GEN',
        'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
        'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
        'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION', ]

    new_list_wind = ['ASHAREFINANCIALDERIVATIVE', 'ASHARESALESSEGMENT', 'ASHAREANNFINANCIALINDICATOR',
                     'AINDEXEODPRICES', 'AINDEXFREEWEIGHT', 'AINDEXMEMBERS', 'AINDEXMEMBERSCITICS', 'ASHAREBALANCESHEET',
                     'ASHAREBLOCKTRADE', 'ASHARECALENDAR', 'ASHARECASHFLOW', 'ASHARECONSENSUSDATA', 'ASHARECONSENSUSROLLINGDATA',
                     'ASHAREDESCRIPTION',
                     'ASHAREEODDERIVATIVEINDICATOR', 'ASHAREEODPRICES', 'ASHAREFINANCIALINDICATOR', 'ASHAREINCOME',
                     'ASHAREINDUSTRIESCLASS_CITICS',
                     'ASHAREINDUSTRIESCLASS_CS', 'ASHAREINDUSTRIESCLASS_GICS', 'ASHAREINDUSTRIESCLASS_SW',
                     'ASHAREINDUSTRIESCLASS_WIND', 'ASHAREMONEYFLOW',
                     'ASHAREPROFITEXPRESS', 'ASHAREPROFITNOTICE', 'ASHAREST', 'ASHARETRADINGSUSPENSION',
                     'CCOMMODITYFUTURESEODPRICES', 'CCOMMODITYFUTURESPOSITIONS', 'CFUTURESCALENDAR', 'CFUTURESCONTPRO',
                     'CFUTURESCONTPROCHANGE', 'CFUTURESDESCRIPTION', 'CFUTURESMARGINRATIO', 'SHSCMEMBERS', 'SZSCMEMBERS',
                     'CHINAMUTUALFUNDSTOCKPORTFOLIO', 'ASHAREMANAGEMENTHOLDREWARD', 'ASHAREDIVIDEND',
                     'AINDEXCSI500WEIGHT', 'AINDEXHS300CLOSEWEIGHT', 'SHSCCHANNELHOLDINGS', 'ASHAREISACTIVITY',
                     'ASHARECONSEPTION', 'ASHAREPLANTRADE', 'ASHAREIPO', 'ASHAREEARNINGEST', 'ASHAREAUDITOPINION',
                     'ASHAREMAJOREVENT', 'ASHAREREGINV']

    new_list_suntime = ['RPT_FORECAST_STK', 'RPT_RATING_ADJUST', 'RPT_TARGET_PRICE_ADJUST', 'RPT_EARNINGS_ADJUST', 'RPT_GOGOAL_RATING',
                        'CON_FORECAST_STK', 'CON_RATING_STK', 'CON_TARGET_PRICE_STK', 'CON_FORECAST_ROLL_STK',
                        'DER_FORECAST_ADJUST_NUM', 'DER_RATING_ADJUST_NUM', 'DER_REPORT_NUM', 'DER_CONF_STK', 'DER_FOCUS_STK',
                        'DER_DIVER_STK', 'DER_CON_DEV_ROLL_STK', 'DER_EXCESS_STOCK', 'DER_PROB_EXCESS_STOCK', 'DER_PROB_BELOW_STOCK']
    test_list = ['RPT_TARGET_PRICE_ADJUST',
                 'RPT_EARNINGS_ADJUST', 'RPT_GOGOAL_RATING', 'DER_FORECAST_ADJUST_NUM']

    all_table = new_list_wind+new_list_suntime

    # get_col_from_dict(xcom_dict['table_name'])

    with ThreadPoolExecutor(max_workers=10) as executor:
        result = {executor.submit(
            start_tasks, table): table for table in all_table}
        for future in as_completed(result):
            data = future.result()
            print(data)

            #     _ = {executor.submit(start_tasks, table): table for table in test_list}


st = time.time()
download_by_multi()
et = time.time()
print(et-st)
