

from concurrent.futures import ThreadPoolExecutor
from airflow.models import Variable
import pandas as pd
import os
# 1.对应好类型


def extract_sql_by_table(table_name: str, load_date: str) -> dict:
    """
    根据表名和日期返回sql查询语句,没有where就跳过
    :return:( connector_id, return_sql, table_name, load_date)
    """
    #
    dynamic = False
    # 查找属于何种数据源
    import json
    try:
        db = json.loads(Variable.get("csc_table_db"))[table_name]  # 去数据字典文件中寻找
    except KeyError as e:
        print(table_name, "csc_table_db", e)
        return False
    # return

    # 不同数据源操作
    return_sql = ''

    if db == 'wind':
        wind_sql = json.loads(Variable.get("csc_wind_sql"))[table_name]

        if wind_sql.count('where') == 1:
            dynamic = True
            return_sql = wind_sql % f"\'{load_date}\'"

        else:
            dynamic = False
            return_sql = wind_sql

    elif db == 'suntime':
        suntime_sql = ''
        # TODO 没有写增量表，需要增加逻辑判断
        try:
            suntime_sql = json.loads(Variable.get("csc_suntime_sql"))[
                table_name]['sql']  # 去数据字典文件中寻找
        except KeyError as e:
            print(table_name, 'suntime下未注册', e)
            dynamic = True
            return_sql = "SELECT * FROM zyyx.%s" % table_name

        if 'WHERE' in suntime_sql:
            dynamic = True
            return_sql = suntime_sql % (
                'zyyx.' + table_name, f"{load_date}")
        else:
            dynamic = False
            return_sql = "SELECT * FROM zyyx.%s" % table_name if suntime_sql == '' else suntime_sql % (
                'zyyx.' + table_name)

    return {'connector_id': db + '_af_connector', 'query_sql': return_sql, 'table_name': table_name,
            'load_date': load_date, 'dynamic': dynamic}

# 下载数据


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
            else:
                print('是空的', table_name)
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
    get_col_from_dict(xcom_dict['table_name'])

    # 返回值
    return xcom_dict


def get_col_from_dict(table_name):
    """
    得到鹏队要的config表
    """
    import json
    from datetime import datetime
    # 字典文件路径
    try:
        df_dict = pd.read_csv(Variable.get(
            'csc_data_dict_path') + table_name+'.csv')[['fieldName', 'fieldType']]
    except Exception as e:  # 没有数据字典的表
        print(e)
        return

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


def download_demo():
    """
    整个流程验证
    """
    table_list = [
        'FIN_BALANCE_SHEET_GEN', 'ASHAREBALANCESHEET', 'ASHARECASHFLOW', 'FIN_CASH_FLOW_GEN',
        'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
        'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
        'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION', ]

    # 新加的表验证

    new_list_wind = ['ASHAREFINANCIALDERIVATIVE', 'ASHARESALESSEGMENT', 'ASHAREANNFINANCIALINDICATOR',
                     'AINDEXEODPRICES', 'AINDEXFREEWEIGHT', 'AINDEXMEMBERS', 'AINDEXMEMBERSCITICS', 'ASHAREBALANCESHEET',
                     'ASHAREBLOCKTRADE', 'ASHARECALENDAR', 'ASHARECASHFLOW', 'ASHARECONSENSUSDATA', 'ASHARECONSENSUSROLLINGDATA', 'ASHAREDESCRIPTION',
                     'ASHAREEODDERIVATIVEINDICATOR', 'ASHAREEODPRICES', 'ASHAREFINANCIALINDICATOR', 'ASHAREINCOME', 'ASHAREINDUSTRIESCLASS_CITICS',
                     'ASHAREINDUSTRIESCLASS_CS', 'ASHAREINDUSTRIESCLASS_GICS', 'ASHAREINDUSTRIESCLASS_SW', 'ASHAREINDUSTRIESCLASS_WIND', 'ASHAREMONEYFLOW',
                     'ASHAREPROFITEXPRESS', 'ASHAREPROFITNOTICE', 'ASHAREST', 'ASHARETRADINGSUSPENSION',
                     'CCOMMODITYFUTURESEODPRICES', 'CCOMMODITYFUTURESPOSITIONS', 'CFUTURESCALENDAR', 'CFUTURESCONTPRO', 'CFUTURESCONTPROCHANGE', 'CFUTURESDESCRIPTION', 'CFUTURESMARGINRATIO', 'SHSCMEMBERS', 'SZSCMEMBERS', 'CHINAMUTUALFUNDSTOCKPORTFOLIO', 'ASHAREMANAGEMENTHOLDREWARD', 'ASHAREDIVIDEND',
                     'AINDEXCSI500WEIGHT', 'AINDEXHS300CLOSEWEIGHT', 'SHSCCHANNELHOLDINGS', 'ASHAREISACTIVITY', 'ASHARECONSEPTION', 'ASHAREPLANTRADE', 'ASHAREIPO', 'ASHAREEARNINGEST', 'ASHAREAUDITOPINION', 'ASHAREMAJOREVENT', 'ASHAREREGINV']

    new_list_suntime = ['RPT_FORECAST_STK', 'RPT_RATING_ADJUST', 'RPT_TARGET_PRICE_ADJUST', 'RPT_EARNINGS_ADJUST', 'RPT_GOGOAL_RATING', 'CON_FORECAST_STK', 'CON_RATING_STK', 'CON_TARGET_PRICE_STK', 'CON_FORECAST_ROLL_STK',
                        'DER_FORECAST_ADJUST_NUM', 'DER_RATING_ADJUST_NUM', 'DER_REPORT_NUM', 'DER_CONF_STK', 'DER_FOCUS_STK', 'DER_DIVER_STK', 'DER_CON_DEV_ROLL_STK', 'DER_EXCESS_STOCK', 'DER_PROB_EXCESS_STOCK', 'DER_PROB_BELOW_STOCK']
    test_list = ['AINDEXFREEWEIGHT']
    # transform_type(load_sql_query(
    # extract_sql_by_table('ASHAREBALANCESHEET', '20220102')))
    all_table = table_list+new_list_wind+new_list_suntime

    def start_tasks(table_name):
        pd_dates = [str(i).replace('-', '').split(' ')[0]
                    for i in pd.date_range('20220101', '20221014')]

        for date in pd_dates:
            # print(date)
            table_path = Variable.get(
                'csc_load_path')+f'{table_name}/{date}.parquet'
            table_path_2 = Variable.get(
                'csc_load_path')+f'{table_name}/{table_name}.parquet'
            if os.path.exists(table_path):
                # print('exists')
                continue
            if os.path.exists(table_path_2):
                # print('exists')
                break

            res_extract = extract_sql_by_table(table_name, date)
            # print(res_extract, date, res_extract['dynamic'])
            res_trans = transform_type(load_sql_query(res_extract))

        # if res_trans:
            # get_col_from_dict(table_name)

    # _ = [start_tasks(table) for table in all_table]
    with ThreadPoolExecutor(max_workers=4) as executor:
        _ = {executor.submit(start_tasks, table): table for table in all_table}


download_demo()


# def df_to_parquet(table_name: str, df_by_sql: pd.DataFrame):
#     # 查找属于何种数据源
#     import json
#     db = json.loads(Variable.get("csc_table_db"))[table_name]  # 去数据字典文件中寻找

#     # 查找SQL语法
#     sql_str = ""
#     # 不同数据源操作
#     if db == 'wind':
#         sql_str = json.loads(Variable.get("csc_wind_sql"))[table_name]
#     elif db == 'suntime':
#         sql_str = json.loads(Variable.get("csc_suntime_sql"))[
#             table_name]['sql']  # 去数据字典文件中寻找
#     sql_str = sql_str.upper().split('WHERE')[-1]
#     date_column = sql_str.split('=')[0].strip()

#     # 输出config文件
#     info_dict = {'primary_key':  [], 'is_append': True,
#                  'date_column': date_column, 'last_update': '',
#                  'all_columns': {}}
#     # 字典文件路径
#     df_dict = pd.read_csv(Variable.get(
#         'csc_data_dict_path') + table_name+'.csv')[['fieldName', 'fieldType', 'isPrimarykey']]

#     # 取出schema
#     pa_schema = pa.Schema.from_pandas(df_by_sql)

#     # print(pa.Table.to_pandas(pa_by_sql).dtypes)
#     # 遍历df_by_sql中的所有字段
#     for column_name in df_by_sql.columns:
#         # 在数据字典中找字段信息
#         target_column = df_dict[df_dict['fieldName'] == column_name]

#         # 数据字典没有找到的默认值
#         if target_column.empty:

#             if column_name == 'OPDATE':
#                 fieldType, isPrimarykey = 'VARCHAR2', 'N'
#             elif column_name == 'OPMODE':
#                 fieldType, isPrimarykey = 'NUMBER', 'N'
#             else:
#                 fieldType, isPrimarykey = 'VARCHAR2', 'N'
#         else:
#             fieldType, isPrimarykey = target_column['fieldType'].iloc[0], target_column['isPrimarykey'].iloc[0]

#         # 更新primary_key
#         if isPrimarykey == 'Y':
#             info_dict['primary_key'] += [column_name]

#         # 更改类型
#         fieldType_judge = fieldType.split('(')[0]
#         if fieldType_judge in ['NUMBER', '']:

#             # 修改dtypes
#             df_by_sql[column_name] = df_by_sql[column_name].astype('float64')

#             # 修改schema
#             pa_schema = pa_schema.set(
#                 pa_schema.get_field_index(column_name), pa.field(column_name, pa.float64()))

#             # 更新all_columns
#             info_dict['all_columns'].update(
#                 {column_name: {'parquet_type': 'double', 'original_type': fieldType}})
#         elif fieldType_judge in ['VARCHAR2', '']:
#             # 修改dtypes
#             df_by_sql[column_name] = df_by_sql[column_name].astype('str')

#             # 修改schema类型
#             pa_schema = pa_schema.set(
#                 pa_schema.get_field_index(column_name), pa.field(column_name, pa.string()))

#             # 更新all_columns
#             info_dict['all_columns'].update(
#                 {column_name: {'parquet_type': 'string', 'original_type': fieldType}})

#     # 生成修改schema后的 Parquet 数据
#     new_schema_table = pa.Table.from_pandas(
#         df_by_sql, schema=pa_schema, safe=False)

#     # 导出config
#     import json
#     with open(f'config/{table_name}_config.json', 'w') as f:
#         json.dump(info_dict, f)

#     # # 输出文件1
#     pq.write_table(new_schema_table, f'parquet_demo/{table_name}.parquet')

#     # table = pa.Table.from_pandas(df, schema=schema)
#     # read_check = pd.read_parquet(
#     #     '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/20221012.parquet')


# table_list = [
#     'FIN_BALANCE_SHEET_GEN', 'ASHARECASHFLOW',
#     'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
#     'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
#     'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION']

# table_test = ['ASHARECASHFLOW']
# for table in table_list:
#     try:
#         df_to_parquet(table, pd.read_csv(
#             f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/load/{table}/20221012.csv'))
#         # break
#     except Exception as e:
#         print(table)
#         continue
