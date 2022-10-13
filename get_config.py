

from airflow.models import Variable
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# df转换
def df_to_parquet(table_name: str, df_by_sql: pd.DataFrame):
    print(df_by_sql.dtypes)
    # 输出config文件
    info_dict = {'primary_key':  [], 'is_append': True,
                 'date_column': 'ANN_DT', 'last_update': '',
                 'all_columns': {}}
    # 字典文件路径
    df_dict = pd.read_csv(Variable.get(
        'csc_data_dict_path') + table_name+'.csv')[['fieldName', 'fieldType', 'isPrimarykey']]

    # 取出schema
    pa_schema = pa.Schema.from_pandas(df_by_sql)

    # print(pa.Table.to_pandas(pa_by_sql).dtypes)
    # 遍历df_by_sql中的所有字段
    for column_name in df_by_sql.columns:
        # 在数据字典中找字段信息
        target_column = df_dict[df_dict['fieldName'] == column_name]

        # 数据字典没有找到的默认值
        if target_column.empty:
            print(table_name)
            if column_name == 'OPDATE':
                fieldType, isPrimarykey = 'VARCHAR2', 'N'
            elif column_name == 'OPMODE':
                fieldType, isPrimarykey = 'NUMBER', 'N'
            else:
                fieldType, isPrimarykey = 'VARCHAR2', 'N'
        else:
            fieldType, isPrimarykey = target_column['fieldType'].iloc[0], target_column['isPrimarykey'].iloc[0]

        # 更新primary_key
        if isPrimarykey == 'Y':
            info_dict['primary_key'] += [column_name]

        # 更改类型
        fieldType_judge = fieldType.split('(')[0]
        if fieldType_judge in ['NUMBER', '']:

            # 修改dtypes
            df_by_sql[column_name] = df_by_sql[column_name].astype('float64')

            # 修改schema
            pa_schema = pa_schema.set(
                pa_schema.get_field_index(column_name), pa.field(column_name, pa.float64()))

            # 更新all_columns
            info_dict['all_columns'].update(
                {column_name: {'parquet_type': 'double', 'original_type': fieldType}})
        elif fieldType_judge in ['VARCHAR2', '']:
            # 修改dtypes
            df_by_sql[column_name] = df_by_sql[column_name].astype('str')

            # 修改类型
            pa_schema = pa_schema.set(
                pa_schema.get_field_index(column_name), pa.field(column_name, pa.string()))

            # 更新all_columns
            info_dict['all_columns'].update(
                {column_name: {'parquet_type': 'string', 'original_type': fieldType}})

    # 生成修改schema后的 Parquet 数据
    new_schema_table = pa.Table.from_pandas(
        df_by_sql, schema=pa_schema, safe=False)

    # 导出config
    import json
    with open('config.json', 'w') as f:
        json.dump(info_dict, f)

    # # 输出文件1
    pq.write_table(new_schema_table, f'table_name.parquet')

    # table = pa.Table.from_pandas(df, schema=schema)
    # read_check = pd.read_parquet(
    #     '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/20221012.parquet')


table_list = [
    'FIN_BALANCE_SHEET_GEN', 'ASHARECASHFLOW',
    'ASHAREINCOME', 'FIN_INCOME_GEN', 'ASHAREEODPRICES', 'QT_STK_DAILY', 'ASHAREEODDERIVATIVEINDICATOR',
    'ASHAREPROFITNOTICE', 'FIN_PERFORMANCE_FORECAST', 'ASHAREPROFITEXPRESS', 'FIN_PERFORMANCE_EXPRESS',
    'ASHAREDIVIDEND', 'ASHAREEXRIGHTDIVIDENDRECORD', 'BAS_STK_HISDISTRIBUTION']
for table in table_list:
    df_to_parquet(table, pd.read_csv(
        f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/load/{table}/20221012.csv'))
