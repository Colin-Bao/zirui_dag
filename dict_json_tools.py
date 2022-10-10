import pandas as pd

import json


def get_datadict_df(table_name: str):
    with open(f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/data_dict/json/{table_name}.json', ) as json_file:
        json_dict = json.load(json_file)
    df_datadict = pd.DataFrame(json_dict['fieldData'])
    # df_datadict = df_datadict[['fieldName', 'fieldChsName',
    #                            'fieldType', 'isPrimarykey', 'remark']]

    df_datadict.to_csv(
        f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/data_dict/{table_name}.csv', index=False)


def get_all_data_dict():
    import os
    filePath = '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/data_dict/json/'
    for i in os.listdir(filePath):
        #
        get_datadict_df(i.split('.')[0])


# 转换json为df
# get_all_data_dict()


def merge_dict(table_name: str, table_path: str):
    df_check = pd.read_csv(table_path)
    df_info = pd.DataFrame(
        {"column": df_check.columns,
         "null_ratio": df_check.isnull().sum() / df_check.count(),
         "type": df_check.dtypes})

    df_dict = pd.read_csv(
        f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/data_dict/{table_name}.csv')

    df_merge = pd.merge(df_info, df_dict, how='left',
                        left_on='column', right_on='fieldName')

    type_path = f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/load/{table_name}/'+table_path.split(
        '/')[-1].split('.')[0]+'_type.csv'

    df_merge.to_csv(type_path, index=False)


# merge_dict('ASHAREBALANCESHEET',
        #    '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output/load/ASHAREBALANCESHEET/20220929.csv')

df=pd.DataFrame(columns=['1','2'])
print(df.empty)