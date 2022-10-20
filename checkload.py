import os
import time
from airflow.models import Variable
from datetime import timedelta, date, datetime
from retry import retry
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq

LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load_check/'


def check_all_files():
    for i in os.listdir(LOAD_PATH_ROOT):
        try:
            LOAD_PATH_ROOT+i+'.parquet'
            df_chunk = pq.ParquetDataset(LOAD_PATH_ROOT+i).read().to_pandas()
            df_chunk.to_hdf('test.h5', 'data')
            df_hdf = pd.read_hdf('test.h5')
            # obj_columns = list(
            #     df_hdf.select_dtypes(include=['int']).columns.values)
            # print(df_hdf.dtypes, df_hdf[obj_columns])
            print(i, 'success')
        except Exception as e:
            print(i, 'fail', e)


def check_dataset():
    table_list = ['AINDEXCSI500WEIGHT',


                  ]
    for i in table_list:
        df_chunk = pq.ParquetDataset(LOAD_PATH_ROOT+i).read().to_pandas()
        print(df_chunk['EXCHANGE'], df_chunk.dtypes)

# AINDEXHS300FREEWEIGHT


def check_hdf5():
    table_list = ['AINDEXHS300FREEWEIGHT',
                  ]
    for i in table_list:
        df = pq.ParquetDataset(LOAD_PATH_ROOT+i).read().to_pandas()
        # print(df_chunk['EXCHANGE'], df_chunk.dtypes)
        df.to_hdf('test.h5', 'data')
    df_hdf = pd.read_hdf('test.h5')
    obj_columns = list(
        df_hdf.select_dtypes(include=['int']).columns.values)
    print(df_hdf.dtypes,
          df_hdf[obj_columns])


def check_dt():
    df_dt = pd.to_datetime(1641408432, unit='s')
    print(df_dt)


check_hdf5()
# check_all_files()
# check_dt()
