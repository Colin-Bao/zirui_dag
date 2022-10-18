import os
import time
from airflow.models import Variable
from datetime import timedelta, date, datetime
from retry import retry
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq

LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load_new/'


def check_all_files():
    for i in os.listdir(LOAD_PATH_ROOT):
        LOAD_PATH_ROOT+i+'.parquet'
        df_chunk = pq.ParquetDataset(LOAD_PATH_ROOT+i).read().to_pandas()
        print(i, 'success')


def check_dataset():
    table_list = ['AINDEXHS300CLOSEWEIGHT/',
                  'ASHARETRADINGSUSPENSION/',

                  ]
    for i in table_list:
        df_chunk = pq.ParquetDataset(LOAD_PATH_ROOT+i).read().to_pandas()
        print(df_chunk.dtypes)


check_all_files()
# check_dataset()
