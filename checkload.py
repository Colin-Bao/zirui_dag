
import os
import time
from airflow.models import Variable
from datetime import timedelta, date, datetime
from retry import retry
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq

LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load/'
# 检查是否可以读取


def check_dataset():
    no_use_list = []
    for table in [
            LOAD_PATH_ROOT + i + '/' for i in os.listdir(LOAD_PATH_ROOT)
    ]:
        try:
            df_chunk = pq.ParquetDataset(table).read().to_pandas()
            # df_chunk.info()
        except Exception as e:
            print(table, e)
            no_use_list.append(table)
            continue
    print([i.split(LOAD_PATH_ROOT)[-1] for i in no_use_list])


check_dataset()
