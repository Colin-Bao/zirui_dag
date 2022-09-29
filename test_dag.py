import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG, Dataset
from airflow.models import Variable
from datetime import datetime, timedelta, date

OUTPUT_PATH = Variable.get("csc_load_path")

from csc_ops_load import csc_ops_load
# load_sql_query('','')

