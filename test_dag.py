import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG, Dataset
from airflow.models import Variable
from datetime import datetime, timedelta, date

OUTPUT_PATH = Variable.get("csc_load_path")


# load_sql_query('','')
