from airflow.decorators import dag, task
import pendulum
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.bash import BashOperator


@task
def push(ti=None):
    """Pushes an XCom without a specific target"""
    ti.xcom_push(key='value from pusher 1', value=[1, 2, 3])


@task
def puller(ti=None):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""
    pulled_value_1 = ti.xcom_pull(task_ids="push", key="value from pusher 1")
    print('pulled_value_1', pulled_value_1)

    # _compare_values(pulled_value_1, value_1)
    # _compare_values(pulled_value_2, value_2)


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def task_ti():
    push() >> puller()


dag1 = task_ti()
