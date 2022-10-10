from airflow.hooks.base import BaseHook
import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.models.xcom import XCom


# @dag(
#     default_args={'owner': 'zirui',
#                   'email': ['523393445@qq.com'],
#                   'email_on_failure': True, },
#     start_date=datetime(2022, 2, 1),
#     schedule=None,
#     tags=['数据运维', '测试用例']
# )
# def email_dag():

#     @task()
#     def task_info(msg):
#         send_email()

#     task_info('ok')
#     # EmailOperator(
#     #     task_id='send_email',
#     #     to='52393445@qq.com',
#     #     subject='Airflow Alert',
#     #     html_content=""" <h3>Email Test</h3> """,

#     # )


# # send_email(to='523393445@qq.com', subject='ok', html_content='s')
# # conn = BaseHook.get_connection('sendgrid_default')
# # sendgrid_client = sendgrid.SendGridAPIClient(api_key=conn.password)

# # print(sendgrid_client)
# email_dag()
