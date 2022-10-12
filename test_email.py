# import sendgrid
from datetime import datetime
from airflow.hooks.base import BaseHook
import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.datasets import Dataset


# from airflow.providers.sendgrid.utils.emailer import send_email
def success_func():
    from airflow.operators.email import EmailOperator
    email = EmailOperator(
        task_id='send_email',
        to='to@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
    )


@dag(
    default_args={'owner': 'zirui',
                  'email': ['523393445@qq.com'],
                  'email_on_failure': True,
                  'email_on_retry': True,
                  # 'retries': 1,
                  },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据运维', '测试用例']
)
def email_dag():
    @task
    def get_info(msg) -> dict[str, str]:
        return {'subject': 'Airflow Alert', 'html_content': f""" <h3>Email Test</h3> {msg}"""}

    def send_email(data_dict: dict):
        """
        发送邮件
        """
        from airflow.operators.email import EmailOperator
        EmailOperator(
            task_id='send_info',
            to='523393445@qq.com',
            subject=data_dict['subject'],
            html_content=data_dict['html_content'],
        )

    send_email(get_info('ok'))


# # send_email(to='523393445@qq.com', subject='ok', html_content='s')
# # conn = BaseHook.get_connection('sendgrid_default')
# # sendgrid_client = sendgrid.SendGridAPIClient(api_key=conn.password)
# # print(sendgrid_client)
# email_dag()
print(datetime.now().hour)
