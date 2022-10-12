import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.models.param import Param


@dag(
    default_args={'owner': 'zirui', },
    params={"msg": Param("Please Use Upper Table Name", type="string"),
            "start_date": Param(20220301, type="integer", minimum=20211231, maximum=20221231),
            "end_date": Param(20220302, type="integer", minimum=20211231, maximum=20221231),
            "table_list": Param(
                ["ASHAREBALANCESHEET", "FIN_BALANCE_SHEET_GEN"],
                type="array",
                items={"type": "string"},
                minLength=1,
                maxLength=10,
    ),
        "update_mode": Param("overwrite", enum=["overwrite", "other"])
    },
    start_date=datetime(2022, 2, 1),
    schedule=None,
    tags=['数据更新', '参数触发']
)
def csc_ops_update():

    @task
    def get_data_list(params=None) -> list:
        """
        从传入的开始和截止日期生成序列
        """
        date_list = [str(i) for i in range(
            params['start_date'], params['end_date'] + 1)]
        return date_list

    @task
    def get_table_list(params=None) -> list:
        return params['table_list']

    # 任务流
    import sys
    sys.path.append(Variable.get('csc_zirui_dag'))  # 导入包
    from csc_ops_load import extract_sql_by_table, load_sql_query
    load_sql_query.expand(data_dict=extract_sql_by_table.expand(table_name=get_table_list(),
                                                                load_date=get_data_list())
                          )


csc_ops_update()
