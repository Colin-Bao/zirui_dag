# @Time      :2022-09-28 15:49:50
# @Author    :Colin
# @Note      :新的合并算法
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import dag, task
from airflow.models import Variable


# Define datasets

# TODO 已经内置了邮件处理函数
# TODO 动态映射,自定义task,自定义dag
# TODO 生成多个DAG用于处理


def get_map_tables() -> dict:
    """
    获取上游数据表,生成下游动态DAG
    """
    import json
    map_dict = json.loads(Variable.get("csc_map_dict"))
    dag_configs = {}
    for csc_table, db in map_dict.items():
        table_list = []
        for v in db.values():
            table_list += v.keys()
        dag_configs.update(
            {csc_table: [Dataset('L_'+i) for i in table_list]})  # 完成Load后的操作
    return dag_configs


dag_configs = get_map_tables()


# 为数据集生成动态dag
for dag_name, tables in dag_configs.items():
    dag_id = f"M_{dag_name}"

    @dag(
        default_args={'owner': 'zirui', },
        dag_id=dag_id,
        start_date=datetime(2022, 2, 1),
        schedule=tables,
        tags=['数据运维', '数据合并']
    )
    # 合并任务
    def dynamic_generated_dag():
        @task
        def print_message():
            print()
        print_message()
    dynamic_generated_dag()
