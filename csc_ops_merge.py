# @Time      :2022-09-28 15:49:50
# @Author    :Colin
# @Note      :新的合并算法
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, date

# TODO 已经内置了邮件处理函数


# 获取上游数据表,生成下游动态DAG
def get_map_tables() -> dict[str, str]:
    import json
    map_dict = json.loads(Variable.get("csc_map_dict"))
    dag_configs = {}
    for csc_table, db in map_dict.items():
        table_list = []
        for v in db.values():
            table_list += v.keys()
        dag_configs.update({csc_table: table_list})  # 完成Load后的操作
    return dag_configs

# 根据表名获取需要的字段信息


def get_transform_info_by_table(csc_name: str, table_name: str) -> dict:
    import json
    db = json.loads(Variable.get("csc_table_db"))[table_name]
    map_dict = json.loads(Variable.get("csc_map_dict"))
    return map_dict[csc_name][db][table_name]


# 为数据集生成动态dag
for csc_table, tables in get_map_tables().items():  # csc_table, tables作为传入参数给dag
    @dag(
        default_args={'owner': 'zirui', },
        dag_id=csc_table.lower()+"_merge",
        start_date=datetime(2022, 2, 1),
        schedule=[Dataset('L_'+i) for i in tables],
        tags=['数据运维', '数据合并']
    )
    # 动态生成合并DAG执行合并任务
    def dynamic_generated_mergedag():
        # 合并任务 - 根据不同的名称选择不同的操作
        @task
        def transform_table(csc_name, table_name) -> dict[str, str]:
            # 昨天的日期 TODO DAG之间传递参数
            load_date = (date.today()+timedelta(-1)
                         ).strftime('%Y%m%d')  # 昨天的数据

            # 获得该表的信息
            table_info_dict = get_transform_info_by_table(csc_name, table_name)

            # 读取文件
            TABLE_PATH = Variable.get(
                "csc_load_path")+table_name+f'/{load_date}.csv'
            import pandas as pd
            df_table = pd.read_csv(
                TABLE_PATH, columns=table_info_dict['target_column'])

            # 转换
            return {}

        # 1.转换为可比的表
        for table in tables:
            transform_table(csc_table, table)

    dynamic_generated_mergedag()
