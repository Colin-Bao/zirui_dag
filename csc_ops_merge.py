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

# 根据表名获取数据库信息


def get_db_by_table(table_name: str) -> str:
    import json
    return json.loads(Variable.get("csc_table_db"))[table_name]


# 根据表名获取需要的字段信息
def get_transform_info_by_table(csc_name: str, table_name: str) -> dict:
    import json
    db = get_db_by_table(table_name)
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
        def transform_table(csc_name, table_name) -> str:
            # 昨天的日期 TODO DAG之间传递参数
            load_date = (date.today()+timedelta(-1)
                         ).strftime('%Y%m%d')  # 昨天的数据

            # 获得该表的信息
            table_info_dict = get_transform_info_by_table(csc_name, table_name)
            target_column = [i.upper()
                             for i in table_info_dict['target_column']]
            code_column = table_info_dict['code_column'].upper(
            )
            date_column = table_info_dict['date_column'].upper(
            )
            index_column = table_info_dict['index_column']

            # 读取文件
            LOAD_PATH = Variable.get(
                "csc_load_path")+table_name+f'/{load_date}.csv'
            import pandas as pd
            df_table = pd.read_csv(LOAD_PATH, usecols=target_column)

            # ------------------转换----------------------#
            if csc_name in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
                db = get_db_by_table(table_name)
                if db == 'wind':
                    # wind的股票代码有后缀，suntime没有
                    df_table[code_column] = df_table[code_column].str[:-3]
                    report_type = index_column[1]  # index写好了
                    df_table[report_type] = df_table[report_type].astype('str').replace(
                        {'408001000': '合并报表', '408006000': '母公司报表', '408004000': '合并报表调整', '408009000': '母公司报表调整', })

                elif db == 'suntime':
                    df_table[date_column] = df_table[date_column].str.replace(
                        '-', '', )  # suntime的日期带有-
                    report_type = index_column[1]  # index写好了
                    report_year = index_column[3]  # index写好了

                    df_table[report_type] = df_table[report_type].astype('str').replace(
                        {'1001': '合并报表', '1002': '母公司报表', '1003': '合并报表调整', '1004': '母公司报表调整', })

                    df_table[report_year] = df_table[report_year].astype(
                        'str')+'1231'
                else:
                    # TODO
                    pass

            # ------------------储存并输出----------------------#
            TRANSFORM_PATH = Variable.get("csc_transform_path") + csc_name
            import os
            if not os.path.exists(TRANSFORM_PATH):
                os.mkdir(TRANSFORM_PATH)
            output_path = TRANSFORM_PATH+f'/{table_name}.csv'
            df_table.to_csv(output_path, index=False)
            return TRANSFORM_PATH

        @task
        def merge_table(csc_name, tables, table_path) -> str:
            if len(tables) > 2:
                # TODO 3表的没有做
                return None
            # 1.从第一个表获取所有的索引字段
            import pandas as pd
            table_info_dict = get_transform_info_by_table(csc_name, tables[0])
            index_column = table_info_dict['index_column']
            index_df = pd.DataFrame(
                columns=index_column).set_index(index_column)

            # 2.遍历表
            df_alldbs = []  # 不同数据源
            for i, table in enumerate(tables):
                table_index = get_transform_info_by_table(
                    csc_name, table)['index_column']
                # 根据路径读取df
                df_table = pd.read_csv(
                    table_path[i]+f'/{table}.csv', dtype={j: 'str' for j in table_index})
                # 设置索引
                df_table.set_index(table_index, inplace=True)
                # 自身索引去重
                df_table = df_table[~df_table.index.duplicated()]
                # 追加df
                df_alldbs.append(df_table)
                # 追加索引
                index_df.index = index_df.index.append(
                    df_table.index)

            # 3.迭代join
            # 索引去重
            index_df.index = index_df.index.drop_duplicates()

            # 分别join
            # TODO 只做了2源的，如果多个要拓展
            df_wind = index_df.join(
                df_alldbs[0], how='left', on=index_df.index.names)
            df_suntime = index_df.join(
                df_alldbs[1], how='left', on=index_df.index.names)

            # 5.对比
            # TODO 只做了2个的,如果要做2个以上的需要自己写一个对比+合并函数
            # print(self.CSC_MERGE_TABLE,)
            assert df_wind.shape == df_suntime.shape

            df_suntime.columns = df_wind.columns
            df_compare = df_wind.compare(
                df_suntime, keep_shape=True, keep_equal=True)

            # 改名字
            df_compare.columns = [f'{i[1]}_{i[0]}'.replace('self', 'wind').replace('other', 'suntime')
                                  for i in df_compare.columns]

            # 6.输出
            output_path = Variable.get("csc_merge_path") + csc_table+'.csv'
            df_compare.sort_index().reset_index().to_csv(output_path, index=False)
            return output_path

        @task
        def check_table(csc_name, table_path):
            import pandas as pd
            df_compare = pd.read_csv(table_path)
            # print(df_compare.columns, df_compare)
            # 逐字段打印出缺失率
            # attr1 = df_compare[pd.isnull(df_compare['wind_ADV_FROM_CUST']) & pd.notnull(
            # df_compare['suntime_ADV_FROM_CUST'])]
            # 统计缺失率
            df_null_count = (df_compare.isnull().sum()/len(df_compare))
            df_null_count = df_null_count.drop(
                [i for i in df_null_count.index if 'suntime_' in i]).sort_values(ascending=False)
            # print(df_null_count)

            # 输出
            output_path = Variable.get(
                "csc_merge_path") + f"{csc_name}_COUNT.csv"
            df_null_count.reset_index().to_csv(output_path, index=False)

        table_path = []
        # 1.转换为可比的表
        for table in tables:
            res = transform_table.override(
                task_id='T_'+table)(csc_table, table)
            table_path.append(res)
        # 2.合并
        merge_path = merge_table.override(
            task_id='M_'+csc_table, outlets=[Dataset('M_'+csc_table)])(csc_table, tables, table_path,)
        # 3.检查
        check_table.override(
            task_id='C_'+csc_table, outlets=[Dataset('C_'+csc_table)])(csc_table, merge_path)

    dynamic_generated_mergedag()
