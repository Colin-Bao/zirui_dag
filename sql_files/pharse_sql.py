
from airflow.models import Variable
# 格式转换
import json


def dict_to_json(old_dict, name):
    import json
    with open(f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/sql_files/{name}.json', 'w') as json_file:
        json_file.write(json.dumps(old_dict, ensure_ascii=False))


def wind_sql():
    from wind_sql import sql_sentence
    wind_sql_dict = {k.upper(): v.replace('\n      ', '').replace(
        '\n', '').replace('    ', '') for k, v in sql_sentence.items()}
    dict_to_json(wind_sql_dict, 'wind_sql')


def get_sql_by_table(table_name) -> str:
    from wind_sql import sql_sentence

    return sql_sentence[table_name]


# 提取suntime所有的主键
def merge_json():
    import json
    with open('sql_files/suntime_bk_sql' + '.json') as f:  # 去数据字典文件中寻找
        suntime_tab = json.load(f)

    suntime_tab1 = suntime_tab['tables']

    with open('sql_files/suntime_bk_sql' + '.json') as f:  # 去数据字典文件中寻找
        suntime_tab2 = json.load(f)
    suntime_tab2 = suntime_tab2['tables']

    suntime_tab1.update(suntime_tab2)

    dict_to_json(suntime_tab1, 'suntime_sql_merge')

# 提取所有的表名与数据源关系


def merge_all():
    from wind_sql import sql_sentence
    wind_table = {k.upper(): 'wind' for k, _ in sql_sentence.items()}

    import json
    with open('sql_files/suntime_sql_merge' + '.json') as f:  # 去数据字典文件中寻找
        suntime_table = json.load(f)
    suntime_table = {k.upper(): 'suntime' for k, _ in suntime_table.items()}

    wind_table.update(suntime_table)

    dict_to_json(wind_table, 'all_table_db')


# merge_all()

def test_load_frome_json():
    suntime_sql = json.loads(Variable.get("csc_suntime_sql"))[
        'FIN_BALANCE_SHEET_GEN']['sql']  # 去数据字典文件中寻找
    suntime_sql = suntime_sql % (
        'zyyx.'+'FIN_BALANCE_SHEET_GEN', f"{'202202'}") if suntime_sql else None
    print(suntime_sql)


# test_load_frome_json()
