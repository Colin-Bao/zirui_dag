

# 格式转换
def dict_to_json(old_dict, name):
    import json
    with open(f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/sql_files/{name}.json', 'w') as json_file:
        json_file.write(json.dumps(old_dict, ensure_ascii=False))


def get_sql_by_table(table_name) -> str:
    from wind_sql import sql_sentence

    return sql_sentence[table_name]


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


merge_json()
