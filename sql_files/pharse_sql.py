from wind_sql import sql_sentence


for k, v in sql_sentence.items():
    print(k)


# 格式转换
def dict_to_json(old_dict):
    import json
    with open('/sql_files/wind_sql.json', 'w') as json_file:
        json_file.write(json.dumps(old_dict, ensure_ascii=False))
