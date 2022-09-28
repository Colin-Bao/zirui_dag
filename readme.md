# 文件说明

af_sql_unit_test.py -> 单元测试类，没有作用
csc_etl_zirui.py -> 部署在airflow上的dag，用于流程控制
map_tables.py -> 实现流程控制中的输出sql查询语句、转换、合并算法
map_tables_same.json -> 静态文件,映射多源关系


输出结果 -> 1.按照表名和日期输出原始文件 2.按照表名和日期输出合并文件，以及合并后的错误检查文件

