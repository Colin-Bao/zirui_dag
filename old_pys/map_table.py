#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :table_map.py
# @Time      :2022/9/7 09:02
# @Author    :Colin
# @Note      :None


import pandas as pd
import json

# AirFlow连接器的名称
AF_CONN = '_af_connector'
# 人工定义的数据映射字典
DICT_PATH = '/home/lianghua/rtt/soft/airflow/dags/zirui_dag/map_tables_same.json'


# 多源数据处理的类
class MapCsc:
    def __init__(self, csc_merge_table=None):
        # --------------配置文件-------------- #
        self.AF_CONN = AF_CONN  # 数据库连接器的名称,数据源+后缀命名
        with open(DICT_PATH) as f:
            self.MAP_DICT = json.load(f)  # 静态的字典文件
        # --------------要处理的数据-------------- #
        self.CSC_MERGE_TABLE = csc_merge_table  # 输入的CSC表名
        self.MULTI_MAP_TABLES = None  # 返回的CSC对应的所有表

        # 多数据源的字典
        self.MULTI_DB_DICT = self.MAP_DICT[csc_merge_table] if csc_merge_table else None
        self.DB_LIST = list(self.MULTI_DB_DICT.keys()
                            ) if csc_merge_table else None  # 数据库

        self.INDEX_COLUM = []  # 用于匹配的索引字段
        self.INDEX_DF = None  # 用于匹配的df
        self.ATTR_COLUMN = []  # 合并后新的字段名

    # 返回映射的表,格式化为SQL语句
    def get_map_tables(self) -> list:
        """
        :return:[( connector_id, table_name, column, date_column,code_column,column_len ),...]
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[self.CSC_MERGE_TABLE].keys()]:

            if db == 'wind':
                all_map_tables += [(db + self.AF_CONN, '[wande].[dbo].[' + table + ']',
                                    ','.join(
                                        list(map(lambda x: '[' + x + ']' if x != '*' else x, attr['target_column']))),
                                    attr['date_column'], attr['code_column'], len(attr['target_column']))
                                   for table, attr in self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]

            elif db == 'suntime':
                all_map_tables += [(db + self.AF_CONN, 'ZYYX.' + table + '',
                                    ','.join(
                                        list(map(lambda x: '' + x + '' if x != '*' else x, attr['target_column']))),
                                    attr['date_column'], attr['code_column'], len(attr['target_column']))
                                   for table, attr in self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]
            else:
                raise Exception

        self.MULTI_MAP_TABLES = all_map_tables
        return self.MULTI_MAP_TABLES

    # 返回sql语句
    # 从写好的文件从返回
    def get_map_tables_by_date(self,load_date)->list:
        """
        :return:[( connector_id, table_name, sql ),...]
        """
        all_map_tables = []
        for db in [i for i in self.MAP_DICT[self.CSC_MERGE_TABLE].keys()]:

            if db == 'wind':
                from sql_files.wind_sql import sql_sentence
                wind_sql_dict={k.upper():v.replace('\n      ','').replace('\n','') for k ,v in sql_sentence.items()}# 转成大写
                all_map_tables += [(db + self.AF_CONN,table,wind_sql_dict[table] % f"\'{load_date}\'")
                                   for table, _ in self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]

            elif db == 'suntime':
                # TODO 没有写增量表，需要增加逻辑判断
                import json
                with open('sql_files/suntime_sql_merge' + '.json') as f:
                    suntime_sql_dict = json.load(f)  # 去数据字典文件中寻找
                all_map_tables += [(db + self.AF_CONN, table, suntime_sql_dict[table]['sql']% (table,f"{load_date}") )
                                   for table, _ in self.MAP_DICT[self.CSC_MERGE_TABLE][db].items()]
                # print(suntime_sql_dict)

            else:
                raise Exception

        self.MULTI_MAP_TABLES = all_map_tables
        return self.MULTI_MAP_TABLES



    # 从外部更新多源数据
    def update_multi_data(self, db: str, table_name: str, file_name: str):
        # print(self.MULTI_DB_DICT[db])
        self.MULTI_DB_DICT[db][table_name].update({'table_df': file_name})

    # 初始化MULTI_TABLE_DICT
    def init_multi_data(self, MULTI_TABLE_DICT: dict):
        self.MULTI_DB_DICT = MULTI_TABLE_DICT

    # 转换
    def transform_df(self) -> dict:
        for db, db_tables in self.MULTI_DB_DICT.items():

            # ------------------逐表转换----------------------#
            for table in db_tables.keys():

                # 根据数据源或者表名转换数据
                code_column = self.MULTI_DB_DICT[db][table]['code_column'].upper(
                )
                date_column = self.MULTI_DB_DICT[db][table]['date_column'].upper(
                )
                index_column = self.MULTI_DB_DICT[db][table]['index_column']
                table_path = self.MULTI_DB_DICT[db][table]['table_df']

                # 根据路径读取df
                df_transform = pd.read_csv(
                    table_path, dtype={code_column: 'str', date_column: 'str'})

                if db == 'wind':

                    # wind的股票代码有后缀，suntime没有
                    df_transform[code_column] = df_transform[code_column].str[:-3]

                    # ------------------部分表的经济意义不一样----------------------#
                    if self.CSC_MERGE_TABLE in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
                        report_type = index_column[1]  # index写好了
                        df_transform[report_type] = df_transform[report_type].astype('str').replace(
                            {'408001000': '合并报表', '408006000': '母公司报表', '408004000': '合并报表调整', '408009000': '母公司报表调整', })

                elif db == 'suntime':
                    # suntime的日期带有-
                    df_transform[date_column] = df_transform[date_column].str.replace(
                        '-', '', )

                    # ------------------部分表的经济意义不一样----------------------#
                    if self.CSC_MERGE_TABLE in ['CSC_Balance_Sheet', 'CSC_CashFlow', 'CSC_Income']:
                        report_type = index_column[1]  # index写好了
                        report_year = index_column[3]  # index写好了

                        df_transform[report_type] = df_transform[report_type].astype('str').replace(
                            {'1001': '合并报表', '1002': '母公司报表', '1003': '合并报表调整', '1004': '母公司报表调整', })

                        df_transform[report_year] = df_transform[report_year].astype(
                            'str')+'1231'
                # ------------------储存并输出----------------------#
                table_path_transform = table_path.replace(
                    '.csv', '_transform.csv')
                self.MULTI_DB_DICT[db][table].update(
                    {'table_df': table_path_transform})
                df_transform.to_csv(table_path_transform, index=False)

        return self.MULTI_DB_DICT

    # 合并
    def merge_df(self) -> dict:
        # 1.从第一个表获取所有的索引字段
        self.INDEX_COLUM = self.MULTI_DB_DICT['wind'][list(
            self.MULTI_DB_DICT['wind'].keys())[0]]['index_column']
        self.INDEX_DF = pd.DataFrame(
            columns=self.INDEX_COLUM).set_index(self.INDEX_COLUM)
        # print(self.INDEX_DF.index)

        # 2.遍历表
        df_alldbs = []  # 不同数据源
        for db, db_tables in self.MULTI_DB_DICT.items():
            # ------------------逐表转换----------------------#
            # TODO 如果同源多表，要写一个concat
            for table in db_tables.keys():
                # 根据数据源或者表名转换数据
                index_column = self.MULTI_DB_DICT[db][table]['index_column']
                table_path = self.MULTI_DB_DICT[db][table]['table_df']
                # 根据路径读取df
                df_table = pd.read_csv(
                    table_path, dtype={i: 'str' for i in index_column})
                # print(df_table, table_path)
                # df_table.loc[:, index_column]
                # 设置索引
                df_table.set_index(index_column, inplace=True)
                # 自身索引去重
                df_table = df_table[~df_table.index.duplicated()]
                # 追加df
                df_alldbs.append(df_table)
                # 追加索引
                self.INDEX_DF.index = self.INDEX_DF.index.append(
                    df_table.index)

        # 4.迭代join
        # 索引去重
        self.INDEX_DF.index = self.INDEX_DF.index.drop_duplicates()
        # 分别join
        # TODO 只做了2源的，如果多个要拓展
        df_wind = self.INDEX_DF.join(
            df_alldbs[0], how='left', on=self.INDEX_DF.index.names)
        df_suntime = self.INDEX_DF.join(
            df_alldbs[1], how='left', on=self.INDEX_DF.index.names)

        # 5.对比
        # TODO 只做了2个的,如果要做2个以上的需要自己写一个对比+合并函数
        # print(self.CSC_MERGE_TABLE,)
        assert df_wind.shape == df_suntime.shape

        df_suntime.columns = df_wind.columns
        df_compare = df_wind.compare(
            df_suntime, keep_shape=True, keep_equal=True)

        # 改名字
        df_compare.columns = [f'{i[1]}_{i[0]}'.replace('self', self.DB_LIST[0]).replace('other', self.DB_LIST[1])
                              for i in df_compare.columns]

        # 6.输出
        output_path = f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output_check/{self.CSC_MERGE_TABLE}.csv'
        df_compare.sort_index().reset_index().to_csv(output_path, index=False)
        return {'table_name': self.CSC_MERGE_TABLE, 'table_path': output_path}

    # 检测和描述性统计
    def check_df(self, table_path: str) -> dict:
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
        output_path = f'/home/lianghua/rtt/soft/airflow/dags/zirui_dag/output_check/{self.CSC_MERGE_TABLE}_COUNT.csv'
        df_null_count.reset_index().to_csv(output_path, index=False)
        return {'table_name': self.CSC_MERGE_TABLE, 'table_path': output_path}
        # print(df_null_count)

    # 合并多源数据(已弃用,备查)
    def merge_multi_data(self) -> pd.DataFrame:
        pass
        # 提取代码和日期
        df_date_code = [
            i['table_df'].loc[:, [i['table_code'], i['table_date']]].rename(
                columns={i['table_code']: 'csc_code', i['table_date']: 'csc_date'})
            for i in self.MULTI_DB_DICT.values()]

        # 拼起来,并去掉code和date完全一样的行,得到面板数据的标识列
        self.MULTI_DATE_CODE = pd.concat(
            [i for i in df_date_code], axis=0).drop_duplicates()

        # 合并所有字段
        for value in self.MULTI_DB_DICT.values():
            pre_name = value['table_db'] + '_' + \
                value['table_name'] + '_'  # 重命名,数据来源+字段名
            self.MULTI_DATE_CODE = pd.merge(left=self.MULTI_DATE_CODE, right=value['table_df'].rename(
                columns={i: pre_name + i for i in value['table_df'].columns}), how='left',
                left_on=['csc_code', 'csc_date'],
                right_on=[pre_name + value['table_code'],
                          pre_name + value['table_date']]).drop(
                columns=[pre_name + value['table_code'], pre_name + value['table_date']])

        # 保存
        return self.MULTI_DATE_CODE


    # 合并多源数据(已弃用,备查)
    def merge_multi_data_v2(self) -> pd.DataFrame:
        pass

        # 1.获取所有的索引
        wind_db = self.MAP_DICT[self.CSC_MERGE_TABLE]['wind']
        self.INDEX_COLUM = wind_db[list(wind_db.keys())[
            0]]['index_column']  # 获取wind的索引字段
        self.INDEX_DF = pd.DataFrame(
            columns=self.INDEX_COLUM).set_index(self.INDEX_COLUM)  # 建立df

        for table_dict in self.MULTI_DB_DICT.keys():
            for value in self.MULTI_DB_DICT[table_dict].values():
                value['table_df'].reset_index(inplace=True)
                value['index_column'] = [i.upper()
                                         for i in value['index_column']]

                # 重建索引
                value['table_df'].set_index(
                    value['index_column'], inplace=True)

                if table_dict == 'wind':
                    new_index = []

                    for i in list(value['table_df'].index):

                        new_index += [(i[0].split('.')[0], i[1])]

                    new_mul_index = pd.MultiIndex.from_tuples(
                        new_index, names=value['index_column'])
                    value['table_df'].index = new_mul_index
                elif table_dict == 'suntime':
                    new_index = []

                    for i in list(value['table_df'].index):

                        new_index += [(i[0], i[1].replace('-', ''))]

                    new_mul_index = pd.MultiIndex.from_tuples(
                        new_index, names=value['index_column'])
                    value['table_df'].index = new_mul_index

                    # value['table_df'].index = pd.MultiIndex.from_tuples(
                    # new_index, names=value['table_df'].names)

                # 追加索引
                self.INDEX_DF.index = self.INDEX_DF.index.append(
                    value['table_df'].index)

        self.INDEX_DF.index = self.INDEX_DF.index.drop_duplicates()

        # print(value['table_df'].index, value['index_column'])
        # print(value['table_df'].set_index(value['index_column']).index)

        # self.INDEX_DF.index = self.INDEX_DF.index.append(
        #     [value['table_df'].set_index([i.upper()
        #                                  for i in value['index_column']]).index for table_dict in
        #      self.MULTI_DB_DICT.keys() for value in self.MULTI_DB_DICT[table_dict].values()]).drop_duplicates()

        # 2.迭代join
        df_joins = []  # 不同数据源
        for table_dict in self.MULTI_DB_DICT.keys():  # ki wind,suntime
            # df_db = pd.DataFrame()  # 同一数据源
            for value in self.MULTI_DB_DICT[table_dict].values():
                # print(value['table_df'], value['table_df'].index)
                # df_table = value['table_df']
                # print(df_table)
                # print(value['table_df'].columns)
                # value['table_df'].drop_duplicates(
                # list(value['table_df'].index.names))

                value['table_df'] = value['table_df'][~value['table_df'].index.duplicated()]
                value['table_df'] = value['table_df'].drop(['index'], axis=1)
                # df_join = pd.merge(self.INDEX_DF, value['table_df'], how='left',
                #    left_on=self.INDEX_DF.index.names, right_on=value['table_df'].index.names)
                # print(value['table_df'])
                # self.INDEX_DF.drop_duplicates()
                # df_join.reset_index(inplace=True)
                df_join = self.INDEX_DF.join(
                    value['table_df'], how='left', on=self.INDEX_DF.index.names)
                print(df_join)
                # df_db = pd.concat([df_db, df_join], axis=1)  # 按顺序拼接同一数据源
                # print(df_db)
            df_joins.append(df_join)  # 不同数据源

        # print('join', df_joins)
        # 3.对比 - 只做了2个的,如果要做2个以上的需要自己写一个对比+合并函数

        self.ATTR_COLUMN = [i for i in df_joins[0].columns]  # 把wind的作为参考源

        df_joins[0].set_axis(self.ATTR_COLUMN, axis=1, inplace=True)
        df_joins[1].set_axis(self.ATTR_COLUMN, axis=1, inplace=True)

        # print(df_joins[0], df_joins[1])

        df_compare = df_joins[0].compare(
            df_joins[1], keep_shape=True, keep_equal=True)
        # # print('MULTI_TABLE_DICT', self.MULTI_TABLE_DICT.keys())
        df_compare.columns = [f'{i[1]}_{i[0]}'.replace('self', self.DB_LIST[0]).replace('other', self.DB_LIST[1])
                              for i in df_compare.columns]

        # print(df_compare.sort_index().reset_index())
        return df_compare.sort_index().reset_index()

    # 多源数据对比 不要使用修改索引的方法，存储的时候直接存成相同格式
    # 合并多源数据(已弃用,备查)
    def merge_multi_data_v3(self) -> pd.DataFrame:
        pass
      