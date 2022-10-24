import os
import json
from airflow.models import Variable
from datetime import timedelta, date, datetime
LOAD_PATH_ROOT = '/home/lianghua/rtt/mountdir/data/load/'
with open(Variable.get("csc_input_table")) as j:
    TABLE_LIST = json.load(j)['need_tables']
with open(Variable.get("db_sql_dict")) as j:
    DB_SQL_DICT = json.load(j)  # 依赖的SQL语句
CONFIG_PATH = '/home/lianghua/ZIRUI/rely_files/test_type_df_and_parquet/'  # 转换依赖的CONFIG

DateColumns = {'name': []}
WAIT_LIST = []  # 没下完的表


def get_new_pk(select_table):
    PrimaryKeys = {
        'aindexcsi500weight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexeodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'aindexfreeweight': [],
        'aindexhs300closeweight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexhs300freeweight': ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'TRADE_DT'],
        'aindexmembers': ['S_INFO_WINDCODE', 'S_CON_WINDCODE'],
        'aindexmemberscitics': ['S_INFO_WINDCODE', 'S_CON_WINDCODE'],
        'ASHAREPLANTRADE': ['S_INFO_WINDCODE', 'ANN_DT', 'ANN_DT_NEW'],
        'ashareannfinancialindicator': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareauditopinion': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'asharebalancesheet': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareblocktrade': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'asharecalendar': ['S_INFO_EXCHMARKET'],
        'asharecashflow': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareconsensusdata': ['S_INFO_WINDCODE', 'EST_DT', 'EST_REPORT_DT', 'CONSEN_DATA_CYCLE_TYP'],
        'ashareconsensusrollingdata': ['S_INFO_WINDCODE', 'EST_DT', 'ROLLING_TYPE'],
        'ashareconseption': ['S_INFO_WINDCODE', 'WIND_SEC_CODE'],
        'asharedescription': ['S_INFO_WINDCODE'],
        'asharedividend': ['S_INFO_WINDCODE', 'S_DIV_PROGRESS', 'ANN_DT'],
        'ashareearningest': ['S_INFO_WINDCODE', 'WIND_CODE'],
        'ashareeodderivativeindicator': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ashareeodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'asharefinancialderivative': ['S_INFO_COMPCODE', 'BEGINDATE', 'ENDDATE'],
        'asharefinancialindicator': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareincome': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD', 'STATEMENT_TYPE'],
        'ashareipo': ['S_INFO_WINDCODE'],
        'ashareisactivity': ['S_INFO_WINDCODE', 'S_SURVEYDATE', 'S_SURVEYTIME', 'S_ACTIVITIESTYPE'],
        'asharemajorevent': ['S_INFO_WINDCODE', 'S_EVENT_CATEGORYCODE', 'S_EVENT_ANNCEDATE'],
        'asharemanagementholdreward': ['S_INFO_WINDCODE', 'ANN_DATE'],
        'asharemoneyflow': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ashareprofitexpress': ['S_INFO_WINDCODE', 'ANN_DT', 'REPORT_PERIOD'],
        'ashareprofitnotice': ['S_INFO_WINDCODE', 'S_PROFITNOTICE_DATE', 'S_PROFITNOTICE_PERIOD'],
        'asharereginv': ['S_INFO_WINDCODE', 'STR_DATE'],
        'asharesalessegment': ['S_INFO_WINDCODE', 'REPORT_PERIOD', 'S_SEGMENT_ITEM'],
        'asharest': ['S_INFO_WINDCODE', 'ANN_DT'],
        'asharetradingsuspension': ['S_INFO_WINDCODE', 'S_DQ_SUSPENDDATE'],
        'ccommodityfutureseodprices': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'ccommodityfuturespositions': ['S_INFO_WINDCODE', 'TRADE_DT', 'S_INFO_COMPCODE', 'FS_INFO_TYPE'],
        'cfuturescalendar': [],
        'cfuturescontpro': ['S_INFO_WINDCODE'],
        'cfuturescontprochange': ['S_INFO_WINDCODE'],
        'cfuturesdescription': ['S_INFO_WINDCODE'],
        'cfuturesmarginratio': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'chinamutualfundstockportfolio': ['S_INFO_WINDCODE', 'ANN_DATE'],
        'con_forecast_roll_stk': ['stock_code', 'con_date'],
        'con_forecast_stk': ['stock_code', 'con_date'],
        'con_rating_stk': ['stock_code', 'con_date'],
        'con_target_price_stk': ['stock_code', 'con_date'],
        'der_conf_stk': ['stock_code', 'con_date', 'con_year'],
        'der_con_dev_roll_stk': ['stock_code', 'con_date'],
        'der_diver_stk': ['stock_code', 'con_date', 'con_year'],
        'der_excess_stock': ['stock_code', 'declare_date', 'report_year'],
        'der_focus_stk': ['stock_code', 'con_date'],
        'der_forecast_adjust_num': ['stock_code', 'con_date', 'con_year'],
        'der_prob_below_stock': ['stock_code', 'report_year', 'declare_date'],
        'der_prob_excess_stock': ['stock_code', 'report_year', 'report_quarter', 'declare_date'],
        'der_rating_adjust_num': ['stock_code', 'con_date'],
        'der_report_num': ['stock_code', 'con_date'],
        'rpt_earnings_adjust': ['stock_code', 'current_create_date', 'report_year'],
        'rpt_forecast_stk': ['stock_code', 'create_date', 'report_year'],
        'rpt_gogoal_rating': ['gg_rating_code'],
        'rpt_rating_adjust': ['stock_code', ''],
        'rpt_target_price_adjust': ['stock_code', 'updatetime', 'organ_id'],
        'shscchannelholdings': ['S_INFO_WINDCODE', 'TRADE_DT'],
        'shscmembers': ['S_INFO_WINDCODE'],
        'szscmembers': ['S_INFO_WINDCODE']
    }
    PrimaryKeys = {k.upper(): v for k, v in PrimaryKeys.items()}
    return PrimaryKeys[select_table]


def check():
    # 列出所有表名
    table_list = [table for table in os.listdir(LOAD_PATH_ROOT)]
    # 列出所有
    for table in table_list:

        # get_config(table)
        date_list = [date for date in os.listdir(LOAD_PATH_ROOT+table)]
        if 3 < len(date_list) <= 5000:
            print(table, len(date_list))
            WAIT_LIST.append(table)
    print(WAIT_LIST)


def get_db_config(select_table):
    """
    生成config
    """
    with open(Variable.get("db_sql_dict")) as j:
        DB_SQL_DICT = json.load(j)  # 依赖的SQL语句
    table_info = DB_SQL_DICT[select_table]
    # print(select_table, table_info['oraignal_table'])
    # DateColumns.update({select_table: [table_info['date_where']]})


def get_config(select_table):
    """
    按表输出config.json文件
    """
    table_info = DB_SQL_DICT[select_table]
    with open(f'{CONFIG_PATH}{select_table}.json') as j:
        config_dict = json.load(j)  # 转换type的config
    # -----------------cols----------------- #
    type_config = {
        k: {
            'original_type': v['database_type'],
            'parquet_type': v['parquet_type']
        }
        for k, v in config_dict.items()
    }

    # -----------------date----------------- #
    table_info.update({"all_columns": type_config})
    # ---------------输出----------------- #

    out_put_config = {
        "primary_key": get_new_pk(select_table),
        "original_table": table_info['oraignal_table'],
        'dynamic': table_info['dynamic'],
        'date_column': table_info['date_where'],
        'last_update': str(datetime.now()),
        'all_cols': table_info['all_columns']
    }
    OUT_PUT_PATH = f'{LOAD_PATH_ROOT}{select_table}/config.json'
    with open(OUT_PUT_PATH, 'w') as f:
        json.dump(out_put_config, f)
    # -----------------config---------------- #


def get_wind_table():
    WIND = []
    for k, v in DB_SQL_DICT.items():
        # print(k,v)
        # break
        if v['data_base'] == 'wind' and v['dynamic'] == True and k in TABLE_LIST:
            # print(v['all_columns'],k)
            if '[OPDATE]' in v['sql']:
                WIND.append(k)

    print(WIND)


def get_suntime_table():
    SUNTIME = []
    for k, v in DB_SQL_DICT.items():
        # print(k,v)
        # break
        if v['data_base'] == 'suntime' and v['dynamic'] == True and k in TABLE_LIST:
            # print(v['all_columns'],k)
            SUNTIME.append(k)

    # print(SUNTIME)


# get_df()
check()
# get_suntime_table()
# print(get_new_pk())
# get_wind_table()
# with open('DateColumns.json', 'w') as f:
#     json.dump(DateColumns, f)
