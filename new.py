import json
wind_tables = """
AShareFinancialderivative
AShareSalesSegment
AShareANNFinancialIndicator
AIndexEODPrices
AIndexFreeWeight
AIndexMembers
AIndexMembersCITICS
AShareBalanceSheet
AShareBlockTrade
AShareCalendar
AShareCashFlow
AShareConsensusData
AShareConsensusRollingData
AShareDescription
AShareEODDerivativeIndicator
AShareEODPrices
AShareFinancialIndicator
AShareIncome
AShareIndustriesClass_CITICS
AShareIndustriesClass_CS
AShareIndustriesClass_GICS
AShareIndustriesClass_SW
AShareIndustriesClass_WIND
AShareMoneyFlow
AShareProfitExpress
AShareProfitNotice
AShareST
AShareTradingSuspension
CCommodityFuturesEODPrices
CCommodityFuturesPositions
CFuturesCalendar
CFuturesContPro
CFuturesContProChange
CFuturesDescription
CFuturesMarginRatio
SHSCMembers
SZSCMembers
ChinaMutualFundStockPortfolio
AShareManagementHoldReward
AShareDividend
AIndexCSI500weight
AIndexHS300closeweight
SHSCChannelholdings
AShareISActivity
AShareConseption
ASharePlanTrade
AShareIPO
AShareEarningEst
AShareAuditOpinion
AShareMajorEvent
AShareRegInv
"""

suntime_tables = """
RPT_FORECAST_STK
RPT_RATING_ADJUST
RPT_TARGET_PRICE_ADJUST
RPT_EARNINGS_ADJUST
RPT_GOGOAL_RATING
CON_FORECAST_STK
CON_RATING_STK
CON_TARGET_PRICE_STK
CON_FORECAST_ROLL_STK
DER_FORECAST_ADJUST_NUM
DER_RATING_ADJUST_NUM
DER_REPORT_NUM
DER_CONF_STK
DER_FOCUS_STK
DER_DIVER_STK
DER_CON_DEV_ROLL_STK
DER_EXCESS_STOCK
DER_PROB_EXCESS_STOCK
DER_PROB_BELOW_STOCK
"""

csc_table_db = {
    "ASHARETTMHIS": "wind",
    "AINDEXVALUATION": "wind",
    "ASHAREFINANCIALDERIVATIVE": "wind",
    "AINDEXFINANCIALDERIVATIVE": "wind",
    "ASHARECONSEPTION": "wind",
    "ASHARESALESSEGMENT": "wind",
    "ASHARECOMPRESTRICTED": "wind",
    "ASHAREANNFINANCIALINDICATOR": "wind",
    "CHINAHEDGEFUNDNAV": "wind",
    "CHINAHEDGEFUNDDESCRIPTION": "wind",
    "CHINAMUTUALFUNDNAV": "wind",
    "CHINAMUTUALFUNDDESCRIPTION": "wind",
    "AINDEXALTERNATIVEMEMBERS": "wind",
    "AINDEXEODPRICES": "wind",
    "AINDEXFREEWEIGHT": "wind",
    "AINDEXINDUSTRIESEODCITICS": "wind",
    "AINDEXMEMBERS": "wind",
    "AINDEXMEMBERSCITICS": "wind",
    "ASHAREBALANCESHEET": "wind",
    "ASHAREBLOCKTRADE": "wind",
    "ASHARECALENDAR": "wind",
    "ASHARECASHFLOW": "wind",
    "ASHARECONSENSUSDATA": "wind",
    "ASHARECONSENSUSROLLINGDATA": "wind",
    "ASHAREDESCRIPTION": "wind",
    "ASHAREEODDERIVATIVEINDICATOR": "wind",
    "ASHAREEODPRICES": "wind",
    "ASHAREEXRIGHTDIVIDENDRECORD": "wind",
    "ASHAREFINANCIALINDICATOR": "wind",
    "ASHAREHOLDERNUMBER": "wind",
    "ASHAREINCOME": "wind",
    "ASHAREINDUSTRIESCLASS_CITICS": "wind",
    "ASHAREINDUSTRIESCLASS_CS": "wind",
    "ASHAREINDUSTRIESCLASS_GICS": "wind",
    "ASHAREINDUSTRIESCLASS_SW": "wind",
    "ASHAREINDUSTRIESCLASS_WIND": "wind",
    "ASHAREISSUINGDATEPREDICT": "wind",
    "ASHAREL2INDICATORS": "wind",
    "ASHAREMARGINTRADE": "wind",
    "ASHAREMONEYFLOW": "wind",
    "ASHAREPROFITEXPRESS": "wind",
    "ASHAREPROFITNOTICE": "wind",
    "ASHAREST": "wind",
    "ASHARESTOCKRATINGCONSUS": "wind",
    "ASHARETECHINDICATORS": "wind",
    "ASHARETRADINGSUSPENSION": "wind",
    "ASHAREYIELD": "wind",
    "ASWSINDEXEOD": "wind",
    "CBONDEODPRICES": "wind",
    "CBONDFUTURESEODPRICES": "wind",
    "CBONDFUTURESPOSITIONS": "wind",
    "CCOMMODITYFUTURESEODPRICES": "wind",
    "CCOMMODITYFUTURESPOSITIONS": "wind",
    "CFUTURESCALENDAR": "wind",
    "CFUTURESCONTPRO": "wind",
    "CFUTURESCONTPROCHANGE": "wind",
    "CFUTURESDESCRIPTION": "wind",
    "CFUTURESINSTOCK": "wind",
    "CFUTURESMARGINRATIO": "wind",
    "CFUTURESWAREHOUSESTOCKS": "wind",
    "CHINAOPTIONCALENDAR": "wind",
    "CHINAOPTIONCONTPRO": "wind",
    "CHINAOPTIONDESCRIPTION": "wind",
    "CHINAOPTIONEODPRICES": "wind",
    "CHINAOPTIONINDEXEODPRICES": "wind",
    "CINDEXFUTURESEODPRICES": "wind",
    "CINDEXFUTURESPOSITIONS": "wind",
    "COPTIONCONTPROCHANGE": "wind",
    "COPTIONDESCRIPTIONCHANGE": "wind",
    "SHSCMEMBERS": "wind",
    "SZSCMEMBERS": "wind",
    "CHINAMUTUALFUNDASSETPORTFOLIO": "wind",
    "QDIISECURITIESPORTFOLIO": "wind",
    "CHINAMUTUALFUNDSTOCKPORTFOLIO": "wind",
    "CMFINDEXEOD": "wind",
    "ASHAREINSIDEHOLDER": "wind",
    "ASHAREINSTHOLDERDERDATA": "wind",
    "ASHAREMANAGEMENTHOLDREWARD": "wind",
    "ASHAREINDUSTRIESCODE": "wind",
    "ASHARETTMANDMRQ": "wind",
    "ASHAREDIVIDEND": "wind",
    "AINDEXCSI500WEIGHT": "wind",
    "AINDEXHS300CLOSEWEIGHT": "wind",
    "SHSCCHANNELHOLDINGS": "wind",
    "CBINDEXEODPRICES": "wind",
    "CFUTURESCONTRACTMAPPING": "wind",
    "ASHAREISACTIVITY": "wind",
    "ASHAREISPARTICIPANT": "wind",
    "ASHAREISQA": "wind",
    "AINDEXWINDINDUSTRIESEOD": "wind",
    "ASHAREINCDESCRIPTION": "wind",
    "ASHAREILLEGALITY": "wind",
    "ASHAREMANAGEMENT": "wind",
    "ASHAREINVESTMENTINCOME": "wind",
    "ASHAREEQUITYPLEDGEINFO": "wind",
    "ASHAREOWNERSHIP": "wind",
    "ASHAREPLEDGEPROPORTION": "wind",
    "ASHAREEQUITYRELATIONSHIPS": "wind",
    "CFUNDWINDINDEXMEMBERS": "wind",
    "CHINAMUTUALFUNDISSUE": "wind",
    "CGBBENCHMARK": "wind",
    "CHINACLOSEDFUNDEODPRICE": "wind",
    "CHINAMUTUALFUNDSEATTRADING": "wind",
    "ASHAREINDUSTRIESCLASS_SWN": "wind",
    "ASHAREDEVALUATIONPREPARATION": "wind",
    "ASHAREBANKINDICATOR": "wind",
    "ASHAREMJRHOLDERTRADE": "wind",
    "ASHAREPLANTRADE": "wind",
    "ASHAREIPO": "wind",
    "ASHAREEARNINGEST": "wind",
    "ASHAREANNCOLUMN": "wind",
    "ASHAREANNINF": "wind",
    "ASHARETRUSTINVESTMENTTOT": "wind",
    "ASHARERELATEDCLAIMSDEBETS": "wind",
    "TOP5BYOPERATINGINCOME": "wind",
    "ASHAREIBROKERINDICATOR": "wind",
    "ASHAREINSURANCEINDICATOR": "wind",
    "ASHAREAUDITOPINION": "wind",
    "ASHARESTRANGETRADEDETIAL": "wind",
    "ASHAREMAJOREVENT": "wind",
    "ASHAREREGINV": "wind",
    "ASHAREPROSECUTION": "wind",
    "MERGEREVENT": "wind",
    "ASHARERDEXPENDITURE": "wind",
    "CBONDNEGATIVECREDITEVENT": "wind",
    "ASHAREFREEFLOATCALENDAR": "wind",
    "ASHAREINTRODUCTION": "wind",
    "ASHAREREGIONAL": "wind",
    "AINDEXMEMBERSWIND": "wind",
    "ASHAREINCOME_OP": "wind",
    "ASHARECASHFLOW_OP": "wind",
    "ASHAREBALANCESHEET_OP": "wind",
    "BAS_COMP_CAPSTRUCTURE": "suntime",
    "BAS_COMP_INFORMATION": "suntime",
    "BAS_IDX_INFORMATION": "suntime",
    "BAS_STK_HISDISTRIBUTION": "suntime",
    "BAS_STK_INFORMATION": "suntime",
    "CON_FORECAST_CICC": "suntime",
    "CON_FORECAST_CITIC": "suntime",
    "CON_FORECAST_GICS": "suntime",
    "CON_FORECAST_IDX": "suntime",
    "CON_FORECAST_ROLL_CICC": "suntime",
    "CON_FORECAST_ROLL_CITIC": "suntime",
    "CON_FORECAST_ROLL_GICS": "suntime",
    "CON_FORECAST_ROLL_IDX": "suntime",
    "CON_FORECAST_ROLL_STK": "suntime",
    "CON_FORECAST_ROLL_SW": "suntime",
    "CON_FORECAST_STK": "suntime",
    "CON_FORECAST_SW": "suntime",
    "CON_RATING_STK": "suntime",
    "CON_TARGET_PRICE_STK": "suntime",
    "DER_NEW_FORTUNE_AUTHOR": "suntime",
    "FIN_BALANCE_SHEET_BANK": "suntime",
    "FIN_BALANCE_SHEET_GEN": "suntime",
    "FIN_BALANCE_SHEET_INSUR": "suntime",
    "FIN_BALANCE_SHEET_SEC": "suntime",
    "FIN_BALANCE_SHEET_SINGLE": "suntime",
    "FIN_BALANCE_SHEET_TTM": "suntime",
    "FIN_CASH_FLOW_BANK": "suntime",
    "FIN_CASH_FLOW_GEN": "suntime",
    "FIN_CASH_FLOW_INSUR": "suntime",
    "FIN_CASH_FLOW_SEC": "suntime",
    "FIN_CASH_FLOW_SINGLE": "suntime",
    "FIN_CASH_FLOW_TTM": "suntime",
    "FIN_INCOME_BANK": "suntime",
    "FIN_INCOME_GEN": "suntime",
    "FIN_INCOME_INSUR": "suntime",
    "FIN_INCOME_SEC": "suntime",
    "FIN_INCOME_SINGLE": "suntime",
    "FIN_INCOME_TTM": "suntime",
    "FIN_MAIN_INDICATOR": "suntime",
    "FIN_MAIN_RATIO": "suntime",
    "FIN_PERFORMANCE_EXPRESS": "suntime",
    "FIN_PERFORMANCE_FORECAST": "suntime",
    "QT_IDX_CONSTITUENTS": "suntime",
    "QT_IDX_DAILY": "suntime",
    "QT_INDUS_CATEGORIES": "suntime",
    "QT_INDUS_CONSTITUENTS": "suntime",
    "QT_NTRADE_DATE": "suntime",
    "QT_STK_BLOCK": "suntime",
    "QT_STK_DAILY": "suntime",
    "QT_SW_DAILY": "suntime",
    "QT_SW_INDUS_HIS": "suntime",
    "QT_TRADE_DATE": "suntime",
    "RPT_AUTHOR_INFORMATION": "suntime",
    "RPT_EARNINGS_ADJUST": "suntime",
    "RPT_FORECAST_STK": "suntime",
    "RPT_GOGOAL_RATING": "suntime",
    "RPT_ORGAN_INFORMATION": "suntime",
    "RPT_RATING_ADJUST": "suntime",
    "RPT_RATING_COMPARE": "suntime",
    "RPT_REPORT_AUTHOR": "suntime",
    "RPT_REPORT_CAPITAL": "suntime",
    "RPT_REPORT_EVENT": "suntime",
    "RPT_REPORT_RELIABILITY": "suntime",
    "RPT_REPORT_TYPE": "suntime",
    "RPT_TARGET_PRICE_ADJUST": "suntime",
    "DER_FORECAST_ADJUST_NUM": "suntime",
    "DER_RATING_ADJUST_NUM": "suntime"
}
suntime_tables = [i.upper() for i in suntime_tables.split('\n') if i != '']
csc_table_db.update({i: 'suntime' for i in suntime_tables})
# wind_tables = [i.upper() for i in wind_tables.split('\n') if i != '']
# suntime_tables = [i.upper() for i in suntime_tables.split('\n') if i != '']

# 导出config

with open('csc_table_db.json', 'w') as f:
    json.dump(csc_table_db, f)

# print(tables.split('\n'))
