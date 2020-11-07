import pandas as pd
import numpy as np
import tushare as ts
import datetime

from commonFunc import *
from loggingMng import *
from globalData import *




# 获取所有股票日数据
# in ： strDate: [str]日期
# ret : allStockDailyData -- [DataFrame]所有数据：每日行情、每日指标
def GetAllStockDailyData(trade_date = datetime.datetime.now().strftime('%Y%m%d')):
    allStockDailyMarket = g_pro.daily(trade_date=trade_date)        #每日行情
    allStockDailyBasic = g_pro.daily_basic(trade_date=trade_date)   #每日指标
    allStockDailyData = pd.merge(allStockDailyMarket, allStockDailyBasic, on=['ts_code','trade_date','close'], how='outer')
    return allStockDailyData


# 获取所有股票一段时间的所有数据
# in ： days -- [int]天数,几天内  endDate -- [str]结束日期  startDate -- [str]开始日期
# ret : allStockDailyData -- [dict of DataFrame]{日期:数据}
def GetAllStockData(days = -1, endDate = datetime.datetime.now().strftime('%Y%m%d'), startDate = datetime.datetime.now().strftime('%Y%m%d')):
    allStockData_dict = {}
    endDate_date = datetime.datetime.strptime(endDate, '%Y%m%d')
    if days > 0:
        startDate_date = endDate_date - datetime.timedelta(days=days-1)
    else:
        startDate_date = datetime.datetime.strptime(startDate,'%Y%m%d')
    if (startDate_date > endDate_date):
        # todo 完善日志管理
        logging.error('日期格式不正确')
        return allStockData_dict
    while startDate_date <= endDate_date:
        allStockDailyData = GetAllStockDailyData(trade_date=startDate_date.strftime('%Y%m%d'))
        if not allStockDailyData.empty:
            allStockData_dict[startDate_date.strftime('%Y%m%d')] = allStockDailyData
        startDate_date += datetime.timedelta(days=1)
    return allStockData_dict


# 获取所有股票基本信息
# ret: stockName_dict -- [dict]{代码:名称}
def GetStockInfo():
    df_basic = g_pro.stock_basic(exchange='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    df_company = g_pro.stock_company(exchange='SZSE', fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope')

    df_data = pd.merge(df_basic, df_company, on=['ts_code','exchange'], how='outer')
    return df_data


if __name__ == '__main__':
    GetStockInfo()
    # data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')