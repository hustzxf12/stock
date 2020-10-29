import pandas as pd
import numpy as np
import tushare as ts
import datetime

from commonFunc import *
from loggingMng import *
from globalData import *




# 获取所有股票日数据
# in ： strDate: [str]日期
# ret : allStockDailyData -- [DataFrame]所有数据
def GetAllStockDailyData(trade_date = datetime.datetime.now().strftime('%Y%m%d')):
    allStockDailyData = g_pro.daily(trade_date=trade_date)
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


# 获取所有股票代码和名称映射关系
# ret: stockName_dict -- [dict]{代码:名称}
def GetStockName():
    df = g_pro.stock_basic(exchange='')
    codes = df.ts_code.values
    names = df.name.values
    stockName_dict = dict(zip(codes, names))
    return stockName_dict



