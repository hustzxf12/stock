import pandas as pd
import numpy as np
import tushare as ts
import datetime

import commonFunc
from loggingMng import *

g_token = '50b2e7a5a3786bbc2eeb9478388618ca59d7fab53c3ec97ff91a822b'
g_pro = ts.pro_api(g_token)

# 常用大盘指数
index = {'上证综指': '000001.SH', '深证成指': '399001.SZ',
         '沪深300': '000300.SH', '创业板指': '399006.SZ',
         '上证50':  '000016.SH', '中证500': '000905.SH',
         '中小板指': '399005.SZ', '上证180': '000010.SH'}

# 获取所有股票日数据
# in ： strDate: [str]日期
# ret : allStockDailyData: [DataFrame]所有数据
def GetAllStockDailyData(trade_date = datetime.datetime.now().strftime('%Y%m%d')):
    allStockDailyData = g_pro.daily(trade_date=trade_date)
    return allStockDailyData


# 获取所有股票一段时间的所有数据
# in ：  days: [int]天数,几天内  endDate: [str]结束日期  startDate: [str]开始日期
# ret : allStockDailyData： [dict of DataFrame]{日期:数据}
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


# 获取所有股票一段时间的收盘价格
# in ：  days: [int]天数,几天内  endDate: [str]结束日期  startDate: [str]开始日期
# ret : dailyCloseData： [DataFrame]收盘价格数据
def GetAllStockCloseDate(days = -1, endDate = datetime.datetime.now().strftime('%Y%m%d'), startDate = datetime.datetime.now().strftime('%Y%m%d')):
    allStockData_dict = GetAllStockData(days=days, endDate=endDate, startDate=startDate)
    dailyCloseData = pd.DataFrame()
    print(dailyCloseData)
    for tradeDate in allStockData_dict.keys():
        dailyCloseDataTemp = allStockData_dict[tradeDate][['ts_code','close']]   # 提取ts_code和close两列
        dailyCloseDataTemp = dailyCloseDataTemp.rename(columns={'close': tradeDate})
        if not dailyCloseData.empty:
            dailyCloseData = pd.merge(dailyCloseData, dailyCloseDataTemp, on='ts_code', how='outer')
        else:
            dailyCloseData = dailyCloseDataTemp
    # dailyCloseData.set_index(["ts_code"], inplace=True)
    return dailyCloseData


# 获取所有股票n日均值
# in ：  averNum: [int]n日均值  days: [int]天数,几天内  endDate: [str]结束日期  startDate: [str]开始日期
# ret : dailyAverCloseData_dict： [dict of DataFrame]{n日均值:数据}
def GetAllStockAverDate(averNum, days = -1, endDate = datetime.datetime.now().strftime('%Y%m%d'), startDate = datetime.datetime.now().strftime('%Y%m%d')):
    dailyCloseData = GetAllStockCloseDate(days=days, endDate=endDate, startDate=startDate)
    dailyCloseData.set_index(["ts_code"], inplace=True)
    dailyAverCloseData_dict = {}
    for i in range(len(list(averNum))):
        # 需要深拷贝 否则求均值将会改变原有数据
        dailyAverCloseData = dailyCloseData.copy()
        print(dailyCloseData)
        for j in range(len(dailyAverCloseData)):
            dailyAverCloseData.iloc[j] = dailyAverCloseData.iloc[j].rolling(window=averNum[i]).mean()
            dailyAverCloseData_dict[averNum[i]] = dailyAverCloseData
    return dailyAverCloseData_dict






if __name__ == '__main__':
    # data = GetAllStockAverDate(averNum=[5,10], days=30)

    # commonFunc.WriteDataFrame(data[5], 'date1.xlsx')
    # commonFunc.WriteDataFrame(data[10], 'date2.xlsx')
    data = GetAllStockData(5)
    commonFunc.WriteDataFrame(data['20201026'], 'data.xlsx')
    print(data)

