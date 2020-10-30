import pandas as pd
import numpy as np
import tushare as ts
import datetime
import sqlite3
import time

from tushareAPI import *
from commonFunc import *
from loggingMng import *
from globalData import *


# 获取某股票一段时间的单项数据（eg 收盘/开盘/成交量等）
# ret : ret_dict -- [dict]{日期: 数据}
def GetStockSingleData(method,           # [str] 数据类型  'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol' 'amount'
                      days = -1,         # [int] 天数,几天内
                      endDate = datetime.datetime.now().strftime('%Y%m%d'),     # [str]结束日期
                      startDate = datetime.datetime.now().strftime('%Y%m%d'),   # [str]开始日期
                      ts_code = '',      # [str] 股票代码
                      stockName = ''):   # [str] 股票名称
    ret_dict = {}
    endDate_date = datetime.datetime.strptime(endDate, '%Y%m%d')
    if days > 0:
        startDate_date = endDate_date - datetime.timedelta(days=days - 1)
    else:
        startDate_date = datetime.datetime.strptime(startDate, '%Y%m%d')
    startDate = startDate_date.strftime('%Y%m%d')

    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()

    if stockName:
        try:
            cmd = "SELECT code FROM %s WHERE name = '%s';" %(g_stockCodeNameTable, stockName)
            ts_code = cursor.execute(cmd).fetchall()[0][0]
        except:
            logging.error('unkown stock name, stock name = ', stockName)
            return ret_dict
    elif ts_code:
        pass
    else:
        logging.error('empty stock name or code')
    codeInitial = ts_code[0:3]
    if codeInitial in g_stockTableName_dict.keys():
        tableName = g_stockTableName_dict[codeInitial]
    else:
        tableName = g_unKnownStockTable
    try:
        cmd = "SELECT trade_date, %s From %s WHERE ts_code = '%s' AND trade_date <=  '%s' AND trade_date >=  '%s';" %(method, tableName, ts_code, endDate, startDate)
        ret = cursor.execute(cmd).fetchall()
        ret_dict = dict(ret)
    except:
        logging.error('error cmd, cmd = ',cmd)

    conn.commit()
    conn.close()
    return ret_dict

# 获取某股票一段时间的单项数据平均值（eg 收盘/开盘/成交量等）
# ret : ret_dict -- [dict]{日期: 数据平均值}
def GetStockSingleAverDate(method,                   # [str] 数据类型  'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol' 'amount'
                          averNum=5,                # [int] n日均值  默认五日线
                          days=-1,                  # [int] 天数,几天内
                          endDate=datetime.datetime.now().strftime('%Y%m%d'),      # [str]结束日期
                          startDate=datetime.datetime.now().strftime('%Y%m%d'),    # [str]开始日期
                          ts_code='',               # [str] 股票代码
                          stockName=''              # [str] 股票名称
                          ):
    ret_dict = {}
    singleDate_dict = GetStockSingleData(method=method, days=days, endDate=endDate, startDate=startDate, ts_code=ts_code, stockName=stockName)
    try:
        singleDate_df = pd.DataFrame.from_dict(singleDate_dict, orient='index')
        if not singleDate_df.empty:
            averData_df = singleDate_df.rolling(axis=0, window=averNum).mean()
            ret_dict = dict(zip(pd.DataFrame(averData_df)._stat_axis.values.tolist(),averData_df.iloc[:,0].tolist()))
            print(ret_dict)
        else:
            logging.error("empty singleDate ts_code = ", ts_code, " name = ", stockName)
            print("empty singleDate")
    except:
        pass
    return ret_dict

def GetStockHaData(days = -1,         # [int] 天数,几天内
                   endDate = datetime.datetime.now().strftime('%Y%m%d'),     # [str]结束日期
                   startDate = datetime.datetime.now().strftime('%Y%m%d'),   # [str]开始日期
                   ts_code = '',      # [str] 股票代码
                   stockName = ''):   # [str] 股票名称
    ret_dict = {}
    openDate_dict = GetStockSingleData(method='open', days=days, endDate=endDate, startDate=startDate, ts_code=ts_code, stockName=stockName)
    closeDate_dict = GetStockSingleData(method='close', days=days, endDate=endDate, startDate=startDate, ts_code=ts_code, stockName=stockName)
    highDate_dict = GetStockSingleData(method='high', days=days, endDate=endDate, startDate=startDate, ts_code=ts_code, stockName=stockName)
    lowDate_dict = GetStockSingleData(method='low', days=days, endDate=endDate, startDate=startDate, ts_code=ts_code, stockName=stockName)
    TODO


if __name__ == '__main__':
    GetStockSingleAverDate(averNum=5, method= 'close', ts_code ='000019.SZ', days=15)