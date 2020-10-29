import pandas as pd
import numpy as np
import tushare as ts
import datetime
import sqlite3
import time

import commonFunc
from loggingMng import *

g_token = '50b2e7a5a3786bbc2eeb9478388618ca59d7fab53c3ec97ff91a822b'
g_pro = ts.pro_api(g_token)
g_dbPath = r'E:/stock/db/stock.db'


# 获取所有股票日数据
# in ： strDate: [str]日期
# ret : allStockDailyData: [DataFrame]所有数据
def GetAllStockDailyData(trade_date = datetime.datetime.now().strftime('%Y%m%d')):
    allStockDailyData = g_pro.daily(trade_date=trade_date)
    return allStockDailyData


def GetStockName():
    df = g_pro.stock_basic(exchange='')
    codes = df.ts_code.values
    names = df.name.values
    stockName_dict = dict(zip(codes, names))
    return stockName_dict


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

# ----------------------------------------------------------------------------------------------------------------------

def UpdateDatabase_StockCodeName():
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()

    try:
        cursor.execute(r'DROP TABLE StockCodeName')
    except:
        logging.info("table StockCodeName is not exist")
    try:
        cursor.execute('''CREATE TABLE IF NOT EXISTS  StockCodeName
                      (ts_code       TEXT    PRIMARY KEY  NOT NULL,
                       name          TEXT                 NOT NULL);''')
    except:
        logging.info('table StockCodeName already exists.')
    stockName_dict = GetStockName()
    for ts_code in stockName_dict.keys():
        name = stockName_dict[ts_code]
        cmd = r'INSERT INTO StockCodeName (ts_code, name) VALUES (' + \
              "'" + str(ts_code) + "'" + r',' + \
              "'" + str(name) + "'" + \
              r')'
        cursor.execute(cmd)

    conn.commit()
    conn.close()


def UpdateDatabase_oneStock(tableName, stockDailyData, cursor):
    try:
        cmd = r'CREATE TABLE IF NOT EXISTS ' + tableName + \
              r'(ts_code         TEXT           NOT NULL,' + \
              r'name             TEXT           NOT NULL,' + \
              r'trade_date       TEXT           NOT NULL,' + \
              r'open             REAL           NOT NULL,' + \
              r'high             REAL           NOT NULL,' + \
              r'low              REAL           NOT NULL,' + \
              r'close            REAL           NOT NULL,' + \
              r'pre_close        REAL           NOT NULL,' + \
              r'change           REAL           NOT NULL,' + \
              r'pct_chg          REAL           NOT NULL,' + \
              r'vol              REAL           NOT NULL,' + \
              r'amount           REAL           NOT NULL, primary key(ts_code,trade_date));'
        cursor.execute(cmd)
    except:
        print("table ", tableName, " already exists.")

    ts_code = stockDailyData.loc['ts_code']
    try:
        cmd = r'SELECT name FROM StockCodeName WHERE ts_code = ' + "'" + ts_code + "'" + ';'
        name = cursor.execute(cmd).fetchall()[0][0]
    except:
        print("failed to find name.", ts_code)
    trade_date = stockDailyData.loc['trade_date']
    open = stockDailyData.loc['open']
    high = stockDailyData.loc['high']
    low = stockDailyData.loc['low']
    close = stockDailyData.loc['close']
    pre_close = stockDailyData.loc['pre_close']
    change = stockDailyData.loc['change']
    pct_chg = stockDailyData.loc['pct_chg']
    vol = stockDailyData.loc['vol']
    amount = stockDailyData.loc['amount']

    try:
        cmd = r'INSERT INTO ' + tableName + \
              r'(ts_code, name, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount) VALUES (' + \
              "'" + str(ts_code) + "'" + r',' + \
              "'" + str(name) + "'" + r',' + \
              "'" + str(trade_date) + "'" + r',' + \
              str(open) + r',' + \
              str(high) + r',' + \
              str(low) + r',' + \
              str(close) + r',' + \
              str(pre_close) + r',' + \
              str(change) + r',' + \
              str(pct_chg) + r',' + \
              str(vol) + r',' + \
              str(amount) + \
              r')'
        cursor.execute(cmd)
    except:
        # print('insert error')
        pass


def UpdateDatabase_stock():
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()

    allStockData_dict = GetAllStockData(2)
    for tradeDate in allStockData_dict.keys():
        dailyData = allStockData_dict[tradeDate]
        for j in range(len(dailyData)):
            stockDailyData = dailyData.loc[j]
            tableName = r'stock' + str(stockDailyData.loc['ts_code'])[0:3]
            UpdateDatabase_oneStock(tableName, stockDailyData, cursor)

    # 数据库提交和关闭耗时长 不要加在循环内
    conn.commit()
    conn.close()


if __name__ == '__main__':
    UpdateDatabase_StockCodeName()
    UpdateDatabase_stock()
