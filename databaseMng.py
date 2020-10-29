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

# ----------------------------------------------------------------------------------------------------------------------


# 获取更新日期
# ret:
def GetUpdateDate():

    pass


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
        cmd = r'SELECT name FROM ' + g_stockCodeNameTable + r' WHERE ts_code = ' + "'" + ts_code + "'" + ';'
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
    # 数据库连接、提交和关闭耗时长 不要加在循环内
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()

    allStockData_dict = GetAllStockData(startDate = g_defaultUpdateDate)
    for tradeDate in allStockData_dict.keys():
        dailyData = allStockData_dict[tradeDate]
        for j in range(len(dailyData)):
            stockDailyData = dailyData.loc[j]
            codeInitial = str(stockDailyData.loc['ts_code'])[0:3]
            if codeInitial in g_stockTableName_dict.keys():
                tableName = g_stockTableName_dict[codeInitial]
            else:
                logging.error('unknown type, code = ', str(stockDailyData.loc['ts_code']))
                tableName = g_unKnownStockTable
            UpdateDatabase_oneStock(tableName, stockDailyData, cursor)

    conn.commit()
    conn.close()



if __name__ == '__main__':
    # 1. 更新股票代码名字对应关系
    UpdateDatabase_StockCodeName()
    # 2. 更新股票日数据（ts_code, name, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount）
    UpdateDatabase_stock()
