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

def GetCurrentTableList():
    currentTable_list = []

    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()
    try:
        cmd = "SELECT name FROM sqlite_master where type='table' order by name"
        result = cursor.execute(cmd).fetchall()
        for i in range(len(result)):
            currentTable_list.append(result[i][0])
    except:
        logging.error("failed to get current table list")
        pass

    conn.commit()
    conn.close()
    return currentTable_list


# 获取更新日期
# ret: updateDate -- 更新日期
def GetUpdateDate():
    currentTable_list = GetCurrentTableList()
    print(currentTable_list)
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()
    updateDate = "99999999"
    for i in range(len(currentTable_list)):
        tableName = currentTable_list[i]
        try:
            cmd = "SELECT trade_date FROM %s ORDER BY trade_date DESC LIMIT 1" %(tableName)
            latestDate = cursor.execute(cmd).fetchall()[0][0]
            if latestDate < updateDate:
                updateDate = latestDate
        except:
            pass
    conn.commit()
    conn.close()
    if updateDate == "99999999":
        logging.error("cannot calculate update date")
        updateDate = g_defaultUpdateDate
    logging.info("update date = %s"%updateDate)

    return updateDate


def UpdateDatabase_StockCodeName():
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()
    # ['ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname',
    #  'market', 'exchange', 'curr_type', 'list_status', 'list_date',
    #  'delist_date', 'is_hs', 'chairman', 'manager', 'secretary',
    #  'reg_capital', 'setup_date', 'province', 'city', 'introduction',
    #  'website', 'email', 'office', 'business_scope', 'employees',
    #  'main_business'],
    try:
        cursor.execute("DROP TABLE StockCodeName")
    except:
        logging.info("table StockCodeName is not exist")
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS  " \
                      "(ts_code       TEXT    PRIMARY KEY  NOT NULL," \
                      "symbol         TEXT    NOT NULL," \
                      "name           TEXT    NOT NULL," \
                      "area           TEXT    NOT NULL," \
                      "industry       TEXT    NOT NULL," \
                      "fullname       TEXT    NOT NULL," \
                      "enname         TEXT    NOT NULL," \
                      "market         TEXT    NOT NULL," \
                      "exchange       TEXT    NOT NULL," \
                      "curr_type      TEXT    NOT NULL," \
                      "list_status    TEXT    NOT NULL," \
                      "list_date      TEXT    NOT NULL," \
                      "delist_date    TEXT    NOT NULL," \
                      "is_hs          TEXT    NOT NULL," \
                      "chairman       TEXT    NOT NULL," \
                      "manager        TEXT    NOT NULL," \
                      "secretary      TEXT    NOT NULL," \
                      "reg_capital    TEXT    NOT NULL," \
                      "setup_date     TEXT    NOT NULL," \
                      "province       TEXT    NOT NULL," \
                      "city           TEXT    NOT NULL," \
                      "introduction   TEXT    NOT NULL," \
                      "website        TEXT    NOT NULL," \
                      "email          TEXT    NOT NULL," \
                      "office         TEXT    NOT NULL," \
                      "business_scope TEXT    NOT NULL," \
                      "employees      TEXT    NOT NULL," \
                      "main_business  TEXT    NOT NULL);"%(g_stockInfoTable))
    except:
        logging.info('table StockCodeName already exists.')
    stockName_dict = GetStockInfo()
    for ts_code in stockName_dict.keys():
        name = stockName_dict[ts_code]
        cmd = "INSERT INTO %s (ts_code, name) VALUES ('%s', '%s');" %(g_stockInfoTable, ts_code, name)
        cursor.execute(cmd)

    conn.commit()
    conn.close()


def UpdateDatabase_oneStock(tableName, stockDailyData, cursor):
    try:
        cmd = "CREATE TABLE IF NOT EXISTS %s "\
              "(ts_code         TEXT           NOT NULL," \
              "name             TEXT           NOT NULL," \
              "trade_date       TEXT           NOT NULL," \
              "open             REAL           NOT NULL," \
              "high             REAL           NOT NULL," \
              "low              REAL           NOT NULL," \
              "close            REAL           NOT NULL," \
              "pre_close        REAL           NOT NULL," \
              "change           REAL           NOT NULL," \
              "pct_chg          REAL           NOT NULL," \
              "vol              REAL           NOT NULL," \
              "amount           REAL           NOT NULL," \
              "turnover_rate    REAL           NOT NULL," \
              "turnover_rate_f  REAL           NOT NULL," \
              "volume_ratio     REAL           NOT NULL," \
              "pe               REAL           NOT NULL," \
              "pe_ttm           REAL           NOT NULL," \
              "pb               REAL           NOT NULL," \
              "ps               REAL           NOT NULL," \
              "ps_ttm           REAL           NOT NULL," \
              "dv_ratio         REAL           NOT NULL," \
              "dv_ttm           REAL           NOT NULL," \
              "total_share      REAL           NOT NULL," \
              "float_share      REAL           NOT NULL," \
              "free_share       REAL           NOT NULL," \
              "total_mv         REAL           NOT NULL," \
              "circ_mv          REAL           NOT NULL," \
              "primary key(ts_code,trade_date));" %(tableName)
        cursor.execute(cmd)
    except:
        print("table ", tableName, " already exists.")

    ts_code = stockDailyData.loc['ts_code']
    try:
        cmd = "SELECT name FROM %s WHERE ts_code = '%s';" %(g_stockInfoTable, ts_code)
        name = cursor.execute(cmd).fetchall()[0][0]
    except:
        name = 'unknownName'
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
    turnover_rate = stockDailyData.loc['turnover_rate']
    turnover_rate_f = stockDailyData.loc['turnover_rate_f']
    volume_ratio = stockDailyData.loc['volume_ratio']
    pe = stockDailyData.loc['pe']
    pe_ttm = stockDailyData.loc['pe_ttm']
    pb = stockDailyData.loc['pb']
    ps = stockDailyData.loc['ps']
    ps_ttm = stockDailyData.loc['ps_ttm']
    dv_ratio = stockDailyData.loc['dv_ratio']
    dv_ttm = stockDailyData.loc['dv_ttm']
    total_share = stockDailyData.loc['total_share']
    float_share = stockDailyData.loc['float_share']
    free_share = stockDailyData.loc['free_share']
    total_mv = stockDailyData.loc['total_mv']
    circ_mv = stockDailyData.loc['circ_mv']


    try:
        cmd = "INSERT INTO %s (ts_code, name, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount," \
              "turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv) VALUES " \
              "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
              %(tableName, ts_code, name, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount,
                turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv)
        cursor.execute(cmd)
    except:
        # print('insert error')
        pass


def UpdateDatabase_stock():
    # 数据库连接、提交和关闭耗时长 不要加在循环内
    conn = sqlite3.connect(g_dbPath)
    cursor = conn.cursor()

    print(list(g_stockTableName_dict.values()))

    startDate = g_defaultUpdateDate
    if set(list(g_stockTableName_dict.values())) <= set(GetCurrentTableList()):
        startDate = GetUpdateDate()
    else:
        pass
    print("startDate = ",startDate)
    allStockData_dict = GetAllStockData(startDate=startDate)

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
    # 2. 更新股票日数据
    UpdateDatabase_stock()

