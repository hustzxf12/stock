import tushare as ts

g_token = '50b2e7a5a3786bbc2eeb9478388618ca59d7fab53c3ec97ff91a822b'
g_pro = ts.pro_api(g_token)

g_index = {'上证综指': '000001.SH', '深证成指': '399001.SZ',
         '沪深300': '000300.SH', '创业板指': '399006.SZ',
         '上证50':  '000016.SH', '中证500': '000905.SH',
         '中小板指': '399005.SZ', '上证180': '000010.SH'}

g_dbPath = r'./db/stock.db'
g_stockTableName_dict = {'000': 'table000',
                         '001': 'table001',
                         '002': 'table002',
                         '003': 'table003',
                         '300': 'table300',
                         '600': 'table600',
                         '601': 'table601',
                         '603': 'table603',
                         '605': 'table605',
                         '688': 'table688'}

g_unKnownStockTable = 'unKnownTypeStock'

g_stockCodeNameTable = 'StockCodeName'

g_defaultUpdateDate = '20201026'










pass