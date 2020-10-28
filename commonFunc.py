import pandas as pd
from loggingMng import *

def WriteDataFrame(data, filePath = "temp.xlsx"):
    # todo 优化成可以写sheet
    writer = pd.ExcelWriter(filePath)
    data.to_excel(writer, sheet_name='sheet1')
    writer.save()
    writer.close()