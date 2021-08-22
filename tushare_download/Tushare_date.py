import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import tushare as ts
from def_class import BasicClass as bc
import datetime
import os
from shutil import copyfile

df = pd.DataFrame
pro = ts.pro_api()

stardate = '19901219'
today = datetime.date.today().strftime('%Y%m%d')
path = "C:\\Users\\Admin\\Desktop\\py\\stockbasic"
filepath = path+'\\trade_cal.csv'
filepath1 = path+'\\SSE.csv'
filepath2 = path + '\\SZSE.csv'

df = pro.trade_cal(exchange='', start_date=stardate, end_date=today)
df.to_csv(filepath,encoding="gbk")

os.startfile(filepath)
# df = pro.trade_cal(exchange='SZSE', start_date=stardate, end_date=today)
# df.to_csv(filepath2,encoding="gbk")



