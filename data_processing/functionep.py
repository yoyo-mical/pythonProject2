import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys

# stardate = "2010-01-01"
# enddate = "2010-01-05"
df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stock.csv",encoding="gbk")
df2 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockindex.csv",encoding="gbk")

# print(df['trade_date'].dtype)
# df["trade_date"]=df["trade_date"].apply(str)
# df["trade_date"]=pd.to_datetime(df["trade_date"],format ='%Y-%m-%d')
# df = df.set_index("trade_date")
# df = pd.DataFrame(df,index=pd.date_range(stardate,enddate))
pd.set_option('display.max_columns',15)
df = df.dropna()
str1 = '洋河'
tscode = df.loc[df['name'].str.contains(str1),'ts_code']
df=df.set_index('ts_code')
tsname = df.loc[df['name'].str.contains(str1),'name']
# print(df.index)
# exit()
tsvalues = pd.DataFrame();
for i in range(len(tscode.values)):
    tsvalues = tsvalues.append (df2.loc[df2['ts_code'] == tscode.values[i],['ts_code','total_mv','close']])
tsvalues['total_mv']=tsvalues['total_mv']/10000
tsvalues=tsvalues.set_index('ts_code')
tsvalues['name']=tsname
print(tsvalues.sort_values('total_mv'))