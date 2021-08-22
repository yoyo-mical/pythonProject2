import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
import xlwings as xw
import datetime
from def_class import BasicClass as bc

dirpath = 'D:\\Astock\\主板'
num = 0
sumnum = 0
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        startpath = os.path.join(root,name)
        df = pd.read_csv(startpath,encoding='gbk')
        num = len(df)
        sumnum = sumnum+num
        print(sumnum)
exit()


# 提取开始与结束交易日期
dirpath = 'D:\\Astock\\主板\\白酒'
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        startpath = os.path.join(root, name)
        df = pd.read_csv(startpath, encoding='gbk')
        print(name,df.at[0,'trade_date'],df.at[len(df.index)-1,'trade_date'])

exit()

# 输入 tscode 提取 tscode 在交易日期间未交易的日期；
tscode = '000001.SZ.csv'
stocknum = tscode.split('.')[0]
stockex = tscode.split('.')[1]

dirpath = 'D:\\Astock'
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        if name == tscode:
            openpath = os.path.join(root, name)

df = pd.read_csv(openpath,encoding='gbk')
listcal = df['trade_date'].to_list()
end = str(df.at[0,'trade_date'])
star = str(df.at[len(df.index)-1,'trade_date'])

df_cal = bc.trade_cal(stockex,star,end)[1]
df_notincal = df_cal[~df_cal['cal_date'].isin(listcal)]
pd.options.display.max_rows = None
print(df_notincal,len(df_notincal.index))

exit()

# tscode 除权分红等日期价格核对
tscode = '000002.SZ.csv'
dirpath = 'D:\\Astock'
for filename in os.listdir(dirpath):
    if filename == tscode:
        openpath = os.path.join(dirpath,filename)
df = pd.read_csv(openpath,encoding='gbk')
df_close = df[['trade_date','close']]
df_preclose = df[['trade_date','pre_close']]
# df_temp = pd.DataFrame(df_preclose.loc[len(df_preclose.index)-1]).T
df_temp = df_preclose.loc[len(df_preclose.index)-1].to_frame().T
# print(df_preclose.loc[[0,len(df_preclose.index)-1]])
# print(df_preclose)
df_preclose = df_preclose.drop(len(df_preclose.index)-1)

df_preclose = pd.concat([df_temp,df_preclose],ignore_index=True)

df_bool = df_close['close'] != df_preclose['pre_close']
df_ = pd.concat([df_close[df_bool],df_preclose[df_bool]],axis=1)
print(df_)


exit()



# 提取所有 tscode 在交易日期间未交易的天数；
listfa = []
temppath = "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\tradecal_gap.xlsx"
dirpath = 'D:\\Astock'
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        listchild =[]
        filepath = os.path.join(root,name)
        df_ = pd.read_csv(filepath,encoding='gbk')
        end = str(df_.at[0,'trade_date'])
        star = str(df_.at[len(df_.index)-1,'trade_date'])
        num_real = len(df_.index)
        stockname = os.path.splitext(name)[0]
        stocknum = stockname.split('.')[0]
        stockex = stockname.split('.')[1]
        num_cal = bc.trade_cal(stockex, star, end)[0]
        num_gap = num_cal - num_real
        listchild= [stockname,num_cal,num_real,num_gap]
        print(listchild)
        listfa.append(listchild)

df = pd.DataFrame(listfa,columns=['stockname','num_cal','num_real','num_gap'])
df.to_excel(temppath,encoding='gbk')
bc.excelshow(temppath)
os.startfile(temppath)

exit()

# 按 tscode 分类对比数据下载日期与上市日期是否有出入；
keyind = '综合类'
path = 'C:\\Users\\Admin\\Desktop\\py\\stock\\主板'+'\\'+keyind
path1='C:\\Users\\Admin\\Desktop\\py\\bak\\主板'+'\\'+keyind
sa=bc.stockbasic('L')

sa.groupby_(['market', 'industry'], keyone='主板', keytwo=keyind)
df1 = sa.dfgp[['ts_code', 'list_date']]
df2 = bc.tradedateall(path)
df = pd.merge(df1, df2, on='ts_code')

dc = bc.datacheck(path1,path)
dc.datacheck()
if dc.df_.empty:
    df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d')
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df['current'] = pd.to_datetime(df['current'], format='%Y%m%d')
    df['dategap'] = df['trade_date'] - df['list_date']
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    print(df)
else:
    df3 = dc.df_
    df4 = pd.merge(df, df3, on='ts_code')
    df4 = df4[['ts_code', 'trade_date_end_bak', 'trade_date_end_main',
               'trade_date_star_bak', 'trade_date_star_main', 'count', 'count_t']]
    df4['checkend'] = df4['trade_date_end_bak'] == df4['trade_date_end_main']
    df4['checkstar'] = df4['trade_date_star_bak'] == df4['trade_date_star_main']
    df4['checkcount'] = df4['count'] == df4['count_t']

    df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d')
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df['current'] = pd.to_datetime(df['current'], format='%Y%m%d')
    df['dategap'] = df['trade_date'] - df['list_date']
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    print(df)
    print(df4)

