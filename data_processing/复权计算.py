import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
from def_class import BasicClass as bc
import xlwings as xw
from inspect import getsource
import datetime


# earn>1 ts 分类显示
savepath = 'D:\\Astock_Save\\years.csv'
df = pd.read_csv(savepath,encoding='gbk')
pd.options.display.max_rows=None
pd.options.display.max_columns=None
# print(df['industry'].value_counts())
# exit()
df_gp = df.groupby(['industry']).get_group('软件服务')
print(df_gp)

exit()


# 统计近十年 earn>1 的所有ts
dirpath = 'D:\\Astock_Save\\years'
savepath = 'D:\\Astock_Save\\years.csv'
k = 1
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        name_year = os.path.splitext(name)[0]
        startpath = os.path.join(root, name)
        df = pd.read_csv(startpath,encoding='gbk')
        df['year']=name_year
        df1 = df[df['earn']>=1]
        if k==1:
            df2 = df1
        else:
            df2 = pd.concat([df2, df1], ignore_index=True)
        k=k+1
df2.to_csv(savepath,encoding='gbk')
os.startfile(savepath)

exit()


# 不同时期所有 ts_code 复权计算
df_base = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL.csv", encoding="gbk")
dirpath = 'D:\\Astock\\主板'
yearlist1 = ['20100101','20110101','20120101','20130101','20140101','20150101','20160101',
             '20170101','20180101','20190101','20200101']
yearlist2 = ['20101231','20111231','20121231','20131231','20141231','20151231','20161231',
             '20171231','20181231','20191231','20201231']

for yearnum in range(len(yearlist1)):
    sortpath = 'D:\\Astock_Save\\'+yearlist1[yearnum][0:4]+'.csv'


    listappend = []
    list2 = []
    for root, dir, filename in os.walk(dirpath):
        for name in filename:
            name_sp = os.path.splitext(name)[0]
            df_part = df_base[df_base['ts_code'] == name_sp][['name', 'industry']]
            part_index = df_part.index[0]
            ts_name = df_part.at[part_index, 'name']
            industry = df_part.at[part_index, 'industry']
            startpath = os.path.join(root, name)
            df = pd.read_csv(startpath, encoding='gbk')

            startdate = yearlist1[yearnum]
            enddate = yearlist2[yearnum]

            if df.at[0, 'trade_date'] > int(enddate) or df.at[len(df.index) - 1, 'trade_date'] < int(startdate):
                list2.append(name_sp)
            else:
                startlist = df[df['trade_date'] >= int(startdate)].index
                endlist = df[df['trade_date'] <= int(enddate)].index
                startdate = startlist[0]
                enddate = endlist[len(endlist) - 1]

                if enddate < startdate:
                    list2.append(name_sp)
                else:
                    df = df.loc[startdate:enddate]
                    df.reset_index(drop=True, inplace=True)

                    # price = df.at[0, 'close'] * 100
                    price = 1

                    for i in range(len(df.index)):
                        if i == 0:
                            df.at[i, '复权因子'] = (1 + df.at[i, 'pct_chg'] * 0.01) * price
                        else:
                            df.at[i, '复权因子'] = (1 + df.at[i, 'pct_chg'] * 0.01) * df.at[i - 1, '复权因子']

                    list1 = []
                    moneylist = df['复权因子'].to_list()
                    in_money = moneylist[0]
                    out_money = moneylist[len(moneylist) - 1]
                    earn_money = moneylist[len(moneylist) - 1] - moneylist[0]
                    list1 = [name_sp, ts_name, industry, in_money, out_money, earn_money]
                    listappend.append(list1)
    if listappend != []:
        df_money = pd.DataFrame(listappend, columns=['ts_code', 'ts_name', 'industry', 'in', 'out', 'earn'])
        df_money.sort_values(by='earn', inplace=True, ascending=False)
        pd.options.display.max_rows = None
        df_money.to_csv(sortpath, encoding='gbk')
        # bc.excelshow(sortpath)
        # os.startfile(sortpath)
        print(list2, '此段时间未有交易')

    else:
        print(list2, '此段时间未有交易')


exit()


# 单只 ts_code 确认
temppath = 'C:\\Users\\Admin\\Desktop\\py\\temp\\000001.SZ.csv'
df = pd.read_csv(temppath,encoding='gbk')
startdate = '20100805'
enddate = '20210319'

startmargin = pd.to_datetime(startdate,format='%Y%m%d')+datetime.timedelta(days=9)
startmargin= startmargin.strftime('%Y%m%d')

endmargin = pd.to_datetime(enddate,format='%Y%m%d')+datetime.timedelta(days=-9)
endmargin= endmargin.strftime('%Y%m%d')

startlist = df[(df['交易日期']>=int(startdate))&(df['交易日期']<=int(startmargin))].index
endlist = df[(df['交易日期']<=int(enddate))&(df['交易日期']>=int(endmargin))].index
startdate = startlist[0]
enddate = endlist[len(endlist)-1]

df = df.loc[startdate:enddate]
df.reset_index (drop=True,inplace = True)

price = df.at[0,'收盘价']*100


for i in range(len(df.index)):
    if i == 0:
        df.at[i,'复权因子'] = price
    else:
        df.at[i,'复权因子'] =(1+df.at[i,'涨跌幅']*0.01)*df.at[i-1,'复权因子']

moneylist = df['复权因子'].to_list()
in_money = moneylist[0]
out_money = moneylist[len(moneylist)-1]
earn_money = moneylist[len(moneylist)-1]-moneylist[0]

print([in_money,out_money,earn_money])

exit()




# ts_code 列名改中文
dirpath = 'D:\\Astock'
temppath = 'C:\\Users\\Admin\\Desktop\\py\\temp\\000001.SZ.csv'
tsdict = {'trade_date':'交易日期','ts_code':'股票代码','open':'开盘价','high':'最高价','low':'最低价',
         'close':'收盘价','pre_close':'前收盘价','change':'涨跌额','pct_chg':'涨跌幅',
         'vol':'成交量','amount':'成交额','name':'曾用名'}

df = pd.read_csv(temppath, encoding="gbk")

for i in df.columns:
    for j in tsdict.keys():
        if i == j:
            df.rename(columns={i: tsdict[j]},inplace=True)
df.drop(['Unnamed: 0'],axis=1,inplace=True)
df.sort_values(by=['交易日期'],ascending=True,inplace=True)
df.reset_index(drop=True,inplace=True)
df.to_csv(temppath, encoding="gbk")



os.startfile(temppath)
exit()



for i in range(len(df.index)):
    if i == 0:
        df.at[i,'复权因子']=(1+df.at[i,'涨跌幅']*0.01)
    else:
        df.at[i,'复权因子'] =(1+df.at[i,'涨跌幅']*0.01)*df.at[i-1,'复权因子']

# for i in range(len(df.index)):
#     if i == 0:
#         df.at[i,'收盘价_后复权'] = df.at[i,'收盘价']
#         df.at[len(df.index),'收盘价_前复权']= df.at[len(df.index),'收盘价']
#
#     else:
#         df.at[i,'收盘价_前复权']=df.at[i,'复权因子']*\
#                             (df.at[len(df.index)-1,'收盘价']/df.at[len(df.index)-1,'复权因子'])

df['开盘价_前复权'] = df['复权因子']*((df.at[len(df.index)-1,'开盘价']/df.at[len(df.index)-1,'复权因子']))
df['最高价_前复权'] = df['复权因子']*((df.at[len(df.index)-1,'最高价']/df.at[len(df.index)-1,'复权因子']))
df['最低价_前复权'] = df['复权因子']*((df.at[len(df.index)-1,'最低价']/df.at[len(df.index)-1,'复权因子']))
df['收盘价_前复权'] = df['复权因子']*((df.at[len(df.index)-1,'收盘价']/df.at[len(df.index)-1,'复权因子']))

df['开盘价_后复权'] = df['复权因子']*((df.at[0,'开盘价']/df.at[0,'复权因子']))
df['最高价_后复权'] = df['复权因子']*((df.at[0,'最高价']/df.at[0,'复权因子']))
df['最低价_后复权'] = df['复权因子']*((df.at[0,'最低价']/df.at[0,'复权因子']))
df['收盘价_后复权'] = df['复权因子']*((df.at[0,'收盘价']/df.at[0,'复权因子']))

pd.options.display.max_rows = None
pd.options.display.max_columns = None
df.to_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799_5.csv', encoding="gbk")

