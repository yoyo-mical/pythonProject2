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

pro = ts.pro_api()

today = datetime.date.today().strftime('%Y%m%d')

listtup1 = []

keymar= 'CDR'

bakpath = 'C:\\Users\\Admin\\Desktop\\py\\bak\\'+keymar
mainpath = 'C:\\Users\\Admin\\Desktop\\py\\stock\\'+keymar

sa = bc.stockbasic('L')
sa.groupby_(['market','industry'],keyone= keymar)
induslist = sa.gpnamelist
# induslist = induslist[73:]

num = 0

for induname in induslist:

    num = num + 1
    keyind = induname
    dirpath = bakpath + '\\' + keyind
    bc.del_file(dirpath)
    maindirpath = mainpath + '\\' + keyind
    bc.del_file(maindirpath)
    listtup1 = []

    sa.groupby_(['market', 'industry'], keyone=keymar, keytwo=keyind)
    df1 = sa.dfgp[['ts_code', 'list_date']]


    for i in range(len(df1.index)):
        index_ = df1.index[i]
        tup = (df1.at[index_, 'ts_code'], df1.at[index_, 'list_date'])
        listtup1.append(tup)

    for i in range(len(listtup1)):
        filepath = dirpath + '\\' + listtup1[i][0] + '1.csv'
        df = pro.query('daily', ts_code=listtup1[i][0], start_date=str(listtup1[i][1]), end_date=today)
        df.to_csv(filepath, encoding="gbk")

    df2 = bc.tradedate(dirpath)
    df = pd.merge(df1, df2, on='ts_code')

    df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d')
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

    df['dategap'] = df['trade_date'] != df['list_date']
    df['dategap_de'] = df['trade_date'] == df['list_date']
    df['1'] = df['trade_date'] + datetime.timedelta(days=-1)
    df['start_'] = df['list_date'].apply(lambda x: x.strftime('%Y%m%d'))
    df['end_'] = df['1'].apply(lambda x: x.strftime('%Y%m%d'))

    listtup2 = []
    df_ob = df[df['dategap']][['ts_code', 'start_', 'end_']]
    df_de = df[df['dategap_de']][['ts_code', 'trade_date', 'list_date']]
    listde = df_de['ts_code'].to_list()

    print(induname, len(induslist) - num)

    if df_ob.empty:
        print(df_de)
        # df_de.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\temp2.csv', encoding='gbk')
        for name in listde:
            copyfile(dirpath + '\\' + name + '1.csv', maindirpath + '\\' + name + '.csv')
    else:
        print(df_ob)
        print(df_de)
        for i in range(len(df_ob.index)):
            index_ = df_ob.index[i]
            tup = (df_ob.at[index_, 'ts_code'], df_ob.at[index_, 'start_'], df_ob.at[index_, 'end_'])
            listtup2.append(tup)

        for i in range(len(listtup2)):
            filepath1 = dirpath + '\\' + listtup2[i][0] + '1.csv'
            df1 = pd.read_csv(filepath1, encoding='gbk')

            filepath2 = dirpath + '\\' + listtup2[i][0] + '2.csv'
            df2 = pro.query('daily', ts_code=listtup2[i][0], start_date=listtup2[i][1], end_date=listtup2[i][2])
            df2.to_csv(filepath2, encoding="gbk")

            df = pd.concat([df1, df2], ignore_index=True)
            df.drop(['Unnamed: 0'], axis=1, inplace=True)
            df.to_csv(maindirpath + '\\' + listtup2[i][0] + '.csv', encoding='gbk')

        for name in listde:
            copyfile(dirpath + '\\' + name + '1.csv', maindirpath + '\\' + name + '.csv')

        # df_ob.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\temp1.csv', encoding='gbk')
        # df_de.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\temp2.csv', encoding='gbk')

# os.startfile(maindirpath)

exit()