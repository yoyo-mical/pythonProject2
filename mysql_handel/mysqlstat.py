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
import pymysql
import requests
import tushare as ts
import datetime
import time

# nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
today=datetime.date.today().strftime("%Y-%m-%d")

db_name = '主板'

basicpath = 'C:\\Users\\Admin\\Desktop\py\\stockbasic\\stockL.csv'

df_basic = pd.read_csv(basicpath, encoding='gbk')
df_basic = df_basic[df_basic['market'] == db_name]
df_basic = df_basic[['ts_code', 'name']]

statmark = 'thisweek'

if statmark == 'thisweek':
    thisweek = bc.get_week_monday_and_sunday_by_date(today)
    thisweek_list = list()
    thisweek_list.append(thisweek)
    df_week = pd.DataFrame(thisweek_list).applymap(lambda x: x.strftime('%Y-%m-%d'))
else:
    dateinter = bc.get_all_monday_and_sunday_by_date_interval('2021-06-02', '2021-06-04')
    df_week = pd.DataFrame(dateinter).applymap(lambda x: x.strftime('%Y-%m-%d'))

for index,row in df_week.iterrows():
    st =row[0]
    en =row[1]

    sht = db_name + st + '_' + en
    sortpath = 'D:\\Astock_Save\\主板\\' + db_name + st + '_' + en + '.csv'
    # sortpath = 'D:\\Astock_Save\\主板\\'+db_name+'.csv'

    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db=db_name, charset='utf8', local_infile=1)
    # conn.autocommit(1)
    cur = conn.cursor()
    table_name = ''
    code = ''
    price = 1
    buflist = []
    nulllist = []
    n = 0
    sql_table = "select table_name from information_schema.tables where table_schema='{}'".format(db_name)

    try:
        cur.execute(sql_table)
        result1 = cur.fetchall()
        df1 = pd.DataFrame(result1)
        tabel_list = df1[0].to_list()
        for table_name in tabel_list:
            sql_codename = 'select stock_code from {} group by stock_code'.format(table_name)
            cur.execute(sql_codename)
            result2 = cur.fetchall()
            df2 = pd.DataFrame(result2)
            code_list = df2[0].to_list()
            for code in code_list:
                try:
                    # sql1 = "select * from {} where state_dt>'2021-06-04' and stock_code = '{}'".format(table_name, code)
                    sql1 = "select * from {} where state_dt between '{}' and '{}' and stock_code = '{}'".format(
                        table_name, st, en, code)
                    df3 = pd.read_sql(sql1, conn)
                    for i in range(len(df3.index)):
                        if i == 0:
                            df3.at[i, 'buf'] = (1 + df3.at[i, 'pct_change'] * 0.01) * price
                        else:
                            df3.at[i, 'buf'] = (1 + df3.at[i, 'pct_change'] * 0.01) * df3.at[i - 1, 'buf']
                            # print(df3.at[len(df3.index) - 1, 'buf'])
                    buflist.append([code, table_name, df3.at[len(df3.index) - 1, 'buf']])
                    n = n + 1
                    print(n, code, table_name)
                except Exception as e:
                    nulllist.append([code, table_name])
                    print(e, code, table_name)

    except Exception as e:
        print(e)

    if buflist != []:
        df_buf = pd.DataFrame(buflist, columns=['ts_code', 'industry', 'earn'])
        df_merge = pd.merge(df_basic, df_buf, on='ts_code')
        df_merge.sort_values(by='earn', inplace=True, ascending=False)
        pd.options.display.max_rows = None
        df_merge.to_csv(sortpath, encoding='gbk')
        # bc.excelshow2(sortpath, sht)
        # os.startfile(sortpath)

    else:
        print('此段时间未有交易')

    cur.close()
    conn.close()

    if nulllist != []:
        df_null = pd.DataFrame(nulllist, columns=['ts_code', 'industry'])
        print(df_null)

exit()








