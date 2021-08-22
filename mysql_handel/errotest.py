import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
from  def_class import BasicClass as bc
import xlwings as xw
from inspect import getsource
import datetime
import pymysql

# df 数据处理
path = "C:\\Users\\Admin\\Desktop\\py\\stockbasic"
filepath = path+'\\trade_cal.csv'

df = pd.read_csv(filepath,encoding='gbk')
df = df[['cal_date','is_open']]
a = df.values
for i in range(len(a)):
    print(a[i][0],a[i][1])


exit()
# mysql插入数据
db_name = 'test'
tablename1 = 'my_capital'
tablename2 = 'my_stock_pool'
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db= db_name, charset='utf8',local_infile=1)
cur = conn.cursor()
sql = 'select stock_code from it group by stock_code'
cur.execute(sql)
stocktupe = cur.fetchall()
df = pd.DataFrame(stocktupe)
codelist = df[0].to_list()
sql = "truncate table my_stock_pool"
cur.execute(sql)
conn.commit()

for code in codelist:

    sql_insert = "INSERT IGNORE INTO {} ".format(tablename2) + \
                 "(stock_code,buy_price,hold_vol,hold_days)" \
                 " VALUES ('%s', '%.2f', '%d','%d')" \
                 % (code, 0, 0, 0)
    cur.execute(sql_insert)



conn.commit()

cur.close()
conn.close()


exit()
# mysql建表；
db_name = 'test'
tablename1 = 'my_capital'
tablename2 = 'my_stock_pool'
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db= db_name, charset='utf8',local_infile=1)
cur = conn.cursor()
sql1 = "CREATE TABLE IF NOT EXISTS {} (" \
           "capital decimal(20, 4) NULL DEFAULT NULL," \
           "money_lock decimal(20, 4) NULL DEFAULT NULL," \
           "money_rest decimal(20, 4) NULL DEFAULT NULL," \
           "deal_action varchar(255) NULL DEFAULT NULL," \
           "stock_code varchar(255) NULL DEFAULT NULL," \
           "deal_price decimal(20, 4) NULL DEFAULT NULL," \
           "stock_vol int(11) NULL DEFAULT NULL," \
           "profit decimal(20, 4) NULL DEFAULT NULL," \
           "profit_rate decimal(20, 4) NULL DEFAULT NULL," \
           "bz varchar(255) NULL DEFAULT NULL," \
           "state_dt varchar(255) NULL DEFAULT NULL," \
           "seq int(11) NOT NULL,PRIMARY KEY (seq))".format(tablename1)

sql2 = "CREATE TABLE IF NOT EXISTS {} (" \
           "stock_code varchar(255) NULL DEFAULT NULL," \
           "buy_price decimal(20, 2) NULL DEFAULT NULL," \
           "hold_vol int(11) NULL DEFAULT NULL," \
           "hold_days int(11) NULL DEFAULT NULL)".format(tablename2)

cur.execute(sql2)
conn.commit()

cur.close()
conn.close()

exit()


db_name = 'test'
tablename = 'model_ev_mid'
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db= db_name, charset='utf8',local_infile=1)
cur = conn.cursor()
sql1 = "CREATE TABLE IF NOT EXISTS {} (" \
           "state_dt varchar(255) NOT NULL," \
           "stock_code varchar(255) NOT NULL," \
           "acc decimal(20, 4) NULL DEFAULT NULL," \
           "recall decimal(20, 4) NULL DEFAULT NULL," \
           "f1 decimal(20, 4) NULL DEFAULT NULL," \
           "acc_neg decimal(20, 4) NULL DEFAULT NULL," \
           "bz varchar(255) NULL DEFAULT NULL," \
           "predict varchar(255) NULL DEFAULT NULL)".format(tablename)

sql2 = "CREATE TABLE IF NOT EXISTS {} (" \
           "state_dt varchar(255) NOT NULL," \
           "stock_code varchar(255) NOT NULL," \
           "resu_predict decimal(20, 2) NULL DEFAULT NULL," \
           "resu_real decimal(20, 2) NULL DEFAULT NULL)".format(tablename)

cur.execute(sql2)
conn.commit()

cur.close()
conn.close()
exit()

today=datetime.date.today().strftime("%Y-%m-%d")
thisweek = bc.get_week_monday_and_sunday_by_date(today)
date_list = list()
date_list.append(thisweek)
df_week = pd.DataFrame(date_list).applymap(lambda x: x.strftime('%Y-%m-%d'))
for index,row in df_week.iterrows():
    st =row[0]
    en =row[1]
    print(st,en)

exit()


dateinter =bc.get_all_monday_and_sunday_by_date_interval('2021-02-26','2021-05-07')
df_week = pd.DataFrame(dateinter).applymap(lambda x: x.strftime('%Y-%m-%d'))
for index,row in df_week.iterrows():
    st =row[0]
    en =row[1]
    print(st,en)


# print(df_week)
exit()

db_name = 'stock_test'
dirpath = "D:\\Astock\\中小板\\IT设备"

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='stock_test', charset='utf8',local_infile=1)
# conn.autocommit(1)
cur = conn.cursor()

k = 0
for root,dirs,filenames in os.walk(dirpath):
    for name in filenames:
        csvpath = os.path.join(root,name)
        df = pd.read_csv(csvpath,encoding='gbk')
        df.drop(columns='Unnamed: 0',inplace=True)
        df = df.fillna('')

        df['ts_code'] = df['ts_code'].astype('str')
        # df['trade_date']=df['trade_date']
        df['open'] = df['open'].astype('float')
        df['high'] = df['high'].astype('float')
        df['low'] = df['low'].astype('float')
        df['close'] = df['close'].astype('float')
        df['pre_close'] = df['pre_close'].astype('float')
        df['change'] = df['change'].astype('float')
        df['pct_chg'] = df['pct_chg'].astype('float')
        df['vol'] = df['vol'].astype('int')
        df['amount'] = df['amount'].astype('float')

        values = df.values.tolist()
        # c_len = df.shape[0]
        s = "'%s'" + ', ' + "'%i'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + \
            "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%i'" + ', ' + "'%.2f'"
        tabel_key = 'stock_code,state_dt,open,high,low,close,pre_close,amt_change,pct_change,vol,amount'

        try:
            # sql_insert = "INSERT IGNORE INTO test (state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
            # sql_insert = "INSERT IGNORE INTO test (state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) " \
            #              "VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f') " \
            #              % ( state_dt, str(resu[0]), float(resu[2]), float(resu[5]), float(resu[3]), float(resu[4]), float(resu[9]),
            #                  float(resu[10]), float(resu[6]), float(resu[7]), float(resu[8]))
            sql_insert = 'INSERT IGNORE INTO test ({}) VALUES ({})'.format(tabel_key, s)
            cur.executemany(sql_insert, values)
            # conn.commit()

        except Exception as err:
                continue


        # values = df.values.tolist()
        # s = ','.join(['%s' for _ in range(len(df.columns))])
        # # insert_sql = 'INSERT INTO {} VALUES ({})'.format(table_name, s)
        # # print(insert_sql)
        # # executemany批量操作 插入数据 批量操作比逐个操作速度快很多
        # cur.executemany('INSERT INTO {} VALUES ({})'.format(table_name, s), values)
        conn.commit()
        k = k+1
        print(k)


cur.close()
conn.close()
exit()




exit()
# sql = 'SELECT * FROM sh600601 where Unnamed0 = 714'
#
# conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='astock', charset='utf8',local_infile=1)
# conn.autocommit(1)
# cur = conn.cursor()
#
# df = pd.read_sql(sql,conn)
# df.to_csv("D:\\1.csv",encoding='gbk')
# print(df)
# os.startfile("D:\\1.csv")
# cur.close()
# conn.close()
#
# exit()
db_name = 'stock_test'
dirpath = "D:\\Astock\\中小板"

def make_table_sql(df):

    global str_sql,char
    columns = df.columns.tolist()

    types = df.dtypes
    # 添加id 制动递增主键模式
    make_table = []
    for item in columns:

        if 'int64' in str(types[item]):
            char = item + ' INT'
        elif 'float64' in str(types[item]):
            char = item + ' FLOAT'
        elif 'object' in str(types[item]):
            char = item + ' VARCHAR(255)'
        elif 'datetime' in str(types[item]):
            char = item + ' DATETIME'
        make_table.append(char)
        str_sql = ','.join(make_table)
    str_sql = str_sql.replace('Unnamed: ', 'Unnamed').replace('change','chg')
    return str_sql


db_name = 'astock'
dirpath = "D:\\Astock\\主板\\橡胶"

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='astock', charset='utf8',local_infile=1)
conn.autocommit(1)
cur = conn.cursor()

k = 0
for root,dirs,filenames in os.walk(dirpath):
    for name in filenames:
        table_name = name.split('.')[1]+name.split('.')[0]

        sql1 = 'drop table {}'.format(table_name)
        # sql2 = 'alter table {} modify {} DATE'.format(table_name, 'trade_date')


        cur.execute(sql1)
        # cur.execute(sql2)
        k = k+1
        print(k)


cur.close()
conn.close()

exit()



db_name = 'astock'
dirpath = "D:\\Astock\\主板"
csvpath = "D:\\600601.SH.csv"

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='astock', charset='utf8',local_infile=1)
conn.autocommit(1)
cur = conn.cursor()

cur.execute('CREATE DATABASE IF NOT EXISTS {}'.format(db_name))
conn.select_db(db_name)

df = pd.read_csv(csvpath, encoding='gbk')
df = df.fillna('')
table_name = 'SH600601'
str_comment = '主板_IT设备'
str_comment = "'{}'".format(str_comment)
sql1 = 'DROP TABLE IF EXISTS {}'.format(table_name)
sql2 = 'CREATE TABLE {}({})'.format(table_name, make_table_sql(df)) + "COMMENT=" + str_comment
cur.execute(sql1)
cur.execute(sql2)

values = df.values.tolist()
s = ','.join(['%s' for _ in range(len(df.columns))])
# insert_sql = 'INSERT INTO {} VALUES ({})'.format(table_name, s)
# print(insert_sql)
# executemany批量操作 插入数据 批量操作比逐个操作速度快很多
cur.executemany('INSERT INTO {} VALUES ({})'.format(table_name, s), values)

cur.close()
conn.close()
