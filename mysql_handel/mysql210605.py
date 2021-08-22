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

# 分区块导入
# db_name = '中小板'
dirpath = "D:\\Astock\\主板"
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='主板', charset='utf8',local_infile=1)
# conn.autocommit(1)
cur = conn.cursor()

k = 0

for root,dirs,filenames in os.walk(dirpath):
    dirname = root.split('\\')[-1]
    sql1 = "CREATE TABLE IF NOT EXISTS {} (" \
           "state_dt DATE NOT NULL," \
           "stock_code VARCHAR(255) NOT NULL COLLATE 'utf8_general_ci'," \
           "open FLOAT NULL DEFAULT NULL," \
           "high FLOAT NULL DEFAULT NULL," \
           "low FLOAT NULL DEFAULT NULL," \
           "close FLOAT NULL DEFAULT NULL," \
           "pre_close FLOAT NULL DEFAULT NULL," \
           "amt_change FLOAT NULL DEFAULT NULL," \
           "pct_change FLOAT NULL DEFAULT NULL," \
           "vol FLOAT NULL DEFAULT NULL," \
           "amount FLOAT NULL DEFAULT NULL," \
           "PRIMARY KEY (state_dt,stock_code))".format(dirname)
    cur.execute(sql1)
    sumcol = 0
    for name in filenames:
        print(dirname,name)
        csvpath = os.path.join(root,name)
        df = pd.read_csv(csvpath,encoding='gbk')
        df.drop(columns='Unnamed: 0',inplace=True)
        df = df.fillna('')
        c_len = df.shape[0]
        sumcol = sumcol+c_len
        for j in range(c_len):
            resu = list(df.iloc[c_len-1-j])
            state_dt = (datetime.datetime.strptime(str(resu[1]), "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT IGNORE INTO {} " .format(dirname) +\
                             "(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change)" \
                             " VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')"\
                             % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),
                                float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                cur.execute(sql_insert)
                # conn.commit()
            except Exception as err:
                continue
        conn.commit()
        k = k+1
        print(k,sumcol)


cur.close()
conn.close()

exit()

db_name = 'astock'
dirpath = "D:\\Astock\\科创板"

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='astock', charset='utf8',local_infile=1)
conn.autocommit(1)
cur = conn.cursor()

k = 0
for root,dirs,filenames in os.walk(dirpath):
    for name in filenames:
        csvpath = os.path.join(root,name)
        df = pd.read_csv(csvpath,encoding='gbk')
        df = df.fillna('')
        table_name = name.split('.')[1]+name.split('.')[0]
        # str_comment = root.split('\\')[2] + '_' + root.split('\\')[3]
        # str_comment = "'{}'" .format(str_comment)
        # sql1 = 'DROP TABLE IF EXISTS {}'.format(table_name)
        # sql2 = 'CREATE TABLE {}({})'.format(table_name, make_table_sql(df)) + "COMMENT=" + str_comment
        # cur.execute(sql1)
        # cur.execute(sql2)

        values = df.values.tolist()
        s = ','.join(['%s' for _ in range(len(df.columns))])
        # insert_sql = 'INSERT INTO {} VALUES ({})'.format(table_name, s)
        # print(insert_sql)
        # executemany批量操作 插入数据 批量操作比逐个操作速度快很多
        cur.executemany('INSERT INTO {} VALUES ({})'.format(table_name, s), values)
        k = k+1
        print(k)


cur.close()
conn.close()
exit()







