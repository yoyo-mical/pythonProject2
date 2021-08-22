import pandas as pd
import numpy as np
import sys
import os
import csv
from def_class import BasicClass as bc
from inspect import getsource
import datetime
import pymysql

# csv data to mysql
path = "C:\\Users\\Admin\\Desktop\\py\\stockbasic"
filepath = path+'\\stockP.csv'

df = pd.read_csv(filepath,encoding='gbk')
df['enname'].replace({"'":","},regex=True,inplace=True)
df = df[['ts_code','name','area','industry','fullname','enname','market','list_date','delist_date','is_hs']]
# pd.options.display.max_rows = None
# pd.options.display.max_columns = None
# print(df)
# exit()
basic_list = df.values

db_name = 'test'
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db=db_name, charset='utf8',local_infile=1)
# conn.autocommit(1)
cur = conn.cursor()
try:
    for basic in basic_list:
        list_date = (datetime.datetime.strptime(str(basic[7]),"%Y%m%d")).strftime("%Y-%m-%d")
        if np.isnan(basic[8]):
            delist_date=''
        else:
            delist_date = (datetime.datetime.strptime(str(int(basic[8])), "%Y%m%d")).strftime("%Y-%m-%d")

        sql_insert1 = "INSERT IGNORE INTO basic (stock_code,name,area,industry,fullname,enname,market,list_date,delist_date,is_hs) " \
                     "value ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"\
            .format(basic[0],basic[1],basic[2],basic[3],basic[4],basic[5],basic[6],list_date,delist_date,basic[9])
        print(sql_insert1)
        cur.execute(sql_insert1)
    # conn.commit()
except Exception as err:
    print(err)
conn.commit()

# sql_insert2 = 'alter TABLE basic add id int(11) primary key auto_increment FIRST'
# sql_insert3 = "DELETE FROM basic " \
#               "WHERE stock_code IN " \
#               "(SELECT stock_code FROM (SELECT stock_code from basic GROUP BY stock_code HAVING count(stock_code)>1) as t1) " \
#               "AND id NOT IN " \
#               "(SELECT t2.min_id FROM (SELECT min(id) AS min_id FROM basic GROUP BY stock_code HAVING count(stock_code)>1) as t2)"
# sql_insert4 = 'alter table basic drop id'
#
# cur.execute(sql_insert2)
# cur.execute(sql_insert3)
# cur.execute(sql_insert4)
# cur.execute(sql_insert2)
# conn.commit()

cur.close()
conn.close()
exit()




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

# 分区块导入
db_name = 'stock_test'
dirpath = "D:\\Astock\\科创板\\IT设备"
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
        c_len = df.shape[0]
        for j in range(c_len):
            resu = list(df.iloc[c_len-1-j])
            state_dt = (datetime.datetime.strptime(str(resu[1]), "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT IGNORE INTO 中小板 (state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) " \
                             "VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" \
                             % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                cur.execute(sql_insert)
                # conn.commit()
            except Exception as err:
                continue
        conn.commit()
        k = k+1
        print(k)


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







