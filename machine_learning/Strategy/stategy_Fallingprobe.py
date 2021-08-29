import pymysql.cursors
import pandas as pd
import datetime

# 寻找连续下跌stock,进行买卖回测；

falllist = []
db_name = '主板'
db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db=db_name, charset='utf8')
cursor = db.cursor()


sql1 = "select table_name from information_schema.tables where table_schema='{}'".format(db_name)
cursor.execute(sql1)
result1 = cursor.fetchall()
df1 = pd.DataFrame(result1)
tabel_list = df1[0].to_list()


date_ = '2021-08-28'
for tb in tabel_list:
    sql2 = "select stock_code from {} group by stock_code".format(tb)
    cursor.execute(sql2)
    result2 = cursor.fetchall()
    df2 = pd.DataFrame(result2)
    codelist = df2[0].to_list()
    for code in codelist:
        n = 0
        try:
            sql3 = "select pct_change from {tb} where stock_code = '{code}' " \
                   "and state_dt< '{date_}'order by state_dt desc".format(tb=tb, code=code, date_=date_)
            cursor.execute(sql3)
            result3 = cursor.fetchall()
            df3 = pd.DataFrame(result3)
            pctlist = df3[0].to_list()
            for pct in pctlist:
                if pct <= 0:
                    n = n + 1
                else:
                    break
            tup = (code, n)
            falllist.append(tup)
        except:
            print(tb, code)

print(falllist)
df4 = pd.DataFrame(falllist)
print(df4)
print(df4.sort_values(by=[1],ascending=False))

db.commit()
db.close()