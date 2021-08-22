import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import datacompy
import pymysql

# filepath = "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockT.csv"
# df1 = pd.read_csv(filepath,encoding='gbk')
# df1 = df1[['ts_code','name','area','industry','fullname','enname','market','list_date','delist_date','is_hs']]
# df1.rename(columns={'ts_code':'stock_code'},inplace=True)
# df1 = df1[['stock_code']]

conn1 = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8',local_infile=1)
df1 = pd.read_sql(sql="select * from stock_all where stock_code = '002151.SZ'",con=conn1)

conn2 = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='中小板', charset='utf8',local_infile=1)
df2 = pd.read_sql(sql="select * from 通信设备 where stock_code = '002151.SZ'",con=conn2)

compare=datacompy.Compare(df1,df2,join_columns='stock_code')
print(compare.report())

exit()

starpath1 = "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL.csv"
starpath2= "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL0614.csv"

df1 = pd.read_csv(starpath1,encoding='gbk')
df1 = df1[df1['market']=='主板']
df2 = pd.read_csv(starpath2,encoding='gbk')
df2 = df2[df2['market']=='主板']

compare=datacompy.Compare(df1,df2,join_columns='ts_code')
print(compare.report())
exit()
list = [1,2,3]
print(DataFrame(list,index=['a']))
exit()


# import tushare as ts
#
# ts.set_token("a9a90478a856a428524e0fa340936b42d00a68e7cc8e699433643ff0")
# pro = ts.pro_api()
# df = pro.trade_cal(exchange='', start_date='20201231', end_date='20210101',
#                    fields='exchange,cal_date,is_open,pretrade_date', is_open='0')

# 数据爬取
# dflist = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\2.csv",encoding="gbk")
# gpcodelist = dflist["stockcode"].values.tolist()
#
# # gpcodelist = ["sh600000","sh688656","sh688095","sh688037"]
#
# listarray = []
# list1 = []
# for i in range(len(gpcodelist)):
#     listarray.append([])
#     gpcode = gpcodelist[i]
#     url = "https://hq.sinajs.cn/list=" + gpcode
#     r = requests.get(url)
#     if r.status_code != 200:
#         raise Exception()
#     r.encoding = "gbk"
#     html_doc = r.text
#     soup = BeautifulSoup(html_doc, "html.parser")
#     strsina = soup.text
#     list1 = strsina.split(",")
#     for j in range(len(list1)):
#         listarray[i].append(list1[j])
#     listarray[i][0] = listarray[i][0][21:]

# 数据处理
# df = pd.DataFrame(listarray)
df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\1.csv", encoding="gbk",index_col="0")
# df.set_index(["0"],inplace=True)
pd.set_option('display.max_columns',30)
# df.to_csv("C:\\Users\\Admin\\Desktop\\py\\1.csv", encoding="gbk")
df.plot.bar(y=["1","2","3","4"])
# plt.plot(df["1"])
# plt.text()
plt.show()
# print(df)










# tfinds = soup.find_all("div", {"id": "price"})

#

# for i in range(len(strsina.split(";"))):
#     str = strsina.split(";")[i]
    # str1 = pd.read_csv(str);
    # str1.to_csv("C:\Users\Admin\Desktop\文件夹\data.csv")
    # print(str)



# print(strsina)

# strsina = "var hq_str_sh600000"+"="\
#     "浦发银行,9.830,9.840,9.870,9.960,9.760,9.860,9.870,55338565,545908487.000,5100,9.860," \
#     "79400,9.850,77900,9.840,742800,9.830,289600,9.820,54460,9.870,588510,9.880,428400,9.890," \
#     "417177,9.900,253333,9.910,2021-01-14,15:00:01,00,"+";"+"\n"+"var hq_str_sh600000"+"="\
#     "中国银行,9.830,9.840,9.870,9.960,9.760,9.860,9.870,55338565,545908487.000,5100,9.860," \
#     "79400,9.850,77900,9.840,742800,9.830,289600,9.820,54460,9.870,588510,9.880,428400,9.890," \
#     "417177,9.900,253333,9.910,2021-01-14,15:00:01,00,"+";"

# url = "https://finance.sina.com.cn/realstock/company/sh600000/nc.shtml"
# url = "http://n.sinaimg.cn/finance/realstock/company/fixTotalShare.js?v=1.1.0"
