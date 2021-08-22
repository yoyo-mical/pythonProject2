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

starpath = "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL0614.csv"
data = pro.query('stock_basic', exchange='', list_status='L',
                 fields='ts_code,symbol,name,area,industry,fullname,'
                        'enname,market,curr_type,list_date,delist_date,is_hs')
data.to_csv(starpath,encoding='gbk')

exit()
# pro = ts.pro_api()

starpath = 'C:\\Users\\Admin\\Desktop\\py\\temp\\0606_2.csv'
df = pro.query('daily', ts_code='300023.SZ', start_date='20210320', end_date='20210606')
# df = ts.pro_bar(ts_code='000001.SZ', adj='None', start_date='20210312', end_date='20210501',adjfactor=True)
# df.sort_values(by='trade_date',inplace=True)
# df.reset_index (drop=True,inplace = True)
df.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\0606_2.csv',encoding="gbk")

os.startfile(starpath)
exit()

today = datetime.date.today().strftime('%Y%m%d')

listtup1 = []

bakpath = 'C:\\Users\\Admin\\Desktop\\py\\bak\\主板'
mainpath = 'C:\\Users\\Admin\\Desktop\\py\\stock\\主板'

sa = bc.stockbasic('L')
keyind = '综合类'
dirpath = bakpath +'\\'+keyind
bc.del_file(dirpath)
maindirpath = mainpath +'\\'+keyind
bc.del_file(maindirpath)

sa.groupby_(['market','industry'],keyone='主板',keytwo=keyind)
df1 = sa.dfgp[['ts_code','list_date']]

for i in range(len(df1.index)):
    index_= df1.index[i]
    tup = (df1.at[index_,'ts_code'],df1.at[index_,'list_date'])
    listtup1.append(tup)

for i in range(len(listtup1)):
    filepath = dirpath + '\\'+listtup1[i][0]+'1.csv'
    df = pro.query('daily', ts_code=listtup1[i][0], start_date=str(listtup1[i][1]), end_date=today)
    df.to_csv(filepath, encoding="gbk")


df2 = bc.tradedate(dirpath)
df = pd.merge(df1,df2,on='ts_code')

df['list_date'] = pd.to_datetime(df['list_date'],format='%Y%m%d')
df['trade_date'] = pd.to_datetime(df['trade_date'],format='%Y%m%d')

df['dategap'] = df['trade_date'] != df['list_date']
df['dategap_de']= df['trade_date'] == df['list_date']
df['1'] = df['trade_date']+ datetime.timedelta(days=-1)
df['start_'] = df['list_date'].apply(lambda x:x.strftime('%Y%m%d'))
df['end_'] = df['1'].apply(lambda x:x.strftime('%Y%m%d'))


listtup2 =[]
df_ob = df[df['dategap']][['ts_code','start_','end_']]
df_de = df[df['dategap_de']][['ts_code','start_','end_']]
listde = df_de['ts_code'].to_list()
print(df_ob,df_de)
df_ob.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\temp1.csv',encoding='gbk')
df_de.to_csv('C:\\Users\\Admin\\Desktop\\py\\temp\\temp2.csv',encoding='gbk')


for i in range(len(df_ob.index)):
    index_= df_ob.index[i]
    tup = (df_ob.at[index_,'ts_code'],df_ob.at[index_,'start_'],df_ob.at[index_,'end_'])
    listtup2.append(tup)

for i in range(len(listtup2)):
    filepath1 = dirpath + '\\' + listtup2[i][0] + '1.csv'
    df1 = pd.read_csv(filepath1,encoding='gbk')

    filepath2 = dirpath + '\\'+listtup2[i][0]+'2.csv'
    df2 = pro.query('daily', ts_code=listtup2[i][0], start_date=listtup2[i][1], end_date=listtup2[i][2])
    df2.to_csv(filepath2, encoding="gbk")

    df = pd.concat([df1, df2], ignore_index=True)
    df.drop(['Unnamed: 0'],axis=1,inplace=True)
    df.to_csv(maindirpath + '\\' + listtup2[i][0] + '.csv', encoding='gbk')


for name in listde:
    copyfile(dirpath + '\\' + name + '1.csv',maindirpath+'\\' + name + '.csv')

os.startfile(maindirpath)
exit()



# tscode= '600001.SH'


# ts.set_token("a9a90478a856a428524e0fa340936b42d00a68e7cc8e699433643ff0")

# df = pro.query('stock_basic', exchange='', list_status='P',
#                  fields='ts_code,symbol,name,area,industry,'
#                         'list_date,delist_date,fullname,enname,'
#                         'market,is_hs,curr_type')
# df.to_csv("C:\\Users\\Admin\\Desktop\\py\\stockP.csv", encoding="gbk")
# exit()

df = pro.query('daily', ts_code='000799.SZ', start_date='19970718', end_date='19970718')
df.to_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799_3.csv', encoding="gbk")
exit()

df = pro.daily(ts_code='600001.SH', start_date='19940701', end_date='20180718')
df.to_csv('C:\\Users\\Admin\\Desktop\\py\\600001.csv', encoding="gbk")
exit()

def tsdaily(tscode,sd,ed):
    for i in range(len(tscode)):
        df = pro.daily(ts_code=tscode[i], start_date=sd, end_date=ed)
        df.to_csv('C:\\Users\\Admin\\Desktop\\py\\'+tscode[i]+'.csv', encoding="gbk")
        return df

tsdaily(tscode,'19940201','20210201')
exit()

def tradecal(sd,ed):
    df = pro.trade_cal(exchange='SZSE', start_date=sd, end_date=ed)
    df.to_csv('C:\\Users\\Admin\\Desktop\\py\\tsadecal.csv', encoding="gbk")

def dailybasic(tscode):
    for i in range(len(tscode)):
        for j in range(len(yeared)):
            sd = yearsd[j] + '0210';ed = yeared[j] + '0209'
            df = pro.query('daily_basic', ts_code=tscode[i], start_date=sd, end_date=ed,
                           fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pb,pe_ttm,'
                                  'ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,'
                                  'circ_mv,total_mv,close')
            df.to_csv('C:\\Users\\Admin\\Desktop\\py\\jiugui\\' + tscode[i] + yearsd[j]+'.csv', encoding="gbk")


# yearsd = ['2016','2015','2014','2013','2012','2011','2010','2009']
# yeared = ['2017','2016','2015','2014','2013','2012','2011','2010']
# sd = '20190210';ed = '20210209'
# df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stock.csv",encoding="gbk")
# df1 = df1.groupby('industry').get_group('白酒')
# tscode = df1['ts_code'].tolist()

# tscode = ['000799.SZ']
#
# dailybasic(tscode)


# df = pro.income(ts_code='000799.SZ', start_date='20100101', end_date='20201230',
#                 fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps'
#                        ',operate_profit,total_profit,n_income,n_income_attr_p,total_revenue')
# df.to_csv('C:\\Users\\Admin\\Desktop\\py\\000799.SZlirun.csv', encoding="gbk")
# df = pro.namechange(ts_code='600809.SH', fields='ts_code,name,start_date,end_date,change_reason')
# df.to_csv('C:\\Users\\Admin\\Desktop\\py\\namechange.csv', encoding="gbk")


# df = pro.query('top10_holders', ts_code='000799.SZ', start_date=sd, end_date=ed)
# df.to_csv('C:\\Users\\Admin\\Desktop\\py\\000799hold.csv', encoding="gbk")

# print(df)

# df = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
# df = pro.query('daily_basic', ts_code='600197.SH ', start_date='20200209',end_date='20210210',
#                fields='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pb,pe_ttm,'
#                       'ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,'
#                       'circ_mv,total_mv,close')

# df = ts.pro_bar(ts_code='600197.SH', adj='qfq', start_date='20200209',end_date='20210210')
# pd.options.display.max_columns = None
# # df.to_csv("C:\\Users\\Admin\\Desktop\\py\\stockindex.csv", encoding="gbk")
# print(df)

# df = pro.daily(trade_date='20210129')
# df.to_csv("C:\\Users\\Admin\\Desktop\\py\\stockdaliy.csv", encoding="gbk")
# print(df)

# df = pro.fund_basic(market='E')
# df.to_csv("C:\\Users\\Admin\\Desktop\\py\\fund.csv", encoding="gbk")



# df.plot.line(x="trade_date", y=["close", "open"])
# print(df)
# plt.show()
