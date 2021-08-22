import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys

df4 = pd.DataFrame()
df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stock.csv",encoding="gbk")
# df2 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockindex.csv", encoding="gbk")
# df = pd.merge(df1,df2,how = 'inner',on='ts_code')
df1 = df1.groupby('industry').get_group('白酒')
pd.options.display.max_rows = None
# print(df['name'].count())
namelist = df1['name'].tolist()
tscodelist = df1['ts_code'].tolist()
carecode = 'close'
for i in range(len(tscodelist)):
    df2 = pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\baijiu\\' + tscodelist[i] + '.csv', encoding="gbk")
    df2["trade_date"] = df2["trade_date"].apply(str)
    df2["trade_date"] = pd.to_datetime(df2["trade_date"], format='%Y-%m-%d')
    df2 = df2.set_index("trade_date")
    df3 = df2[carecode].to_frame()
    df3 = df3.rename(columns={carecode:namelist[i]})
    if i == 0:
        df4 = df3
    else:
        df4 = pd.merge(df4,df3,how='outer',on = 'trade_date')


df4.sort_values(by = 'trade_date',ascending=False)

df4.plot.line(y=['酒鬼酒'])
plt.show()
exit()
df4.to_csv("C:\\Users\\Admin\\Desktop\\py\\baijiu2.csv",encoding="gbk")
df4=df4.T
df4['bili'] = (df4['20210210']-df4['20200210'])/df4['20200210']
print(df4['bili'].max(),df4['bili'].min())
# print(df4.columns)
df4.plot.bar(y=['bili'])
# df4.plot.bar(y=pd.to_datetime(['20210210','20200210'], format='%Y-%m-%d'))
# # df4.plot.line(y=['贵州茅台'])
plt.tight_layout()
plt.show()




# df1 =  pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\tsadecal.csv', encoding="gbk")
# df2 = df1[df1['is_open']==1]
# pd.options.display.max_rows = None
# print(df2['cal_date'].tolist())
