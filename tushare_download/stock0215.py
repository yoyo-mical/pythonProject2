import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys
import os




# print(list.__str__())
#
# exit()

# class test():
#     def __init__(self,data=1):
#         self.data = data
#
#     def __iter__(self):
#         return self
#     def __next__(self):
#         if self.data > 5:
#             raise StopIteration
#         else:
#             self.data+=1
#             return self.data
# t = test(3)
# for item in t:
#     print(item)
#
# exit()



# class test():
#     def __init__(self,n):
#         self.n = n
#         print(self.n)
#
#     def __iter__(self):
#         print('调用')
#         return  self
#     def __next__(self):
#         if self.n >= 8:
#             raise StopIteration
#         else:
#             self.n = self.n+1
#             return self.n
#
#
# t = test(3)
# for item in t:
#
#     print(item)
#     t.n=8
#
# # print(dir(test))
# exit()


# print(range(10).__iter__().__next__())
# exit()


# df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\000799.SZlirun.csv",encoding="gbk")


#
# df1['end_date']=df1['end_date'].apply(str)
# df1['end_date'] = pd.to_datetime(df1['end_date'], format='%Y-%m-%d')
# df1 = df1.set_index('end_date')
# df1=df1.dropna()
# df1.plot.bar(y=['n_income_attr_p'])
# plt.gca().invert_xaxis()
# plt.tight_layout()
# plt.show()
# exit()


# df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL.csv", encoding="gbk")
# df2 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockD.csv", encoding="gbk")
# df3 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockP.csv", encoding="gbk")
#
# df4 = pd.concat([df1,df2,df3],axis=0,ignore_index=True)
# df4.sort_values(by='ts_code')
# df4.to_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockT.csv", encoding="gbk")
# exit()


# df1=pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799_1.csv', encoding="gbk")
# df2=pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799_2.csv', encoding="gbk")
# df3=pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\namechange1.csv', encoding="gbk")
# df = pd.concat([df1,df2],axis= 0,join='outer',ignore_index= False)
# df.reset_index(drop=True)
# # df['0'].drop()
#
# df.drop_duplicates(subset='trade_date',keep='first',inplace=True)
# # df.reset_index(drop=True)
#
# df3['start_date'] = df3['start_date'].apply(str)
# df3['start_date'] = pd.to_datetime(df3['start_date'],format='%Y-%m-%d')
#
# df3['end_date'] = df3['end_date'].apply(str)
# df3['end_date'] = pd.to_datetime(df3['end_date'],format='%Y-%m-%d')
#
# # print(df3['start_date'])
# # exit()
#
#
# df["trade_date"] = df["trade_date"].apply(str)
# df["trade_date"] = pd.to_datetime(df["trade_date"], format='%Y-%m-%d')
# df.set_index('trade_date',inplace=True)
# df.sort_index(ascending=True,inplace=True)
# df.drop(['Unnamed: 0'],axis=1,inplace=True)
# df.reset_index(inplace=True)
#
#
# for i in range(len(df3.index)):
#     sd = df3.at[i,'start_date']; ed = df3.at[i,'end_date']
#     for j in range(len(df.index)):
#         if sd<=df.at[j,'trade_date']<=ed:
#             df.at[j,'name'] = df3.at[i,'name']
# # print(df)
# # df.to_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799.csv', encoding="gbk")
# exit()
# df2=pd.DataFrame
# list = []
# df1=pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\stock\\000799.csv', encoding="gbk")
#
# a=df1.__iter__().__next__()
# for _ in df1:
#     print(_)
# # print(a)
# exit()

    # if df1.at[i, 'close'] != df1.at[i + 1, 'pre_close']:
    #     print(df1.loc[i,'trade_date'],df1.loc[i,'close'],df1.loc[i+1,'pre_close'],df1.loc[i+1,'trade_date'])



# print(df2)
# exit()


# df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockT.csv", encoding="gbk")
# # pd.options.display.max_rows = None
# print(df1.loc[df1['name'].str.contains('酒鬼'),['ts_code','list_date','delist_date']])
#
# exit()


filePath = 'C:\\Users\\Admin\\Desktop\\py\\jiugui'
namelist = os.listdir(filePath)
# print(namelist)
# exit()

df3 = pd.DataFrame()
df4 = pd.DataFrame()
help(df3)
exit()

# df1 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stock.csv",encoding="gbk")
# # df2 = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockindex.csv", encoding="gbk")
# # df = pd.merge(df1,df2,how = 'inner',on='ts_code')
# df1 = df1.groupby('industry').get_group('白酒')
# pd.options.display.max_rows = None
# # print(df['name'].count())

carecode = 'pe_ttm'
for i in range(len(namelist)):
    df2 = pd.read_csv('C:\\Users\\Admin\\Desktop\\py\\jiugui\\' + namelist[i], encoding="gbk")

    # df2["trade_date"] = df2["trade_date"].apply(str)
    # df2["trade_date"] = pd.to_datetime(df2["trade_date"], format='%Y-%m-%d')
    # df2 = df2.set_index("trade_date")
    df3 = df2[carecode].to_frame()
    df3 = df3.rename(columns={carecode:namelist[i][9:13]})

    df3 = df3[namelist[i][9:13]].iloc[[0,-1]]
    df3 = df3.reset_index()



    if i == 0:
        df4 = df3
    else:
        df4 = pd.concat([df4, df3], axis=1)

df4= df4.drop(columns=['index'])


df4=df4.sort_index(ascending= False)
df4 = df4.T
print(df4)
# exit()
df4.plot.bar()
plt.gca().invert_xaxis()
plt.tight_layout()
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