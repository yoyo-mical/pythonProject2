import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt


stardate = "2010-01-01"
enddate = "2010-01-10"
df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\sh600000.csv", encoding="gbk")
print(df['trade_date'].dtype)
df["trade_date"]=df["trade_date"].apply(str)
df["trade_date"]=pd.to_datetime(df["trade_date"],format ='%Y-%m-%d')
df = df.set_index("trade_date")

df = pd.DataFrame(df,index=pd.date_range(stardate,enddate))

pd.set_option('display.max_columns',15)

# df.columns=['1','2','3','4','5','6','7','8','9','10','10']
# for r in df.itertuples():
#     print(len(r))

# for i in df.iterrows():
#     print(i[1])

df2=df['close'].apply(lambda x: df[df['close']==x].index if x > 21 else '')
for i,r in df2.iteritems():
    if r =='':
        df = df.drop(i,axis=0)


print(df)
# r = ("a=1","b=2")

#
# print(r[1])


# df=df.dropna()
#
# print(df.iat[2,2])
#
#

# df.plot.line(y=["close"])
# plt.show()