# -*- coding:utf8 -*-
import numpy as np
import pymysql
import pandas as pd
from sklearn import svm
from machine_learning.Strategy import Operator

db_name = 'test'
tablename1 = 'my_capital'
tablename2 = 'my_stock_pool'
buy_opdate = '2021-07-12'
sell_opdate = '2021-06-18'
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db= db_name, charset='utf8',local_infile=1)
cur = conn.cursor()

# sql = 'select stock_code from it group by stock_code'
# cur.execute(sql)
# stocktupe = cur.fetchall()

sql = "truncate TABLE {}".format(tablename1)
sql_id = "alter table my_capital AUTO_INCREMENT=1"
sql_update= "insert into my_capital (capital,money_rest) VALUES ('%.2f', '%.2f')" % (150000,150000)
cur.execute(sql)
cur.execute(sql_id)
cur.execute(sql_update)

conn.commit()
cur.close()
conn.close()
exit()
#
# df = pd.DataFrame(stocktupe)
codelist = ['000001.SZ','000002.SZ','002151.SZ','600916.SH','601899.SH']

for code in codelist:
    Operator.buy(code,buy_opdate,10000)

exit()

# sql = 'select stock_code from my_stock_pool group by stock_code'
# cur.execute(sql)
# stocktupe = cur.fetchall()
# conn.commit()
# cur.close()
# conn.close()
# df = pd.DataFrame(stocktupe)

# codelist = []
#
# for code in codelist:
#     Operator.sell(code,sell_opdate,-1)
#
# exit()

# 建立数据库连接，获取日线基础行情(开盘价，收盘价，最高价，最低价，成交量，成交额)
in_code = '002049.SZ'
start_dt = '2017-03-01'
end_dt = '2018-03-01'
db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8',local_infile=1)
cursor = db.cursor()
sql_done_set = "SELECT * FROM stock_all a where stock_code = '%s' and state_dt >= '%s' and state_dt <= '%s' order by state_dt asc" \
               % (in_code, start_dt, end_dt)
cursor.execute(sql_done_set)
done_set = cursor.fetchall()
if len(done_set) == 0:
    raise Exception
date_seq = []
open_list = []
close_list = []
high_list = []
low_list = []
vol_list = []
amount_list = []
for i in range(len(done_set)):
    date_seq.append(done_set[i][0])
    open_list.append(float(done_set[i][2]))
    close_list.append(float(done_set[i][3]))
    high_list.append(float(done_set[i][4]))
    low_list.append(float(done_set[i][5]))
    vol_list.append(float(done_set[i][6]))
    amount_list.append(float(done_set[i][7]))
cursor.close()
db.close()
# 将日线行情整合为训练集(其中train是输入集，target是输出集，test_case是end_dt那天的单条测试输入)
data_train = []
data_target = []
data_target_onehot = []
cnt_pos = 0
test_case = []

for i in range(1,len(close_list)):
    train = [open_list[i-1],close_list[i-1],high_list[i-1],low_list[i-1],vol_list[i-1],amount_list[i-1]]
    data_train.append(np.array(train))

    if close_list[i]/close_list[i-1] > 1.0:
        data_target.append(float(1.00))
        data_target_onehot.append([1,0,0])
    else:
        data_target.append(float(0.00))
        data_target_onehot.append([0,1,0])
cnt_pos =len([x for x in data_target if x == 1.00])
test_case = np.array([open_list[-1],close_list[-1],high_list[-1],low_list[-1],vol_list[-1],amount_list[-1]])
data_train = np.array(data_train)
data_target = np.array(data_target)

# print(len(data_train))

model = svm.SVC()  # 建模
model.fit(data_train, data_target)  # 训练
ans2 = model.predict([test_case])  # 预测
# 输出对2018-03-02的涨跌预测，1表示涨，0表示不涨。
print([test_case])

