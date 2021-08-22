import pymysql
import pandas as pd
import datetime
from time import sleep

def category(stocklist):
    condition = 'stock_code ='
    marketdic={}
    for i in range(len(stocklist)):

        if i != len(stocklist)-1:
            condition = condition + "'{}'".format(stocklist[i])+' OR stock_code ='
        else:
            condition = condition + "'{}'".format(stocklist[i])

    db_name = 'test'
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db=db_name, charset='utf8', local_infile=1)
    cur = conn.cursor()
    sql_select = 'select stock_code,industry,market from basic where {}'.format(condition)
    df = pd.read_sql(sql_select,conn)
    df_group1 = df.groupby(['market'])

    for name1,group1 in df_group1:
         df_group2 = group1.groupby(['industry'])
         indudic = {}
         for name2,group2 in df_group2:
             listA = group2['stock_code'].to_list()
             indudic.update({name2:listA})
         marketdic.update({name1: indudic})

    # marketdic = {name:group['industry'].to_list() for name,group in df_group }
    cur.close()
    conn.close()
    # print(marketdic)
    return marketdic

def stock_all_in(stocklist):

    tabel_key = 'state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change'
    s = "'%s'" + ', ' + "'%s'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%i'" + ', ' + \
        "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'" + ', ' + "'%.2f'"

    values = []
    db_map = category(stocklist)
    for key_ma,value_ma in db_map.items():

        db_name = key_ma
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db=db_name, charset='utf8',local_infile=1)
        cur = conn.cursor()

        for key_indu,value_indu in value_ma.items():

            condition = 'stock_code ='
            for i in range(len(value_indu)):
                if i != len(value_indu) - 1:
                    condition = condition + "'{}'".format(value_indu[i]) + ' OR stock_code ='
                else:
                    condition = condition + "'{}'".format(value_indu[i])

            sql_select = 'select {} from {} where {}'.format(tabel_key,key_indu,condition)
            cur.execute(sql_select)
            values_sig= cur.fetchall()

            for i in range(len(values_sig)):
                str_dt = values_sig[i][0].strftime("%Y-%m-%d")
                values_tup = (str_dt, str(values_sig[i][1]), float(values_sig[i][2]), float(values_sig[i][3]), float(values_sig[i][4]),
                         float(values_sig[i][5]), float(values_sig[i][6]), float(values_sig[i][7]), float(values_sig[i][8]),
                         float(values_sig[i][9]), float(values_sig[i][10]))
                values.append(values_tup)

        conn.commit()
        cur.close()
        conn.close()

    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8',local_infile=1)
    cur = conn.cursor()

    for i in range(len(values)):
        insertvalue = (values[i][0],values[i][1],values[i][2],values[i][3],values[i][4],values[i][5],values[i][6],
                       values[i][7],values[i][8],values[i][9],values[i][10])

        sql_insert = "INSERT IGNORE INTO stock_all ({}) VALUES ({})".format(tabel_key,s) % insertvalue
        cur.execute(sql_insert)

    conn.commit()
    cur.close()
    conn.close()

def get_price(code,dt):
    conn=pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8',local_infile=1)
    cur = conn.cursor()

    sql= "select close from stock_all where stock_code = '{}' and state_dt = '{}'".format(code,dt)
    cur.execute(sql)
    price_tup = cur.fetchall()
    cur.close()
    conn.close()

    if not price_tup:
        print(code + ' + '+dt+' 未获取到请求数据')
        stock_all_in([code])
        conn = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8',local_infile=1)
        cur = conn.cursor()

        sql = "select close from stock_all where stock_code = '{}' and state_dt = '{}'".format(code, dt)
        cur.execute(sql)
        price_tup = cur.fetchall()

        cur.close()
        conn.close()
        if not price_tup:
            print(code + ' + '+dt+' 该条件数据无法获取')
            return -1
        else:
            print(code + ' + '+dt+' 数据获取成功02')
            close_price=price_tup[0][0]
            return close_price
    else:
        print(code + ' + ' + dt + ' 数据获取成功01')
        close_price = price_tup[0][0]
        return close_price

if __name__ == '__main__':
    # stock_all_in(['000001.SZ','002151.SZ','000002.SZ','601899.SH','600916.SH'])
    get_price('000001.SZ','2021-07-23')


