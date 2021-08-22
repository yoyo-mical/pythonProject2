import pymysql.cursors
from machine_learning.Strategy import Deal
from machine_learning.Strategy import In_Stock_all as instal

def buy(stock_code,opdate,buy_money):
    #获取并更新 my_capital&stock_pool 现有状态Database;
    deal_buy = Deal.Deal(stock_code,opdate)
    if deal_buy.get_price_err == -1:
        return -1
    elif deal_buy.get_price_err == -2:
        return -2
    #获取交易日 close_price；buy 条件设定及my_capital数据计算：
    buy_price = deal_buy.cur_price
    if deal_buy.cur_money_rest + 1 >= buy_money:
        if buy_price >= 195:
            return -3
        vol, rest = divmod(min(deal_buy.cur_money_rest, buy_money), buy_price * 100)
        vol = vol * 100
        if vol == 0:
            return 0

        new_capital = deal_buy.cur_capital - vol * buy_price * 0.0005
        new_money_lock = deal_buy.cur_money_lock + vol * buy_price
        new_money_rest = deal_buy.cur_money_rest - vol * buy_price * 1.0005

        # my_capital&my_stock_pool表格更新：
        db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8')
        cursor = db.cursor()

        # my_capital 数据更新：
        if deal_buy.seq == 1:
            sql_id = "alter table my_capital AUTO_INCREMENT=1"
            sql_buy_update2 = "insert into my_capital" \
                              "(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,state_dt,deal_price)" \
                              "VALUES ('%.2f', '%.2f', '%.2f','%s','%s','%i','%s','%.2f')" \
                              % (new_capital, new_money_lock, new_money_rest, 'buy', stock_code, vol, opdate, buy_price)
            cursor.execute(sql_id)
            cursor.execute(sql_buy_update2)
        else:
            sql_buy_update2 = "insert into my_capital" \
                              "(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,state_dt,deal_price)" \
                              "VALUES ('%.2f', '%.2f', '%.2f','%s','%s','%i','%s','%.2f')" \
                              % (new_capital, new_money_lock, new_money_rest, 'buy', stock_code, vol, opdate, buy_price)
            cursor.execute(sql_buy_update2)
        db.commit()

        # my_stock_pool数据更新：
        if stock_code in deal_buy.stock_all:
            new_buy_price = (deal_buy.stock_map1[stock_code] * deal_buy.stock_map2[stock_code] + vol * buy_price) / (deal_buy.stock_map2[stock_code] + vol)
            new_vol = deal_buy.stock_map2[stock_code] + vol
            sql_buy_update3 = "update my_stock_pool w set w.buy_price = (select '%.2f' from dual) where w.stock_code = '%s'" % (new_buy_price, stock_code)
            sql_buy_update3b = "update my_stock_pool w set w.hold_vol = (select '%i' from dual) where w.stock_code = '%s'" % (new_vol, stock_code)
            sql_buy_update3c = "update my_stock_pool w set w.hold_days = (select '%i' from dual) where w.stock_code = '%s'" % (1, stock_code)
            cursor.execute(sql_buy_update3)
            cursor.execute(sql_buy_update3b)
            cursor.execute(sql_buy_update3c)

        else:
            sql_buy_update3 = "insert into my_stock_pool(stock_code,buy_price,hold_vol,hold_days) " \
                              "VALUES ('%s','%.2f','%i','%i')" % (stock_code, buy_price, vol, int(1))
            cursor.execute(sql_buy_update3)
        db.commit()
        db.close()
        return 1
    else:
        return -4

def sell(stock_code,opdate,predict):

    deal = Deal.Deal(stock_code,opdate)
    init_price = deal.stock_map1[stock_code]
    hold_vol = deal.stock_map2[stock_code]
    hold_days = deal.stock_map3[stock_code]
    sell_price = deal.cur_price

    new_money_lock = deal.cur_money_lock - sell_price * hold_vol
    new_money_rest = deal.cur_money_rest + sell_price * hold_vol
    new_capital = deal.cur_capital + (sell_price - init_price) * hold_vol
    new_profit = (sell_price - init_price) * hold_vol
    new_profit_rate = sell_price / init_price

    bz = ''
    if sell_price > init_price*1.03 and hold_vol > 0:
        bz = 'GOODSELL'
    elif sell_price < init_price*0.97 and hold_vol > 0:
        bz = 'BADSELL'
    elif hold_days >= 4 and hold_vol > 0:
        bz = 'OVERTIMESELL'
    elif predict == -1:
        bz = 'PredictSell'

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8')
    cursor = db.cursor()

    sql_sell_insert = "insert into my_capital(capital,money_lock,money_rest,deal_action,stock_code,stock_vol,profit,profit_rate,bz,state_dt,deal_price)" \
                      "values('%.2f','%.2f','%.2f','%s','%s','%.2f','%.2f','%.2f','%s','%s','%.2f')" \
                      % (new_capital, new_money_lock, new_money_rest, 'SELL', stock_code, hold_vol, new_profit,
                         new_profit_rate, bz, opdate, sell_price)
    cursor.execute(sql_sell_insert)
    db.commit()
    sql_sell_update = "delete from my_stock_pool where stock_code = '%s'" % (stock_code)
    cursor.execute(sql_sell_update)
    db.commit()

    db.close()
    return 0

def Cap_Update_dailay(opdate):
    stock_code = '000000.SZ'
    capupdate = Deal.Deal(stock_code,opdate)
    new_total_cap = capupdate.cur_capital
    new_lock_cap = capupdate.cur_money_lock
    new_cash_cap = capupdate.cur_money_rest

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8')
    cursor = db.cursor()

    sql_insert = "insert IGNORE into my_capital(capital,money_lock,money_rest,bz,state_dt)" \
                 "values('%.2f','%.2f','%.2f','%s','%s')" \
                 % (new_total_cap, new_lock_cap, new_cash_cap, str('Daily_Update'), opdate)
    cursor.execute(sql_insert)
    db.commit()

    db.close()

def Dealback(rows):

    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8')
    cursor = db.cursor()

    for i in range(rows):
        sql_insert1 = "select deal_action,deal_price,stock_vol,stock_code from my_capital order by seq desc limit 1"
        cursor.execute(sql_insert1)
        deal_action = cursor.fetchall()
        sql_insert2 = "delete from my_capital order by seq desc limit 1"
        if deal_action[0][0] == None:
            cursor.execute(sql_insert2)
        elif deal_action[0][0] == 'buy':
            deal_price = deal_action[0][1]
            stock_vol = deal_action[0][2]
            stock_code = deal_action[0][3]
            sql_insert3 = "select * from my_stock_pool where stock_code = '{}'".format(stock_code)
            cursor.execute(sql_insert3)
            poolset = cursor.fetchall()
            now_price = poolset[0][1]
            now_vol = poolset[0][2]
            now_hold= poolset[0][3]
            if now_vol-stock_vol == 0:
                sql_insert4 = "delete from my_stock_pool where stock_code = '{}'".format(stock_code)
                cursor.execute(sql_insert4)
                cursor.execute(sql_insert2)
            else:
                old_vol = now_vol-stock_vol
                old_price = (now_price*now_vol-deal_price*stock_vol)/old_vol
                old_hold = now_hold -1
                sql_insert5 = "update my_stock_pool set buy_price = {0},hold_vol = {1},hold_days = {2} " \
                              "where stock_code = '{3}'".format(old_price,old_vol,old_hold,stock_code)
                cursor.execute(sql_insert5)
                cursor.execute(sql_insert2)
    db.commit()
    db.close()



if __name__=='__main__':

    Dealback(1)

    exit()
    for code in ['000001.SZ','000002.SZ']:
        result = buy(code, '2021-07-28', 10000)
        print(result)
        if result == -2:
            break
    exit()
    operatorMark = 'buy'
    # buy参数设定：

    # sell参数设定：

    # update_daily参数设定：

    # retracement参数设定：

