import pymysql.cursors
from machine_learning.Strategy import In_Stock_all as instal

class Deal(object):
    cur_capital = 0.00
    cur_money_lock = 0.00
    cur_money_rest = 0.00
    cur_price = 0.00
    seq = 1
    stock_pool = []
    stock_map1 = {}
    stock_map2 = {}
    stock_map3 = {}
    stock_all = []
    ban_list = []
    get_price_err = 0

    def __init__(self,code,state_dt):

        try:
        # 建立数据库连接，读取 my_capital 最新一行数据 & my_stock_pool 所有数据；
            db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='test', charset='utf8')
            cursor = db.cursor()
            sql_select = 'select capital,money_lock,money_rest,seq from my_capital a order by seq desc limit 1'
            cursor.execute(sql_select)
            done_set = cursor.fetchall()
            self.cur_capital = 0.00
            self.cur_money_lock = 0.00
            self.cur_money_rest = 0.00
            if len(done_set) > 0:
                # self.cur_capital = float(done_set[0][0])
                self.cur_money_rest = float(done_set[0][2])
                self.seq = int(done_set[0][3])
            sql_select2 = 'select * from my_stock_pool'
            cursor.execute(sql_select2)
            done_set2 = cursor.fetchall()
            db.commit()
            db.close()

        # 对 my_capital & my_stock_pool 数据按照state_dt进行更新；
            self.stock_pool = []
            self.stock_all = []
            self.stock_map1 = []
            self.stock_map2 = []
            self.stock_map3 = []
            self.ban_list = []
            if len(done_set2) > 0:
                self.stock_pool = [x[0] for x in done_set2 if x[2] > 0]
                self.stock_all = [x[0] for x in done_set2]
                self.stock_map1 = {x[0]: float(x[1]) for x in done_set2}
                self.stock_map2 = {x[0]: int(x[2]) for x in done_set2}
                self.stock_map3 = {x[0]: int(x[3]) for x in done_set2}

            code_mark = 0
            if code == '000000.SZ':
                for i in range(len(done_set2)):
                    price_dt = instal.get_price(done_set2[i][0], state_dt)
                    if price_dt == -1:
                        print("my_stock_pool " + done_set2[i][0] + " 数据更新错误")
                        self.get_price_err = -2
                        break
                    code_mark = 1
                    self.cur_money_lock += price_dt * float(done_set2[i][2])

            else:
                for i in range(len(done_set2)):
                    price_dt = instal.get_price(done_set2[i][0],state_dt)
                    if price_dt == -1:
                        print("my_stock_pool "+done_set2[i][0]+" 数据更新错误")
                        self.get_price_err = -2
                        break

                    if done_set2[i][0]==code:
                        self.cur_price = price_dt
                        code_mark = 1

                    self.cur_money_lock +=  price_dt * float(done_set2[i][2])

            self.cur_capital = self.cur_money_lock + self.cur_money_rest

            if code_mark == 0:
                self.cur_price = instal.get_price(code,state_dt)
                if self.cur_price == -1:
                    print("my_stock_pool "+done_set2[i][0]+" 数据更新错误")
                    self.get_price_err = -1

            # sql_select3 = 'select * from ban_list'
            # cursor.execute(sql_select3)
            # done_set3 = cursor.fetchall()
            # if len(done_set3) > 0:
            #     self.ban_list = [x[0] for x in done_set3]

        except Exception as excp:
            #db.rollback()
            print('Deal Err')
            print(excp)
            self.cur_price = -1

