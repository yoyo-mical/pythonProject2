import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
import xlwings as xw
import datetime
import tushare as ts
import shutil
import pymysql


class stockbasic:
    df = pd.DataFrame
    dfgp = pd.DataFrame
    filepath = "C:\\Users\\Admin\\Desktop\\py\\stockbasic\\temp.xlsx"
    gpnamelist =[]
    gpnumlist =[]
    def __init__(self,status):
        self.status = status
        if self.status == 'L':
            self.df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockL.csv", encoding="gbk")
        elif self.status == 'D':
            self.df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockD.csv", encoding="gbk")
        elif self.status == 'P':
            self.df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockP.csv", encoding="gbk")
        elif self.status == 'T':
            self.df= pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\stockT.csv", encoding="gbk")

    def groupby_(self,namelist,keyone=None,keytwo=None):
        gp = self.df.groupby(namelist[0])
        gpcount = gp.count()['ts_code']
        if keyone is None and keytwo is None:
            self.gpnamelist = gpcount.index.tolist()
            self.gpnumlist = gpcount.tolist()

        elif keyone is not None and keytwo is None:
            self.dfgp=gp.get_group(keyone)
            gp2 = self.dfgp.groupby(namelist[1])
            gpcount2 = gp2.count()['ts_code']
            self.gpnamelist = gpcount2.index.tolist()
            self.gpnumlist = gpcount2.tolist()
            gpcount2.to_excel(self.filepath, encoding="gbk")
        elif keyone is not None and keytwo is not None:
            self.dfgp = gp.get_group(keyone).groupby(namelist[1]).get_group(keytwo)
            if len(namelist)>2:
                self.gpnamelist = self.dfgp[namelist[2]].tolist()
            else:
                pass
            self.dfgp.to_excel(self.filepath, encoding="gbk")


            # self.excelshow(self.filepath)



        # os.startfile(filepath)


    def excelshow(self,filepath):
        app = xw.App(visible=True, add_book=False)
        app.display_alerts = False
        app.screen_updating = True
        wb = app.books.open(filepath)
        sht = wb.sheets('Sheet1')
        sht.autofit()
        sht.range('A:Z').api.Font.Size = 10
        sht.range('A:Z').api.Font.Bold = False
        sht.range('A1:Z1').api.Font.Bold = True
        sht.range('A:Z').api.Font.Name = '微软雅黑'



def mkdir(path):
    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        print(path + ' 创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path + ' 目录已存在')
        return False

def excelshow(filepath):
    app = xw.App(visible=True, add_book=False)
    app.display_alerts = False
    app.screen_updating = True
    wb = app.books.open(filepath)
    sht = wb.sheets('Sheet1')
    sht.autofit()
    sht.range('A:Z').api.Font.Size = 10
    sht.range('A:Z').api.Font.Bold = False
    sht.range('A1:Z1').api.Font.Bold = True
    sht.range('A:Z').api.Font.Name = '微软雅黑'

def excelshow2(filepath,shtname):
    app = xw.App(visible=True, add_book=False)
    app.display_alerts = False
    app.screen_updating = True
    wb = app.books.open(filepath)
    sht = wb.sheets(shtname)
    sht.autofit()
    sht.range('A:Z').api.Font.Size = 10
    sht.range('A:Z').api.Font.Bold = False
    sht.range('A1:Z1').api.Font.Bold = True
    sht.range('A:Z').api.Font.Name = '微软雅黑'


def tradedate(path_):
    df1 = pd.DataFrame
    list = os.listdir(path_)
    k = 0
    for i in list:
        path = os.path.join(path_, i)
        pathname,ext = os.path.splitext(path)
        if ext == '.csv':
            df = pd.read_csv(path, encoding='gbk')
            df = pd.DataFrame(df.loc[len(df.index)-1, ['ts_code', 'trade_date']]).T

            if k == 0:
                df1 = df
            else:
                df1 = pd.concat([df1, df])
            k=k+1
    return df1

def tradedateall(path_):
    df_ = pd.DataFrame
    list = os.listdir(path_)
    k = 0

    for i in list:
        path = os.path.join(path_, i)
        pathname,ext = os.path.splitext(path)
        if ext == '.csv':
            df_all = pd.read_csv(path, encoding='gbk')
            df = pd.DataFrame(df_all.loc[len(df_all.index)-1, ['ts_code', 'trade_date']]).T
            df['current'] = df_all.at[0,'trade_date']
            df['count'] = len(df_all.index)
            if k == 0:
                df1 = df
            else:
                df1 = pd.concat([df1, df])
            k=k+1
    return df1

class datacheck:
    listdir1=[]
    listdir2 =[]
    totalcount=[]
    df_ =pd.DataFrame

    def __init__(self,path1,path2):
        self.path1 = path1
        self.path2 =path2
        self.listdir1 = os.listdir(self.path1)
        self.listdir2 = os.listdir(self.path2)

    def datacheck(self):
        k=0
        for namestr in self.listdir1:
            if namestr.__contains__('2.csv'):
                namestr2 = namestr
                namestr1 = namestr2[0:9] + '1.csv'
                namestr3 = namestr2[0:9] + '.csv'
                path1 = os.path.join(self.path1, namestr1)
                path2 = os.path.join(self.path1, namestr2)
                path3 = os.path.join(self.path2,namestr3)
                df1 = pd.read_csv(path1, encoding='gbk')
                df2 = pd.read_csv(path2, encoding='gbk')
                df_main =pd.read_csv(path3, encoding='gbk')
                df3 = pd.DataFrame(df1.loc[len(df1.index)-1,['ts_code','trade_date']]).T
                df3.rename(columns={'trade_date':'trade_date_star_bak'},inplace=True)
                df4 = pd.DataFrame(df2.loc[0,['ts_code','trade_date']]).T
                df4.rename(columns={'trade_date': 'trade_date_end_bak'},inplace=True)

                df_main1 = pd.DataFrame(df_main.loc[len(df1.index)-1,['ts_code','trade_date']]).T
                df_main1.rename(columns={'trade_date':'trade_date_star_main'},inplace=True)
                df_main1['trade_date_end_main'] = df_main.at[len(df1.index),'trade_date']

                df = pd.merge(df3,df4,on='ts_code')
                df['count_t'] = len(df1.index)+len(df2.index)
                df = pd.merge(df,df_main1,on='ts_code')

                if k == 0:
                    self.df_ = df
                else:
                    self.df_ = pd.concat([self.df_, df])
                k = k + 1

def trade_cal(mark,star,end):
    if mark == 'SH':
        df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\SSE.csv", encoding='gbk')
    elif mark == 'SZ':
        df = pd.read_csv("C:\\Users\\Admin\\Desktop\\py\\stockbasic\\SZSE.csv", encoding='gbk')

    listindex = df[df['cal_date'].isin([star, end])].index
    df_cal = df.loc[listindex[0]:listindex[1], ['cal_date', 'is_open']]
    df_cal = df_cal[df_cal['is_open'].isin(['1'])]
    num = len(df_cal.index)
    return (num,df_cal)

def del_file(filepath):
    """
    删除某一目录下的所有文件或文件夹
    :param filepath: 路径
    :return:
    """
    del_list = os.listdir(filepath)
    for f in del_list:
        file_path = os.path.join(filepath, f)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)




def last_first_date_and_last_date(n):
    """
    获取前n周开始时间和结束时间，参数n：代表前n周
    :param n: int类型 数字：1，2，3，4，5
    :return: 返回前n周的周一0点时间  和 周日23点59分59秒
    """
    now = datetime.datetime.now()
    before_n_week_start = now - datetime.timedelta(days=now.weekday() + 7*n, hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    before_n_week_end = before_n_week_start + datetime.timedelta(days=6, hours=23, minutes=59, seconds=59)
    return before_n_week_start, before_n_week_end


def get_week_monday_and_sunday_by_date(date_str):
    """
    给定一个日期-返回日期所在周的周一0点时间 和 周日23点59分59秒
    :param date_str: 如："2020-05-01"
    :return: 给定一个日期-返回日期所在周的周一0点时间 和 周日23点59分59秒
    """
    now_time = datetime.datetime.strptime(date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    week_start_time = now_time - datetime.timedelta(days=now_time.weekday(), hours=now_time.hour, minutes=now_time.minute, seconds=now_time.second, microseconds=now_time.microsecond)
    week_end_time = week_start_time + datetime.timedelta(days=6, hours=23, minutes=59, seconds=59)

    # week_start_time = week_start_time.strftime('%Y-%m-%d')
    # week_end_time = week_end_time.strftime('%Y-%m-%d')

    return week_start_time, week_end_time


def get_all_monday_and_sunday_by_date_interval(start_date_str, end_date_str):
    """
    给定时间（日期）区间，返回区间中所有的周起止时间列表（不含本周）
    :param start_date_str: "2020-01-31"
    :param end_date_str: "2020-05-08"
    :return:
    """
    date_list = list()

    # 本周一开始时间
    now = datetime.datetime.now()
    now_week_monday = now - datetime.timedelta(days=now.weekday(), hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    # print('now_week_monday = {}'.format(now_week_monday))

    # 起始时间所在周 - 周一和周日
    start_week_monday, start_week_sunday = get_week_monday_and_sunday_by_date(start_date_str)
    # print('start_week = {} -> {}'.format(start_week_monday, start_week_sunday))

    # 截止时间所在周 - 周一和周日
    end_week_monday, end_week_sunday = get_week_monday_and_sunday_by_date(end_date_str)
    # print('end_week = {} -> {}'.format(end_week_monday, end_week_sunday))
    if end_week_monday < now_week_monday:
        #date_list.append({"start_time": end_week_monday, "end_time": end_week_sunday})
        date_list.append((end_week_monday,  end_week_sunday))


    count = 1
    while True:
        week_start_time = end_week_monday - datetime.timedelta(days=7 * count)
        week_end_time = week_start_time + datetime.timedelta(days=6, hours=23, minutes=59, seconds=59)
        # print('middle_week = {} -> {}'.format(week_start_time, week_end_time))

        count += 1
        if week_start_time >= now_week_monday:
            continue
        if week_start_time < start_week_monday:
            break
        # print('append middle_week = {} -> {}'.format(week_start_time, week_end_time))
        # date_list.append({"start_time": week_start_time, "end_time": week_end_time})
        date_list.append((week_start_time,  week_end_time))
    return date_list