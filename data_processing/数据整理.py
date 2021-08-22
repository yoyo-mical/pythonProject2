import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
import xlwings as xw
import datetime
from def_class import BasicClass as bc
from shutil import copyfile

exit()
# 按交易日期升序排列；
n=0
dirpath = 'D:\\Astock'
for root,dir,filename in os.walk(dirpath):
    for name in filename:
        starpath = os.path.join(root,name)
        df = pd.read_csv(starpath, encoding='gbk')
        df.drop(columns=['Unnamed: 0'], inplace=True)
        df.sort_values(by='trade_date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(starpath, encoding="gbk")
        n=n+1
        print(n)

exit()

# sourcepath = 'C:\\Users\\Admin\\Desktop\\py\\stock'
# addresspath = 'D:\\Astock'
# for root,dirname,filename in os.walk(sourcepath):
#     for name in filename:
#         source = os.path.join(root,name)
#         address = os.path.join(addresspath,name)
#         copyfile(source,address)
#
# exit()

# pathbak='C:\\Users\\Admin\\Desktop\\py\\bak'
# pathmain='C:\\Users\\Admin\\Desktop\\py\\stock'

sa = bc.stockbasic('L')
# sa.groupby_(['market'])
# marketlist = sa.gpnamelist
# for market in marketlist:
#     path1 = pathbak+'\\'+ market
#     path2 = pathmain+'\\'+market
#     bc.mkdir(path1)
#     bc.mkdir(path2)
keyind = 'CDR'

pathbak = 'C:\\Users\\Admin\\Desktop\\py\\bak\\'+keyind
pathmain = 'C:\\Users\\Admin\\Desktop\\py\\stock\\'+keyind

sa.groupby_(['market','industry'],keyone= keyind)
induslist = sa.gpnamelist
for industry in induslist:
    path1 = pathbak+'\\'+ industry
    path2 = pathmain+'\\'+industry
    bc.mkdir(path1)
    bc.mkdir(path2)

