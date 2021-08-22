import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
from def_class import BasicClass as bc
import xlwings as xw
from inspect import getsource
import datetime

datapath = 'D:\\Astock_Save\\主板'
listanaly = []

for root,dir,filename in os.walk(datapath):
    for name in filename:
        csvname = os.path.splitext(name)[0]
        csvpath = os.path.join(root,name)
        df = pd.read_csv(csvpath,encoding='gbk')
        totalnum = len(df.index)
        df = df[df['earn']>1.05]
        thenum = len(df.index)
        bili = round(thenum/totalnum,2)
        listanaly.append((csvname,thenum,totalnum,bili))

df_analy = pd.DataFrame(listanaly,columns=['date','thenum','totalname','bili'])
print(df_analy.describe())
exit()


