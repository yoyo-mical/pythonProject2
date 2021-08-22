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
from collections.abc import Iterable
from def_class import BasicClass

# print('1'*3)
# exit()
bc = BasicClass()

print(type(bc))
dic = {'a':1,'b':2,'c':3}
print(type(dic))

print(isinstance(dic,Iterable))
