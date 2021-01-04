# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2020/10/26 下午8:27
@Author  : wuzhexiaolu
@FileName    : merge_csv.py
@comment   :
"""
import csv
import codecs
import pandas as pd
import numpy as np
import time

def data_write_csv(file):
    file_csv = codecs.open(file, 'w+', 'utf-8')  # 追加
    writer = csv.writer(file_csv, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    for i in range(len(df)):
        date = df.iloc[i,0]
        index_ = int(date_to_index(date))
        columns_ = int(df.iloc[i,1])
        value = df.iloc[i,2]
        print(n[index_,columns_])
        n[index_,columns_] = value
    writer.writerow(n)
    print("保存文件成功，处理结束")


def date_to_index(date):
    """
    via the date to get the index for the matrix values
    :param date: %Y/%m/%d %H:%M%S
    :return: matrix index
    """
    struct_time = time.strptime(date,'%Y/%m/%d %H:%M:%S')        # know the date to get the struct_time
    day = struct_time.tm_mday
    hour = struct_time.tm_hour
    min = struct_time.tm_min

    index =int((day-1)*288+(hour-8)*12+(min/5)+1)
    return index


if __name__ == '__main__':
    n = np.zeros((2,80000))
    file_name = '20121101080000.csv'
    file = '1.csv'
    df = pd.read_csv(file_name)
    data_write_csv(file)
