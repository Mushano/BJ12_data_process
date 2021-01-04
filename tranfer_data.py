# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2020/10/27 下午10:40
@Author  : wuzhexiaolu
@FileName    : tranfer_data.py
@Comment   :
"""
import csv
import pandas as pd
import time
import numpy as np


def date_to_utc(date):
    time_arr = time.strptime(date,'%Y/%m/%d %H:%M:%S')
    time_stamp = int(time.mktime(time_arr))
    return time_stamp

def utc_to_date(u):
    t_arr = time.localtime(u)
    date = time.strftime('%Y/%m/%d %H:%M:%S',t_arr)
    return date

def judge_value(x):
    if x !=-1:
        print('wrong')
    else:
        return True

if __name__ == '__main__':


    start_date = '2012/11/01 08:00:00'
    end_date = '2012/11/30 23:55:00'
    start_utc = date_to_utc(start_date)
    end_utc = date_to_utc(end_date)
    utc_lst = [i for i in range(start_utc,end_utc+300,300)]
    date_lst = [utc_to_date(i) for i in utc_lst]
    print(len(date_lst))
    date_lst.insert(0,' ')
    df = pd.read_csv('link_id_speed.csv',header=None,skiprows=[0])
    df = df.drop(columns=[i for i in range(8546,8641)])
    df = df.drop(columns=[1])


    print(df.shape[0])
    df.columns = date_lst
    date_lst[0] = '2012/11/01 08:00:00'
    df = pd.DataFrame(df.values.T,index = df.columns)
    df.insert(0,'select1',date_lst)
    df.insert(0, 'select2', date_lst)
    df['select2'] = df['select1'].map(lambda x: int(x.split(' ')[1].split(':')[1]))
    df['select1'] = df['select1'].map(lambda x:int(x.split(' ')[1].split(':')[0]))
    df = df.drop(index= df[(df['select1']>=0) & (df['select1']<8)].index)
    df = df.drop(columns=['select1','select2'])

    df.to_csv('link_id_speed_2.csv')