# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2020/10/27 下午7:39
@Author  : wuzhexiaolu
@FileName    : select_roadid.py
@Comment   :
"""
import glob

import pandas as pd
import logging
import os
import numpy as np
import gc
import csv
import time
import line_profiler
import sys
from tqdm import tqdm
from merge_csv import date_to_index


def get_linkid_list():
    df = pd.read_csv('att_table.txt')
    lst = df['OID_'].values.tolist()
    return lst


def select_r():
    for day_path in date_list:
        print(day_path)
        path = '/home/wuzhexiaolu/floatcardata/201211' + day_path + '/count/'
        files = os.listdir(path)
        for filename in files:
            data = pd.read_csv(path + filename)
            if roadid_lst[0] in data['RoadID'].values:
                data = data[data['RoadID'] == roadid_lst[0]]
                for itm in data.values:
                    idx = date_to_index(itm[0])
                    link_dict[itm[1]][idx] = itm[2]

def merge_csv():
    path = "/home/wuzhexiaolu/floatcardata/merge/"
    csv_list = glob.glob(f'{path}*.csv')
    print(u'共发现%s个CSV文件' % len(csv_list))
    print(u'正在处理............')
    for i in csv_list:
        fr = open(i, 'r')
        fr = fr.read()
        with open('/home/wuzhexiaolu/floatcardata/merge/all.csv', 'a') as f:
            f.write(fr)
    print(u'合并完毕！')


if __name__ == '__main__':
    time_s = time.time()
    date_list = ['01','02','03','04','05','06','07','08','09',
                 '10','11','12','13','14','15','16','17','18','19',
                 '20','21','22','23','24','25','26','27','29','30']
    link_dict = {}
    roadid_lst = get_linkid_list()
    #roadid_lst = [107983,107982]
    tqdm_a = tqdm(roadid_lst)
    for i in roadid_lst:
        link_dict[int(i)] = np.ones(30*288)*(-1)
    idx = 0
    for roadid in tqdm_a:
        tqdm_a.set_description(f'process roadid:{roadid}')
        path = '/home/wuzhexiaolu/floatcardata/merge/'
        files = os.listdir(path)
        for filename in files:
            data = pd.read_csv(path + filename)
            data.columns = ['time', 'RoadID', 'COUNT_CAR_NUM', 'speed']
            if roadid in data['RoadID'].values:
                data = data[data['RoadID'] == roadid]
                for itm in data.values:
                    idx = date_to_index(itm[0])
                    link_dict[int(itm[1])][idx] = int(itm[2])


        f = open('link_id_num.csv','w',encoding='utf-8')
        csv_writer = csv.writer(f)
        title = ['link_id']
        title.extend([i for i in range(30*288)])
        csv_writer.writerow(title)

        for key in link_dict.keys():
            arr = [key]
            arr.extend(link_dict[key])
            csv_writer.writerow(arr)
        f.close()

