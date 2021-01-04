# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2020/10/26 下午9:47
@Author  : wuzhexiaolu
@FileName    : read_a_road.py
@Comment   :
"""
import pandas as pd
import os
import numpy as np
import gc
import csv

date_list = ['01','02','03','04','05','06','07','08','09',
             '10','11','12','13','14','15','16','17','18','19',
             '20','21','22','23','24','25','26','27','29','30']
link_dict = {}

for day_path in date_list:
    print(day_path)
    path = '/home/wuzhexiaolu/floatcardata/201211' + day_path + '/count/'
    files = os.listdir(path)
    spd_list=[]
    car_list=[]
    #cnt = 0
    for filename in files:
        #print(filename)
        #cnt += 1
        data = pd.read_csv(path + filename).values
        #print(type(data))
        #print(data[0])
        #print(len(data))
        for itm in data:
            if int(itm[1]) not in link_dict.keys():
                link_dict[int(itm[1])] = np.zeros(29)
        del data
        gc.collect()
            #spd_list.append(itm[3])
            #car_list.append(itm[2])
    #print(len(spd_list))
    #print(spd_list)
    #print(car_list)

    #print(spd_list.count(0))
    #print(car_list.count(0))
    #print(cnt)
###8:00-24:00 16*12 = 192 timestamp

###
idx = 0
for day_path in date_list:
    print(day_path)
    path = '/home/wuzhexiaolu/floatcardata/201211' + day_path + '/count/'
    files = os.listdir(path)
    for filename in files:
        data = pd.read_csv(path + filename).values
        for itm in data:
            link_dict[int(itm[1])][idx] += itm[2]
        del data
        gc.collect()

    idx += 1

print(len(list(link_dict.keys())))

f = open('dict_carnum.csv','w',encoding='utf-8')
csv_writer = csv.writer(f)
title = ['link_id']
title.extend(date_list)
csv_writer.writerow(title)

for key in link_dict.keys():
    arr = [key]
    arr.extend(link_dict[key])
    csv_writer.writerow(arr)
f.close()
