# -*- encoding:utf-8 -*-
"""
@作者：武者小路
@文件名：data_process.py
@时间：2020/10/20  11:17
@文档说明:
"""

import pymssql
import time
import pandas as pd
import os
from tqdm import tqdm
import numpy as np


def get_utc_list(date):
    """

  :param date: 标准时间 %Y/%m/%d %H:%M:%S
  :return: 当天的每隔五分钟的utc时列表
  """
    start_utc = time.mktime(time.strptime(date, r'%Y/%m/%d %H:%M:%S'))
    utc_list = [start_utc + i * 300 for i in range(288)]  # 一天有288个五分钟，用于生成当天的utc_list
    return utc_list


def SQLServer_connect(config_dict):
    '''
    SQLServer 数据库连接
    '''
    connect = pymssql.connect(**config_dict)
    print('Connect Successful!!!')
    return connect


def utc_to_date(utc):
    time_array = time.localtime(utc)
    date = time.strftime("%Y%m%d%H%M%S", time_array)
    return date


def tuple_to_dict(tuple_list):
    """
    元组列表转字典列表
    :param tuple_list: 存储元组的列表
    :return: 返回存储字典的列表
    """
    dict_list = dict((x, y) for x, y in tuple_list)
    return dict_list


def select_records(connect, table_name, str_sql):
    """
    通过sql语句进行查询
    :param connect: 数据库连接后所得到的connect
    :param table_name: 表名
    :param str_sql: sql语句
    :return: 返回查询到的数据，以字典列表形式保存
    """
    cursor = connect.cursor(as_dict = True)
    select_sql = str_sql
    cursor.execute(select_sql)
    row = cursor.fetchall()
    cursor.close()
    return row


def dic_data_to_csv(data, t):
    """
    把时间相关的字典数据转成csv文件，并以时间命名
    :param data: 字典数据
    :param t: utc时间
    :return:
    """
    df = pd.DataFrame(data)
    df = df.sort_values('RoadID')
    date = utc_to_date(t)
    df.to_csv(r'/home/floatcardata/20121101\count\{a}.csv'.format(a=date), index=False)


if __name__ == '__main__':

    config_dict = {
        'user': 'sa',
        'password': 'Yanbin520',
        'host': 'localhost',
        'database': 'MatchedBJ'
    }

    connect = SQLServer_connect(config_dict)

    if (os.path.exists(r'/home/wuzhexiaolu/floatcardata/20121101') == False):
        os.makedirs(r'/home/wuzhexiaolu/floatcardata/20121101/count')
    table_name = 'MatchedBJ20121101'
    utc_list = get_utc_list('2012/11/1 00:00:00')

    sql_min = 'SELECT MIN(UtcTime) as m FROM {table}'.format(table=table_name)
    min_utc = select_records(connect, table_name, sql_min)[0]['m']

    tqdm_a = tqdm(range(len(utc_list)))
    for i in tqdm_a:
        tqdm_a.set_description("Processing %s" % i)
        t_s = utc_list[i]
        t_e = t_s + 300
        if t_e < min_utc:
            # 如果最小的utc都比选的最大区间大了，那就没必要查询了
            continue
        else:
            sql_5min = 'SELECT RoadID,COUNT(UtcTime) as COUNT_CAR_NUM FROM {table} WHERE UtcTime>{t1} AND UtcTime<{t2} GROUP BY RoadID'.format(
                table=table_name, t1=t_s, t2=t_e)
            data_5min = select_records(connect, table_name, sql_5min)
            dic_data_to_csv(data_5min, t_s)
    connect.close()

# df_count = pd.DataFrame(columns=[i for i in range(152217)])     #一共有152217条路段
# for i in utc_list:
