# -*- coding:utf-8 -*-
"""
@File    ：data_preprocessing.py
@Author  ：Dong Guangshan
@Create Date Time  ：
@Modified Date time  :2020/12/20 16:46
@Version  :
@Modified Contents  :
"""

import gc
import logging
import os
import pickle
import datetime
import time
import csv

import pandas as pd
import numpy as np

logger = logging.getLogger('main.data_preprocessing')


# plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
# plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


def utc_to_local(utc_time_str, local_format="%Y-%m-%d %H:%M:%S", utc_format=f'%Y-%m-%dT%H:%M:%S'):
    # local_tz = pytz.timezone('Asia/Chongqing')
    utc_dt = datetime.datetime.strptime(utc_time_str, utc_format)
    # local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    time_str = utc_dt.strftime(local_format)
    ltime = time.localtime(int(time.mktime(time.strptime(time_str, local_format))))
    
    # datetime格式
    date_time = datetime.datetime.strptime(time.strftime(local_format, ltime), "%Y-%m-%d %H:%M:%S")
    return time.strftime(local_format, ltime), date_time


def data_cleaning(orig_point_df, freq='30S'):
    """
    数据预处理
    Args:
        orig_point_df:单个测点的数据文件, DataFrame格式，索引为 datatime，列为：value
        freq: 采样频率

    Returns: 均匀重采样后的样本，DataFrame格式，(nxm)，每行为一个样本，每列为一个测点特征

    """
    gauging_point = orig_point_df.columns.values[-1]
    # 检测异常点，通过箱线图
    # Q1 = orig_point_df[gauging_point].quantile(q=0.25)
    # Q3 = orig_point_df[gauging_point].quantile(q=0.75)
    # 异常值判断标准， 1.5倍的四分位差 计算上下限对应的值
    # low_quantile = Q1 - 1.5 * (Q3 - Q1)
    # high_quantile = Q3 + 1.5 * (Q3 - Q1)
    
    # drop_index_list = []
    # for index, row in orig_point_df.iterrows():
    #     if row[gauging_point] > high_quantile or row[gauging_point] < low_quantile:
    #         drop_index_list.append(index)
    # orig_point_df = orig_point_df.drop(index=drop_index_list)
    # 均匀采样,重采样频率    30s采集一次，5分钟从数据库读取一次数据，重采样后得到10条待计算时序数据
    point_resampled_df = orig_point_df.resample(freq, closed='right', label='right').mean()
    
    return point_resampled_df


def resample(csv_path, save_path,freq, headers):
    """
    数据重采样
    Args:
        csv_path: 数据文件,csv格式

    Returns:
        重采样后的结果文件,csv格式

    """
    
    orig_data = pd.read_csv(csv_path)
    orig_data['time'] = orig_data.apply(lambda x: utc_to_local(x['time'][:19])[0], axis=1)
    orig_data.index = pd.DatetimeIndex(orig_data["time"])
    orig_data.drop(['time', 'time'], axis=1, inplace=True)
    orig_data.dropna(axis=0, inplace=True)
    
    resampled_data = orig_data.resample(freq, closed='right', label='right').mean()
    # resampled_data.to_csv(save_path, encoding="utf-8-sig", mode="a", header=headers, index=False)
    resampled_data.to_csv(save_path, header=headers, index=False)


def query_ts_data_for_run(data_path):
    """
    
    Args:
        data_path:
        start_query_datetime:
        end_query_datetime:
        host:
        port:
        username:
        password:
        database:

    Returns:

    """
    ts_list = []
    for root, dirs, files in os.walk(data_path, topdown=True):
        for name in files:
            gauging_point = name.split('.')[0]
            try:
                with open(os.path.join(root, name), 'rb') as f:
                    field_total_df = pd.read_pickle(f)
                    field_total_df['data_time'] = field_total_df.apply(lambda x: utc_to_local(x['time'][:19])[0],
                                                                       axis=1)
                    field_total_df.index = pd.DatetimeIndex(field_total_df["data_time"])
                    field_total_df.drop(['data_time', 'time', 'id'], axis=1, inplace=True)
                    field_total_df.dropna(axis=0, inplace=True)
                    field_total_df.rename(columns={'value': gauging_point}, inplace=True)
                    
                    mp_dict = {'measurement': gauging_point, 'ts_value_df': field_total_df}
                    ts_list.append(mp_dict)
                    
                    # garbage collection
                    del field_total_df
                    gc.collect()
                    print(len(ts_list), gauging_point)
            except Exception as e:
                print('异常测点：%s\n%s\n ' % (e, gauging_point))
            continue
    
    return ts_list


def get_data_for_run(ts_data_list, start_query_datetime, end_query_datetime, freq='30s'):
    """
    
    Args:
        ts_data_list:
        freq:

    Returns:

    """
    
    data_resampled_dict = {}
    measurement_info_dict = {}  # 存储所有测点的测点信息，key包括：测点索引、测点名列表、测点描述
    df_index_list = []
    
    for i, mp_dict in enumerate(ts_data_list):
        try:
            mp_df = mp_dict['ts_value_df']
            mp_resampled_df = data_cleaning(mp_df, freq)
            df_index_list.append(mp_resampled_df.index)
            data_resampled_dict[mp_dict['measurement']] = mp_resampled_df.loc[:, mp_dict['measurement']]
            single_mp_dict = {'measurement_list': [mp_dict['measurement']]}
            measurement_info_dict[mp_dict['measurement']] = single_mp_dict
        
        except Exception as e:
            print(e)
        continue
    num_row = min(len(df_index_list[i]) for i in range(len(df_index_list)))
    data_dict = {}
    for key, value in data_resampled_dict.items():
        data_dict[key] = value.iloc[:num_row]
        print(len(data_dict), key)
    format_df = pd.DataFrame(data_dict, index=df_index_list[0][:num_row])
    format_df.fillna(method='pad')  # 填充缺失值
    
    # 合并同类测点，求均值
    # format_df.loc[:, '6BSCS8AI:AIN618035.PNT'] = (format_df['6BSCS8AI:AIN618035.PNT'] +
    #                                               format_df['6BSCS8AI:AIN618054.PNT'] +
    #                                               format_df['6BSCS8AI:AIN618045.PNT']) / 3
    # format_df = format_df.drop(['6BSCS8AI:AIN618054.PNT', '6BSCS8AI:AIN618045.PNT'], axis=1)
    # measurement_info_dict['6BSCS8AI:AIN618035.PNT']['measurement_list'].extend(['6BSCS8AI:AIN618054.PNT',
    #                                                                             '6BSCS8AI:AIN618045.PNT'])
    
    return measurement_info_dict, format_df.loc[start_query_datetime: end_query_datetime, :]


if __name__ == '__main__':
    pass