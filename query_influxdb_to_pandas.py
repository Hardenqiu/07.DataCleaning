# -*- coding:utf-8 -*-

import datetime
import gc
import os
import time

import dask.dataframe as dd
import pandas as pd
import pytz
import csv
from influxdb import InfluxDBClient
import numpy as np

# from file_operation import get_filesize


def query_influx_to_dataframe_sysh(measurement_point, start_query_datetime, end_query_datetime, host='10.33.4.67',
                                   port=8086, username='rkadmin', password='rkadmin2018', database='etsdb'):
    """
    双鸭山测点
    # timedelta = datetime.timedelta(hours=8)
    如果读取的数据的时区不对，用当前时间减去8个小时
    Args:
        measurement_point:
        start_query_datetime: str, 日期时间字符串，'2020-07-15 12:08:10'，str(datetime.datetime(2020, 7, 15, 12, 8, 10))
        end_query_datetime:str, 日期时间字符串，'2020-07-15 13:00:00'
        host:
        port:
        username:
        password:
        database:

    Returns:

    """

    client = InfluxDBClient(host, port, username, password, database)

    sql = "SELECT * FROM \"" + measurement_point + "\" WHERE time > '" + start_query_datetime + "' AND time < '" + \
          end_query_datetime + "'"
    result = client.query(sql)

    field_total_df = pd.DataFrame(list(result.get_points()))
    field_total_df.to_csv('./field_total' + '.csv', encoding='GBK')
    print(len(field_total_df))


# UTC时间转本地时间
def utc_to_local(utc_time_str, local_format="%Y-%m-%d %H:%M:%S", utc_format=f'%Y-%m-%dT%H:%M:%S'):
    local_tz = pytz.timezone('Asia/Chongqing')
    utc_dt = datetime.datetime.strptime(utc_time_str, utc_format)
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    time_str = local_dt.strftime(local_format)
    ltime = time.localtime(int(time.mktime(time.strptime(time_str, local_format))))
    # datetime格式
    date_time = datetime.datetime.strptime(time.strftime(local_format, ltime), "%Y-%m-%d %H:%M:%S")
    return time.strftime(local_format, ltime), date_time


# def query_time_series_to_hdf5_tjbt(gauging_point_csv):
#     """
#     从influxdb时序数据库中读取测点数据及测点描述信息，另存为HDF5格式，每个测点保存成一个单独的文件
#     :param gauging_point_csv:str, csv文件，两列：测点名，测点描述
#     :return:
#     """
#     # 天津北塘测点(local)
#     client_tjbt = InfluxDBClient(host='192.168.3.108', port=8086, username='admin', password='admin', database='etsdb')
#
#     point_df = pd.read_csv(gauging_point_csv)
#     point_info_df = pd.DataFrame(columns=['name', 'descriptor', 'size', 'row_length', 'unit', 'start_time', 'end_time',
#                                           'time_interval'])
#     with open('point_info.csv', 'w', encoding='utf-8') as f:
#         csv_writer = csv.writer(f)
#         for i in range(len(point_df)):
#             gauging_point = point_df.iloc[i]['measurement']
#             descriptor = point_df.iloc[i]['descriptor']
#             unit = point_df.iloc[i]['unit']
#             hdf_file_path = os.path.join('../TJBT/#1_boiler_set', gauging_point + '.h5')
#             if not os.path.exists(hdf_file_path):
#                 try:
#                     sql_tjbt = "SELECT * FROM \"" + gauging_point + "\""
#                     result_tjbt = client_tjbt.query(sql_tjbt)
#                     field_total_df = pd.DataFrame(list(result_tjbt.get_points()))
#                     field_total_df.to_hdf(hdf_file_path, key='field_total_df', mode='w')
#                     print('%d %s: %d' % (i, descriptor, len(field_total_df)))
#                     start_time = utc_to_local(field_total_df.loc[0]['time'][:19])[1]
#                     end_time = utc_to_local(field_total_df.loc[len(field_total_df) - 1]['time'][:19])[1]
#                     file_size = str(get_filesize.get_file_size(hdf_file_path)) + 'MB'
#                     point_info_df = point_info_df.append(
#                         pd.DataFrame({'name': [gauging_point], 'descriptor': [descriptor],
#                                       'size': [file_size], 'row_length': [len(field_total_df)],
#                                       'unit': [unit], 'start_time': [start_time],
#                                       'end_time': [end_time], 'time_interval': [end_time - start_time]}),
#                         ignore_index=True)
#                     csv_writer.writerow(
#                         [gauging_point, descriptor, file_size, len(field_total_df), unit, start_time, end_time,
#                          end_time - start_time])
#                     # garbage collection
#                     del field_total_df
#                     gc.collect()
#                 except Exception as e:
#                     print('异常测点：%s\n%s-%s\n ' % (e, gauging_point, descriptor))
#                 continue
#             else:
#                 continue
#     f.close()
#     point_info_df.to_csv(os.path.join('../TJBT/#1_boiler_set', 'gauging_point_info.csv'))


def drop_dupl_row_csv(str):
    """
    # 删除csv文件中的重复行
    :param str: 需要删除重复行的csv文件路径
    :return:
    """
    df_orig = pd.read_csv(str)
    non_repeat_df = df_orig.drop_duplicates(['Tag'], inplace=False)
    non_repeat_df.to_csv('../TJBT/tjbt_xnjs_non_repeat.csv')


if __name__ == '__main__':
    measurement = "PER2_EFFBLRGB"
    query_influx_to_dataframe_sysh(measurement)
    print('"' + measurement + '"' + ' has been saved!')

    # ret = '2020-06-20T00:00:45.543000Z'.split('.')[0]
    # current_time_int = utc_to_local(ret)
    # print(current_time_int)

    # point_csv = './gauging_point_tjbt_#1.csv'
    # query_time_series_to_hdf5_tjbt(point_csv)

    # str = '../TJBT/tjbt_xnjs_20200721.csv'
    # drop_dupl_row_csv(str)
