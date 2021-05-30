# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
import os
import time
import seaborn as sns
import matplotlib.pyplot as plt
from query_influxdb_to_pandas import utc_to_local

def data_cleaning(orig_data_h5_path, info_csv_path, gauging_point_code):
    """
    数据清洗
    :param orig_data_h5_path: 原始数据文件，HDF5
    :return:
    """
    
    orig_df = pd.DataFrame(pd.read_hdf(orig_data_h5_path)).iloc[:1000000]
    gauging_point_info_df = pd.read_csv(info_csv_path)
    gp_row = gauging_point_info_df.loc[gauging_point_info_df['name'] == gauging_point_code]
    start_time = gp_row.loc[:, 'start_time']
    end_time = gp_row.loc[:, 'end_time']
    time_interval = gp_row.loc[:, 'time_interval']
    row_num = gp_row.loc[:, 'row_length']
    print(orig_df.head(10))
    orig_df['datetime'] = None
    start = time.time()
    date_series = orig_df.apply(lambda row: utc_to_local(row['time'][:19])[1],  axis=1, result_type='broadcast')
    end = time.time()
    print('时间是:%s' % (end - start))
    orig_df['datetime'] = date_series['time']
    print(orig_df.head(10))
    reindex_orig_df = orig_df.set_index('datetime')
    print(reindex_orig_df.head(10))
    reindex_orig_df.iloc[:1000]['value'].plot()
    plt.show()
    reindex_orig_df.to_hdf('./1DCS_AI_SELMW.h5', key='orig_df', mode='w')


if __name__ == '__main__':
    data_path = 'D:\\IndustrialBigdata\\TJBT\\data_export_1_boiler_set_20200721'
    gp_code = '1DCS_AI_SELMW'
    h5_path = os.path.join(data_path, gp_code+'.h5')
    info_file_path = os.path.join(data_path, 'gauging_point_export_info', 'gauging_point_info.csv')
    data_cleaning(h5_path, info_file_path, gp_code)
    