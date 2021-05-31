# coding=utf-8
"""
@File    ：CorrelationAnylysis.py
@Author  ：Qiu Heng-tan
@Create Date Time  ：2021/5/25
@Modified Date time  :2021/05/31 11:01
@Version  :
@Modified Contents  :
"""
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

print("-" * 100)
df = pd.read_csv('.\\ResH\\Hresult_table.csv',
                 index_col=0
                 # header=None,
                 # sep='\s+'
                 )

cols = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
        '11', '12', '13', '14', '15']

# cols = ['TS_DCS2_THTLE_PRES', 'TS_DCS2_TOT_STM_FLW', 'TS_DCS2_20LBA10CT601', 'TS_DCS2_TOT_FUEL_FLOW',
#         'TS_DCS2_FW_FLW_COMP', 'TS_DCS2_TOTAL_AIR_FLOW', 'TS_DCS2_20HNA10CQ101', 'TS_DCS2_20HNA10CQ102',
#         'TS_DCS2_20HHA10AA129AO', 'TS_DCS2_20HHA10AA128AO', 'TS_DCS2_20HHA10AA127AO', 'TS_DCS2_20HHA10AA126AO',
#         'TS_DCS2_20HHA10AA125AO', 'TS_DCS2_20HHA10AA124AO', 'TS_DCS2_20HHA10AA123AO', 'TS_DCS2_20HHA10AA122AO',
#         'TS_DCS2_20HHA10AA121AO', 'TS_DCS2_20HHA10AA120AO', 'TS_DCS2_20HHA10AA119AO', 'TS_DCS2_20HHA10AA118AO',
#         'TS_DCS2_20HHA10AA117AO', 'TS_DCS2_20HHA10AA116AO', 'TS_DCS2_20HHA10AA115AO', 'TS_DCS2_20HHA10AA114AO',
#         'TS_DCS2_20HHA10AA113AO', 'TS_DCS2_20HHA10AA112AO', 'TS_DCS2_20HHA10AA111AO', 'TS_DCS2_20HHA10AA132AO',
#         'TS_DCS2_20HHA10AA133AO', 'TS_DCS2_20HHA10AA134AO', 'TS_DCS2_20HHA10AA135AO', 'TS_DCS2_20HHA10AA136AO',
#         'TS_DCS2_20HHA10AA137AO', 'TS_DCS2_20HHA10AA130AO', 'TS_DCS2_SCRAINNOX', 'TS_DCS2_SCRBINNOX']

flag = 1  # o画直方图  1画热力图
if (flag == 0):
    # 获取直方图
    sns.pairplot(df)
    plt.tight_layout()
    if os.path.exists('pairplot.png'):
        os.remove('pairplot.png')
    plt.savefig('pairplot.png')

    # plt.show()
else:
    # 获取热力图
    cm = np.corrcoef(df[cols].values.T)

    hm = sns.heatmap(cm,
                     cbar=True,
                     annot=True,
                     square=True,
                     fmt='.2f',
                     annot_kws={'size': 6},
                     yticklabels=cols,
                     xticklabels=cols)

    plt.tight_layout()
    if os.path.exists('heatmap.png'):
        os.remove('heatmap.png')
    plt.savefig('heatmap.png')

    # plt.show()
