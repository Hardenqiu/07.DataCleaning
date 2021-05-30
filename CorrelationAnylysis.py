# coding=utf-8
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# from scipy import stats

# df1 = pd.read_csv('.\\ResH\\Hresult_table.csv', usecols=['1'])
# df2 = pd.read_csv('.\\ResH\\Hresult_table.csv', usecols=['2'], header=0)
#
# df3 = pd.read_csv('.\\ResH\\Hresult_table.csv')
#
# # print(df1)
# # print(df2)
# # print(df3)
#
# sns.heatmap(df3.corr(),linewidths=0.1,vmax=1.0, square=True,linecolor='red', annot=True)
# plt.show()

# fig = plt.figure()
# ax1 = fig.add_subplot(1,2,1)
# ax1.scatter(df1, df2)
# plt.grid()
# plt.show()


# print(data)
# print(data.head())
print("-"*100)
df = pd.read_csv('.\\ResH\\Hresult_table.csv',
                 # header=None,
                 # sep='\s+'
                 )

# print(df)
# df.columns = ['1','2','3','4','5','6','7','8','9','10',
#               '11','12','13','14','15','16','17','18','19','20',
#               '21','22','23','24','25','26','27','28','29','30',
#               '31','32','33','34','100','200']

# print(df.head())

# cols = ['1','2','3','4','5','6','7','8','9','10',
#               '11','12','13','14','15','16','17','18','19','20',
#               '21','22','23','24','25','26','27','28','29','30',
#               '31','32','33','34','100','200']

cols = ['TS_DCS2_THTLE_PRES', 'TS_DCS2_TOT_STM_FLW', 'TS_DCS2_20LBA10CT601', 'TS_DCS2_TOT_FUEL_FLOW', 'TS_DCS2_FW_FLW_COMP', 'TS_DCS2_TOTAL_AIR_FLOW', 'TS_DCS2_20HNA10CQ101', 'TS_DCS2_20HNA10CQ102', 'TS_DCS2_20HHA10AA129AO', 'TS_DCS2_20HHA10AA128AO', 'TS_DCS2_20HHA10AA127AO', 'TS_DCS2_20HHA10AA126AO', 'TS_DCS2_20HHA10AA125AO', 'TS_DCS2_20HHA10AA124AO', 'TS_DCS2_20HHA10AA123AO', 'TS_DCS2_20HHA10AA122AO', 'TS_DCS2_20HHA10AA121AO', 'TS_DCS2_20HHA10AA120AO', 'TS_DCS2_20HHA10AA119AO', 'TS_DCS2_20HHA10AA118AO', 'TS_DCS2_20HHA10AA117AO', 'TS_DCS2_20HHA10AA116AO', 'TS_DCS2_20HHA10AA115AO', 'TS_DCS2_20HHA10AA114AO', 'TS_DCS2_20HHA10AA113AO', 'TS_DCS2_20HHA10AA112AO', 'TS_DCS2_20HHA10AA111AO', 'TS_DCS2_20HHA10AA132AO', 'TS_DCS2_20HHA10AA133AO', 'TS_DCS2_20HHA10AA134AO', 'TS_DCS2_20HHA10AA135AO', 'TS_DCS2_20HHA10AA136AO', 'TS_DCS2_20HHA10AA137AO', 'TS_DCS2_20HHA10AA130AO', 'TS_DCS2_SCRAINNOX', 'TS_DCS2_SCRBINNOX']

# print('df[cols]',df[cols])

# sns.pairplot(df)
# plt.tight_layout()
#
# if os.path.exists('pairplot.png'):
#     os.remove('pairplot.png')
#
# plt.savefig('pairplot.png')

# plt.show()



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
