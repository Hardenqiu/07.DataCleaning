# coding=utf-8
"""
@File    ：fill.py
@Author  ：Qiu Heng-tan
@Create Date Time  ：2021/5/25
@Modified Date time  :2021/05/28 22：35
@Version  :
@Modified Contents  :
"""

import os
import time
import logging
import pandas as pd
import numpy as np
from data_preprocessing import *
from time_series_data_cleaning import *
from DiscretePointsCleaning import DiscretePointsCleaning
# import xlrd
import csv

# xls文件存放路径
INPUTPATH = u"test-name1.xlsx"

# 生成的csv文件存放路径
# TEMPPATH = u"resampled_temp.csv"

# 生成总的csv文件存放路径
# OUTPATH = u"resampled_out.csv"

sheets = pd.read_excel(INPUTPATH, sheet_name=None)  # 包括全部sheets
# print(sheet)
print('sheet长度:', len(sheets))
# print('一个sheet的值：', sheets)
sheet_num = 0
sheet_num_I = 0
sheet_num_O = 0
# result_table_I = []
# result_table_O = []
# result_table = []
# result_table_I = pd.DataFrame()
# result_table_O = pd.DataFrame()
# result_table = pd.DataFrame()
result_table_header = []
result_table_I_header = []
result_table_O_header = []
for i in sheets.keys():
    # 测点名称
    print(i)

    # 读取sheet  #################################################################################
    sheet = pd.read_excel(INPUTPATH, sheet_name=i)

    print(type(sheet))
    # print('第 %d 个sheet的值：\n'%(sheet_num + 1),sheet)

    # 将sheet分别保存 #############################################################################
    # sheet.to_save('%s.csv'%i)
    if os.path.exists('.\\csvfiles\\%s.csv' % i):
        os.remove('.\\csvfiles\\%s.csv' % i)

    sheet.to_csv('.\\csvfiles\\%s.csv' % i, index=False, header=['', 'time', 'id', 'value'])

    # 根据i的名称，选择合适的搜索半径、阈值
    if i =='TS_DCS2_SCRAINNOX':
        dpc = DiscretePointsCleaning(SearchRadT=120, ThresholdValue=20)
        dpc.run('.\\csvfiles\\%s.csv' % i,'.\\csvfiles\\%s.csv' % i)

    # 重采样 #####################################################################################
    sheet_name = '.\\csvfiles\\%s.csv' % i
    save_sheet_name = '.\\Rcsvfiles\\R_%s.csv' % i
    if os.path.exists(save_sheet_name):
        os.remove(save_sheet_name)

    resample(sheet_name, save_sheet_name, freq="05s", headers=['', i])

    # 读取采样后的数值
    csv = pd.read_csv(save_sheet_name, usecols=[1])

    # 若是输出的量
    if i == "TS_DCS2_SCRAINNOX" or i == "TS_DCS2_SCRBINNOX":
        save_csv_name = '.\\Scsvfiles_O\\S_%s.csv' % i
        if os.path.exists(save_csv_name):
            os.remove(save_csv_name)

        csv.to_csv(save_csv_name, mode='a', index=False, header=[i])

        # 拼接
        if sheet_num_O == 0:
            result_table_O = csv
        else:
            result_table_O = pd.concat([result_table_O, csv], axis=1)

        sheet_num_O += 1
        result_table_O_header.append(i)
        # result_table_O_header.append(str(sheet_num_O))
        # result_table_O.append(csv, ignore_index=True)
        # print(csv)
    else:
        # 若输入的量
        save_csv_name = '.\\Scsvfiles_I\\S_%s.csv' % i
        if os.path.exists(save_csv_name):
            os.remove(save_csv_name)

        csv.to_csv(save_csv_name, mode='a', index=False, header=[i])
        # result_table_I.append(csv, ignore_index=True)
        # print(csv)

        # 拼接
        if sheet_num_I == 0:
            result_table_I = csv
        else:
            result_table_I = pd.concat([result_table_I, csv], axis=1)

        sheet_num_I += 1
        result_table_I_header.append(i)
        # result_table_I_header.append(str(sheet_num_I))

    sheet_num += 1
    result_table_header = result_table_I_header + result_table_O_header
    print('已导出第  %d  个表单\n' % sheet_num)

# print('result_table:', result_table)


# 有输出的时候打开此处
if (len(result_table_O_header) == 0):
    result_table = result_table_I
else:
    result_table = pd.concat([result_table_I, result_table_O], axis=1)
#
# result_table = pd.concat(result_table)
# result_table_I = pd.concat(result_table_I)
# result_table_O = pd.concat(result_table_O)

# save
if os.path.exists(".\\Fcsvfiles\\F_result_table.csv"):
    os.remove(".\\Fcsvfiles\\F_result_table.csv")

if os.path.exists(".\\Fcsvfiles\\F_result_table_I.csv"):
    os.remove(".\\Fcsvfiles\\F_result_table_I.csv")

if os.path.exists(".\\Fcsvfiles\\F_result_table_O.csv"):
    os.remove(".\\Fcsvfiles\\F_result_table_O.csv")

print('result_table_header:', result_table_header)
print('result_table_I_header:', result_table_I_header)
print('result_table_O_header:', result_table_O_header)

result_table.fillna(method='ffill', inplace=True)
# result_table_I.fillna(method = 'ffill', inplace = True)
# result_table_O.fillna(method = 'ffill', inplace = True)

#
result_table.to_csv(".\\Fcsvfiles\\F_result_table.csv", index=True, mode='a', header=result_table_header)
# result_table_I.to_csv(".\\Fcsvfiles\\F_result_table_I.csv", index = True, mode = 'a', header= result_table_I_header)
# result_table_O.to_csv(".\\Fcsvfiles\\F_result_table_O.csv", index = True, mode = 'a', header= result_table_O_header)

# save
if os.path.exists(".\\Res\\result_table_I.csv"):
    os.remove(".\\Res\\result_table_I.csv")

if os.path.exists(".\\Res\\result_table_O.csv"):
    os.remove(".\\Res\\result_table_O.csv")

# 保存不带index的表 ##############################################################################################
df2 = pd.read_csv(".\\Fcsvfiles\\F_result_table.csv")
# print(df2)
df2.to_csv(".\\Res\\result_table.csv",   index=False, header=False, columns=result_table_header)
df2.to_csv(".\\Res\\result_table_I.csv", index=False, header=False, columns=result_table_I_header)
df2.to_csv(".\\Res\\result_table_O.csv", index=False, header=False, columns=result_table_O_header)

# 保存带index的表（以H为标记） #####################################################################################
if os.path.exists(".\\ResH\\Hresult_table.csv"):
    os.remove(".\\ResH\\Hresult_table.csv")

if os.path.exists(".\\ResH\\Hresult_table_I.csv"):
    os.remove(".\\ResH\\Hresult_table_I.csv")

if os.path.exists(".\\ResH\\Hresult_table_O.csv"):
    os.remove(".\\ResH\\Hresult_table_O.csv")

df2.to_csv(".\\ResH\\Hresult_table.csv",   index=True, header=result_table_header, columns=result_table_header)
df2.to_csv(".\\ResH\\Hresult_table_I.csv", index=True, header=result_table_I_header, columns=result_table_I_header)
df2.to_csv(".\\ResH\\Hresult_table_O.csv", index=True, header=result_table_O_header, columns=result_table_O_header)


print("-"*100)
##################################################################################################
df = pd.read_csv('.\\ResH\\Hresult_table.csv',
                 index_col= 0
                 # header=None,
                 # sep='\s+'
                 )

flag = 0   # o画直方图  1画热力图
if(flag == 0):
    # 获取直方图
    sns.pairplot(df)
    plt.tight_layout()
    if os.path.exists('pairplot.png'):
        os.remove('pairplot.png')
    plt.savefig('pairplot.png')

    # plt.show()
else:
    # 获取热力图
    cols = result_table_header
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

############################################################################################################


# result_table.to_csv(".\\Fcsvfiles\\F_result_table.csv", index = False, columns= result_table_header)
# result_table.to_csv(".\\Fcsvfiles\\F_result_table_I.csv", index = False, columns= result_table_I_header)
# result_table.to_csv(".\\Fcsvfiles\\F_result_table_O.csv", index = False, columns= result_table_O_header)

# result_table.to_csv(".\\result_table.csv", index = False, mode = 'a', header= False)
# result_table_I.to_csv(".\\result_table_I.csv", index = False, mode = 'a', header= False)
# result_table_O.to_csv(".\\result_table_O.csv", index = False, mode = 'a', header= False)
# result_table.to_csv(".\\result_table.csv", index = True, mode = 'a', header= result_table_header)
# result_table_I.to_csv(".\\result_table_I.csv", index = True, mode = 'a', header= result_table_I_header)
# result_table_O.to_csv(".\\result_table_O.csv", index = True, mode = 'a', header= result_table_O_header)


# for i in range(len(sheets)):
#     sheet = pd.read_excel(INPUTPATH,sheet_name=i)
#     print(sheet.keys())


# if __name__ == '__main__':
#     filename = "test-20210401.csv"
#     save_path = "./resampled_csv.csv"
#     resample(filename, save_path, freq='5S')
#     # df = pd.read_csv("test-20210401.csv")
#     # print(df)
#     # print(df['time'][0])
#     # print(df['id'][0])
#     # print(df['value'][0])
#     # A = np.array(20000, 2)
#     # print(A.size())
