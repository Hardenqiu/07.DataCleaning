#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os

import pandas as pd

# # 相对路径常量
# UPLOADED_FILE_PATH = '.\\Scsvfiles_I\\'
#
# # 获取文件目录下的文件列表
# file_list = os.listdir(UPLOADED_FILE_PATH)
# # print(file_list)
#
# df1 = pd.read_csv(UPLOADED_FILE_PATH + file_list[0])
# # print(df1)
# for i in range(1, len(file_list)):
#     df2 = pd.read_csv(UPLOADED_FILE_PATH + file_list[i])
#     df1 = pd.concat([df1, df2], axis=1)
#
# if os.path.exists(".\\" + 'merged_file.csv'):
#     os.remove(".\\" + 'merged_file.csv')
# df1.to_csv('merged_file.csv', index=False, header=-1)

dff = pd.read_csv("result_table_O.csv")

# print('dff数据：', dff[0:10])
print('dff特定位置的数据：', dff.loc[7][0])
print('dff特定位置的数据：', dff.loc[7])


# 读取第一个CSV文件并包含表头
# df1 = pd.read_csv(UPLOADED_FILE_PATH + file_list[0], encoding="gbk")  # 编码默认UTF-8，若乱码自行更改
# # 循环遍历列表中各个CSV文件名，并追加到合并后的文件
# for i in range(1, len(file_list)):
#     df2 = pd.read_csv(UPLOADED_FILE_PATH + file_list[i], encoding="gbk")
#     # 合并
#     df1 = df1.merge(df2)
#     # 展示全部列
#     pd.options.display.max_columns = None
#     # 展示前两行
#     print(df1[0:2])
# # 写进文件里去，这里有很多设置参数的，需要的自己调
# df1.to_csv(PROCESSED_FILE_PATH + 'merged_file.csv', index=False)