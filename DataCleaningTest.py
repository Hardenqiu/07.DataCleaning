# import os
#
# import pandas as pd
# from matplotlib import pyplot as plt
# import numpy as np
# import csv
# #用来正常显示中文标签
# plt.rcParams['font.sans-serif']=['SimHei']
# #用来正常显示负号
# plt.rcParams['axes.unicode_minus']=False
#
# #定义两个空列表存放x,y轴数据点
# x=[]
# y=[]
# value = []
# with open("TS_DCS2_SCRBINNOX.csv",'r') as csvfile:
#     plots = csv.reader(csvfile, delimiter=',')
#
#     aa = 0
#
#     for row in plots:
#         if(aa ==0):
#             aa = 1
#         else:
#             aa += 1
#             print(row)
#
#             x.append(row[0])  #从csv读取的数据是str类型
#     #         print("x:",x)
#             y.append(row[3])
#
#             # value.append(row[3])
#     #         print("y:",y)
# #画折线图
#
#
# # plt.plot(x,y,label='模拟数据')
# plt.scatter(x, y,label='模拟数据')
# plt.xlabel('x')
# plt.ylabel('y')
# plt.title('演示从文件加载数据')
# plt.legend()
# # if os.path.exists('显示散点图.png'):
# #     os.remove('显示散点图.png')
# #
# # plt.savefig('显示散点图.png')
# plt.show()
#

import numpy as np
import pandas as pd


SearchRadT = 100  ## 秒 时间内 即x轴

ThresholdVal = 120  # 暂设100，即欧拉距离

# 有效点计算模式
# VALIDMODE = 'PointPercent'
VALIDMODE = 'PointNum'
# 有效点取百分比（请取0~1内小数）
PointsUpPercent = 0.5
PointsDownPercent = 0.5
PointsPercent = 0.6

# 有效点个数（请取整数）
PointsUpNum = 5
PointsDownNum = 5
PointsNum = 10

# 使用read_csv读取数据
hr = pd.read_csv("TS_DCS2_SCRBINNOX - 副本.csv", index_col=0)
hrcp = hr.copy()
# print(hrcp)
print(hrcp.loc[0]['time'])

print(hrcp.loc[1]['time'])
# print(hrcp.loc[1]['time']-hrcp.loc[0]['time'])
print(pd.to_datetime(hrcp.loc[1]['time']))
print(pd.to_datetime(hrcp.loc[0]['time']))
delta = pd.to_datetime(hrcp.loc[1]['time']) - pd.to_datetime(hrcp.loc[0]['time'])
# delta1 = pd.to_datetime(hrcp.loc[0]['time']) - pd.to_datetime(hrcp.loc[1]['time'])
print('时间差：', delta)
# print('时间差：', delta1)
print('天：', delta.days)
print('秒：', delta.seconds)
print('换算成秒：', delta.days * 86400 + delta.seconds)

print('尺寸：', hrcp.shape[0])
#
for i in range(hrcp.shape[0]):  # 遍历行数

    InternalNum = 0  # 内部点计数
    InternalUpNum = 0  # 内部的上面点数
    InternalDownNum = 0  # 内部的下面点数

    # SearchNum = 10

    counter1 = 0  # 计数器1
    counter2 = 0  # 计数器1

    # 上时间差
    flag1 = True  # 设置上区间计算开关
    flag2 = True  # 设置下区间计算开关

    # 上区间统计
    while (flag1):
        counter1 += 1
        # 统计上面在内的点
        if (i - counter1 >= 0):
            timedelta1= pd.to_datetime(hrcp.loc[i]['time']) - pd.to_datetime(hrcp.loc[i - counter1]['time'])
            DiffUpT = timedelta1.days*86400 + timedelta1.seconds
            if(DiffUpT<SearchRadT):
                # 如果在设置半径内
                DiffUp = hrcp.loc[i]['value'] - hrcp.loc[i - counter1]['value']
                DistanceUp = (DiffUpT ** 2 + DiffUp ** 2) ** 0.5

                print('该点与当前点的距离U：%d\n\n'% DistanceUp)
                if (DistanceUp <= ThresholdVal):
                    InternalUpNum += 1


                    # 这里提前判断，节约计算资源。
                    if (VALIDMODE == 'PointPercent'):
                        validflag = (
                                    InternalUpNum > counter1 * PointsUpPercent or InternalDownNum > counter2 * PointsDownPercent or InternalNum > (
                                        counter1 + counter2) * PointsPercent)
                    elif (VALIDMODE == 'PointNum'):
                        validflag = (
                                    InternalUpNum > PointsUpNum or InternalDownNum > PointsDownNum or InternalNum > PointsNum)
                    else:
                        print("请选择正确的评价模式！\n")
                        break

                    if (~validflag):
                        pass  # 不做处理
                    else:
                        hr.at[i, 'value'] = None

                        flag1 = False   # 删除相应数值后，退出该循环。



                else:
                    pass  # 不做计数处理
            else:
                flag1 = False  # 大于搜索半径将推出该循环
        else:
            flag1 = False

        if(counter1 > 100): # 强制退出该循环
            flag1 = False

    # 下区间统计
    while (flag2):
        counter2 += 1
        # 统计下面在内的点
        # 判读区间点是否在表中
        if (i + counter2 < hrcp.shape[0]):
            timedelta2 = pd.to_datetime(hrcp.loc[i + counter2]['time']) - pd.to_datetime(hrcp.loc[i]['time'])
            print(timedelta2)
            DiffDownT = timedelta2.days*86400 + timedelta2.seconds

            if(DiffDownT <= SearchRadT):
                # 如果在设置半径内
                DiffDown = hrcp.loc[i + counter2]['value'] - hrcp.loc[i]['value']
                DistanceDown = (DiffDownT ** 2 + DiffDown ** 2) ** 0.5

                print('该点与当前点的距离D：%d\n\n'% DistanceDown)

                if (DistanceDown <= ThresholdVal):
                    InternalDownNum += 1


                    # 这里提前判断，节约计算资源。
                    if (VALIDMODE == 'PointPercent'):
                        validflag = (
                                    InternalUpNum > counter1 * PointsUpPercent or InternalDownNum > counter2 * PointsDownPercent or InternalNum > (
                                        counter1 + counter2) * PointsPercent)
                    elif (VALIDMODE == 'PointNum'):
                        validflag = (
                                    InternalUpNum > PointsUpNum or InternalDownNum > PointsDownNum or InternalNum > PointsNum)
                    else:
                        print("请选择正确的评价模式！\n")
                        break

                    if (~validflag):
                        pass  # 不做处理
                    else:
                        hr.at[i, 'value'] = None

                        flag2 = False   # 删除相应数值后，退出该循环。

                else:
                    pass  # 不做计数处理
            else:
                flag2 = False   # 大于搜索半径将推出该循环
        else:
            flag2 = False

        if (counter2 >= 100):  # 强制退出该循环
            flag2 = False

    InternalNum = InternalUpNum + InternalDownNum

    if(VALIDMODE == 'PointPercent'):
        validflag = (InternalUpNum > counter1 * PointsUpPercent or InternalDownNum > counter2 * PointsDownPercent or InternalNum > (counter1+counter2)*PointsPercent)
    elif(VALIDMODE == 'PointNum'):
        validflag = (InternalUpNum > PointsUpNum or InternalDownNum > PointsDownNum or InternalNum > PointsNum)
    else:
        print("请选择正确的评价模式！\n")
        break

    if(validflag):
        pass  # 不做处理
    else:
        hr.at[i, 'value'] = None

    print('-' * 100)

# print(hrcp.loc[0]['value'])
# if (hrcp.loc[0]['value']> 100):
#     pass
#     hr.at[0, 'value']=

# print(hr)
hr.to_csv("TS_DCS2_SCRBINNOX - 副本1.csv")
print('*' * 100)
# print(hrcp)
