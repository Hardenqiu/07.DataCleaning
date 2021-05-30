# coding=utf-8
"""
@File    ：DiscretePointsCleaning.py
@Author  ：Qiu Heng-tan
@Create Date Time  ：2021/5/25
@Modified Date time  :2021/05/28 22：35
@Version  :
@Modified Contents  :
"""
import pandas as pd


class DiscretePointsCleaning(object):

    def __init__(self, SearchRadT, ThresholdValue):
        # 基于时间的搜索半径、搜索阈值
        self.SearchRadT = SearchRadT  ## 秒 时间内 即x轴 100
        self.ThresholdValue = ThresholdValue  # 要对比的那点在y轴上的距离

        # 有效点计算模式
        # self.VALIDMODE = 'PointsPercent'
        self.VALIDMODE = 'PointsNum'

        # 有效点取百分比（请取0~1内小数）
        self.PointsLeftPercent = 0.5
        self.PointsRightPercent = 0.5
        self.PointsPercent = 0.6

        # 有效点个数（请取整数）
        self.resolution = 5
        self.PointsLeftNum = int((SearchRadT + self.resolution) / self.resolution)
        self.PointsRightNum = int((SearchRadT + self.resolution) / self.resolution)
        self.PointsNum = int((self.PointsLeftNum + self.PointsRightNum) * 0.6)

    # 读取csv数据，并复制一份
    def read_csv(self, csv_dir):
        raw_data = pd.read_csv(csv_dir, index_col=0)
        copy_data = raw_data.copy()
        return raw_data, copy_data

    # 直接保存csv文件，不需要返回数据
    def save_csv(self, raw_data, csv_dir):
        raw_data.to_csv(csv_dir)

    # 遍历数据点，并删除离散点
    def inquire_discrete_points(self, raw_data, copy_data):

        # 点的个数
        points_num = copy_data.shape[0]

        # 遍历每个点
        for i in range(points_num):
            #
            # 初始化函数内部需要使用的计数器、标记量
            InternalNum = 0  # 内部点计数
            InternalLeftNum = 0  # 左区间内部的点数
            InternalRightNum = 0  # 右区间内部的点数

            counter1 = 0  # 计数器1
            counter2 = 0  # 计数器2
            counter = 0  # 计数器

            # 区间计算开关
            flag1 = True  # 设置左区间计算开关
            flag2 = True  # 设置右区间计算开关

            # 遍历该点的左区间内的点
            while flag1:
                counter1 += 1

                # 统计左区间内的点
                h = i - counter1

                if h >= 0:  # 保证在数据内可查
                    # 该点与左区间的点的时差
                    timedelta1 = pd.to_datetime(copy_data.loc[i]['time']) - pd.to_datetime(
                        copy_data.loc[h]['time'])

                    # 换算成秒，便于设置以时间（单位：秒）为查询半径。
                    DiffLeftTime = timedelta1.days * 86400 + timedelta1.seconds

                    # 判断是否在区间内
                    if DiffLeftTime <= self.SearchRadT:  # 在查询半径内（单位：秒）
                        # 求欧拉距离
                        # 两点的差值
                        DiffLeft = copy_data.loc[i]['value'] - copy_data.loc[h]['value']
                        # DistanceLeft = (DiffLeftTime ** 2 + DiffLeft ** 2) ** 0.5
                        DistanceLeft = abs(DiffLeft)

                        # 判断距离是否在阈值内
                        if DistanceLeft <= self.ThresholdValue:  # 在阈值内
                            # 在，左区间计数+1
                            InternalLeftNum += 1

                            # 判断是否符合条件，可提前结束该点的判断。(只在有效点个数模式下判断）
                            if self.VALIDMODE == 'PointsNum':
                                # 判断点数是否已符合
                                if InternalLeftNum >= self.PointsLeftNum:
                                    # 可以退出查询，且右区间也不必查询
                                    flag1 = False
                                    flag2 = False
                                else:  # 这里是不符合。
                                    pass  # 不执行操作，继续遍历。

                        else:
                            pass  # 不做计数处理

                    else:
                        # 不在查询半径内（单位：秒）
                        # 退出此遍历
                        flag1 = False

                else:
                    # 无可查的数据，停止查询
                    flag1 = False

                # 100次查询后，强制退出，防止多点聚集导致无法收敛。
                if counter1 >= 100:
                    flag1 = False

            # 遍历该点的右区间内的点
            while flag2:
                counter2 += 1

                # 统计右区间内的点
                k = i + counter2
                # 判断区间点是否在表中
                if k < copy_data.shape[0]:
                    # 该点与右区间的点的时差
                    timedelta2 = pd.to_datetime(
                        copy_data.loc[k]['time']) - pd.to_datetime(copy_data.loc[i]['time'])

                    # 换算成秒，便于设置以时间（单位：秒）为查询半径。
                    DiffRightTime = timedelta2.days * 86400 + timedelta2.seconds

                    # 判断是否在区间内
                    if DiffRightTime <= self.SearchRadT:  # 在查询半径内（单位：秒）
                        # 求欧拉距离
                        # 两点的差值
                        DiffRight = copy_data.loc[i]['value'] - copy_data.loc[k]['value']
                        # DistanceRight = (DiffRightTime ** 2 + DiffRight ** 2) ** 0.5
                        DistanceRight = abs(DiffRight)

                        # 判断距离是否在阈值内
                        if DistanceRight <= self.ThresholdValue:  # 在阈值内
                            # 在，右区间计数+1
                            InternalRightNum += 1

                            # 判断是否符合条件，可提前结束该点的查询。（只在有效点个数模式下判断）
                            if self.VALIDMODE == 'PointsNum':
                                # 判断点数是否已符合
                                if InternalRightNum >= self.PointsRightNum:
                                    # 可以退出查询
                                    flag2 = False

                                else:
                                    pass  # 不执行操作，继续遍历。
                        else:
                            pass  # 不执行操作，继续遍历。

                else:  # 在表中无法查询到，中止查询。
                    flag2 = False

                # 100次查询后，强制退出，防止多点聚集导致无法收敛。
                if counter2 >= 100:
                    flag2 = False

            # 总计算的点
            counter = counter1 + counter2  # 总查询的次数
            InternalNum = InternalLeftNum + InternalRightNum  # 总实际检索到的点

            # 根据选择的模式判定是否删除该点的数据
            if self.VALIDMODE == 'PointsPercent':
                validflag = InternalNum >= counter * self.PointsPercent

            elif self.VALIDMODE == 'PointsNum':
                validflag = (
                        InternalLeftNum >= self.PointsLeftNum or InternalRightNum >= self.PointsRightNum or InternalNum >= self.PointsNum)

            else:
                print("请选择正确的评价模式！\n")
                return -1

            if validflag:
                pass  # 周围有足够多的点，不应删除该点。

            else:
                raw_data.at[i, 'value'] = None
        return raw_data

    # 将算法封装
    def run(self, csv_dir, save_csv_dir):
        raw_data, copy_data = self.read_csv(csv_dir)
        Processed_data = self.inquire_discrete_points(raw_data, copy_data)
        self.save_csv(Processed_data, save_csv_dir)


if __name__ == '__main__':
    dpc = DiscretePointsCleaning(SearchRadT=20, ThresholdValue=10)
    csv_dir = "TS_DCS2_SCRBINNOX.csv"
    save_csv_dir = "TS_DCS2_SCRBINNOX1.csv"
    dpc.run(csv_dir, save_csv_dir)

    # raw_data, copy_data = dpc.read_csv(csv_dir)
    # print(raw_data)
    # print('*' * 100)
    # print(copy_data)
    # print('-' * 100)
    # Processed_data = dpc.inquire_discrete_points(raw_data, copy_data)
    # print(Processed_data)
    # save_csv_dir = "TS_DCS2_SCRBINNOX - 副本1.csv"
    # dpc.save_csv(Processed_data, save_csv_dir)
