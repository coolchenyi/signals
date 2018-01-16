# -*- coding:utf-8 -*-

import datetime
# import numpy as np
import pandas as pd
import mysql.connector

import stockstats


startdate = datetime.date(2017, 6, 1)
today = enddate = datetime.date.today()
stock_id = '000938'


class MACD_INDICATOR(object):

    # 获取股票或指数的MACD指标数据

    def __init__(self, start_time, end_time, code, period='day', ):
        self.indicator_start_time = start_time
        self.indicator_end_time = end_time
        self.indicator_code = code
        self.period = period
        cnx = mysql.connector.connect(user='ai_team', password='123456', host='120.26.72.215', database='smartk_demo',
                                      port='3306')
        cursor = cnx.cursor()
        query = "SELECT date,open,high,close,low,volume,amount,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
        cursor.execute(query, (start_time, end_time, code))
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'close', 'low', 'volume', 'amount', 'code'])
        if len(df) < 30:  # 因MACD默认的慢线参数是26，如要获取准确的MACD值，尽量延长时间周期
            print('time period is too short for MACD calculation')
        df = df.sort_values(by=['date'], ascending=True)
        self.stock_data = stockstats.StockDataFrame.retype(df)
        self.stock_data.get('macd')

    def __str__(self):
        return self.stock_data.__str__()

    # 获取MACD指标，DIFF值的顶点。取5个点的数据，判断中间点的数值是否是最大值，如是则判定为顶点
    def get_peak_points(self, parameter='macd'):
        #         print(self.stock_data)
        #         self.stock_data.to_csv(r'c:\smartkline\output\stock_data.csv')
        macd_data = self.stock_data[parameter]
        points_num = 5  # 默认在5个点中取顶点
        rolling_data_max = self.stock_data[parameter].rolling(points_num, center=True).max()
        if len(macd_data) < points_num:
            raise ValueError  # 点数不够，无法判断顶点
        peak_point_list = []
        for i in range(int(points_num/2), len(macd_data.index)-int(points_num/2)):
            middle_point = macd_data.index[i]
            middle_point_value = macd_data.loc[macd_data.index[i]]
            if middle_point_value == rolling_data_max.loc[middle_point]:  # 判断是否顶点
                peak_point_list.append([middle_point, middle_point_value])
        #         print(peak_point_list)
        return peak_point_list

    # 获取macd指标，diff值的底点。取5个点的数据，判断中间点的数值是否是最小值，如是则判定为底点
    def get_bottom_points(self, parameter='macd'):
        macd_data = self.stock_data[parameter]
        points_num = 5  # 默认在5个点中取顶点
        rolling_data_min = self.stock_data[parameter].rolling(points_num, center=True).min()
        if len(macd_data) < points_num:
            raise ValueError
        bottom_point_time_list = []
        bottom_point_value_list = []
        for i in range(int(points_num/2), len(macd_data.index)-int(points_num/2)):
            middle_point = macd_data.index[i]
            middle_point_value = macd_data.loc[macd_data.index[i]]
            if middle_point_value == rolling_data_min.loc[middle_point]:  # 判断是否底点
                bottom_point_time_list.append(middle_point)
                bottom_point_value_list.append(middle_point_value)
        s = pd.Series(bottom_point_value_list, index=bottom_point_time_list)
        return s

# 获取MACD指标DIFF值顶点/底点对应的K线值，最高/最低价
    def get_kline_high_price(self):
        pass

    def get_kline_low_price(self):
        pass


#以下为测试
stock_macd = MACD_INDICATOR(startdate, enddate, stock_id)
print(stock_macd)
print(stock_macd.get_peak_points())  # print(stock_macd.get_bottom_points())
