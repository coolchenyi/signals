# -*- coding:utf-8 -*-
'''
在k线图上画趋势线，分最高价的趋势线、最低价的趋势线、收盘价的趋势线

'''

import stockstats
import numpy as np
import pandas as pd
import datetime
import mysql.connector
import matplotlib.finance as mpf
import matplotlib.pyplot as plt

from sklearn import linear_model

start_time = datetime.date(2017, 6, 1)
end_time = datetime.date(2018, 1, 3)
start_time1 = datetime.date(2017, 12, 21)
end_time1 = datetime.date(2018, 1, 3)
stock_id = '000938'
#########
#数据库连接部分
cnx = mysql.connector.connect(user='ai_team', password='123456', host='120.26.72.215', database='smartk_demo',
port='3306')
cursor = cnx.cursor()

############


def get_kline_trend(start_time, end_time, stock_id, period='day', price_type='high'):
    '''
    传入开始时间和结束时间，交易周期（日、60分钟、30分钟），拟合的价格类
    获取时间段内的最高价/最低价/收盘价
    将时间坐标进行整型数转换，起点时间为0
    采用普通最小二乘法进行线性回归  Ordinary Least Squares，得到斜率和截距系数
    再将开始时间和结束时间代入线性回归模型，得到趋势线起点与终点预测值

    '''
    query = "SELECT date,open,high,close,low,volume,amount,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
    cursor.execute(query, (start_time, end_time, stock_id))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'close', 'low', 'volume', 'amount', 'code'])
    time_map = df['date']  # 构造一个交易时间与X轴坐标的映射 ,取整型数
    x_data = time_map.index.values
    y_data = df['high'].values
    # print(x_data, y_data)
    xt = x_data.reshape(-1,
                        1)  # 将x数据转化为n samples，1 feature 的数据，numpy array or sparse matrix of shape [n_samples,n_features]
    yt = y_data.reshape(-1, 1)  # 将y数据转化为n samles，1 target 的数据，numpy array of shape [n_samples, n_targets]
    # print(xt, yt)
    reg = linear_model.LinearRegression()
    reg.fit(xt, yt)
    # LinearRegression(copy_X=True, fit_intercept=True, n_jobs=1, normalize=False)
    y_start = reg.predict([0])
    y_end = reg.predict(x_data[-1])
    trend_points = pd.Series({time_map.iloc[0]: y_start[0][0], time_map.iloc[-1]: y_end[0][0]})
    # print(trend_points )
    return trend_points


print(get_kline_trend(start_time,end_time,stock_id))

def draw_kline_and_trend_line(start_time, end_time, stock_id, trend_points):

    # 重新选择k线图绘制的时间区间,注意顺序为date，open，close，high，low
    query = "SELECT date,open,close,high,low,volume,amount,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
    cursor.execute(query, (start_time, end_time, stock_id))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high',  'low', 'volume', 'amount', 'code'])
    fig, ax = plt.subplots(figsize=(14, 7))
    qutotes = []
    for index,(d,o,c,h,l) in enumerate(
        zip(df.date, df.open, df.close, df.high, df.low )
    ):
        #蜡烛图的日期使用date2num转化为数值
        d = mpf.date2num(d)
        val = (d,o,c,h,l)
        qutotes.append(val)
    #使用candlestick_ochl函数绘图，ochl代表open，close，high，low
    mpf.candlestick_ochl(ax, qutotes, width=0.6, colorup='red', colordown='green', alpha=0.75)
    ax.autoscale_view()
    ax.xaxis_date()
    plt.plot(trend_points.index,trend_points.values)

    plt.show()
    return

trend_points1 = get_kline_trend(start_time1,end_time1,stock_id)
draw_kline_and_trend_line(start_time, end_time, stock_id, trend_points1)