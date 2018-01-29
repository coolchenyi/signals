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
from macd_indicator import MACD_INDICATOR

start_time = datetime.date(2017,11,1)
end_time = datetime.date(2017,12,30)
start_time1 = datetime.date(2017, 12, 21)
end_time1 = datetime.date(2018, 1, 18)
stock_id = '600031'
#########
#数据库连接部分


cursor = cnx.cursor()

############

def get_peak_points(start_time, end_time, stock_id, period='day', price_type='high'):
    '''

    :param start_time: 时间段内的开始时间
    :param end_time: 时间段内的结束时间
    :param stock_id: 获取顶点的股票
    :param period: 时间周期，以日、60分钟、30分钟、15分钟等为间隔
    :param price_type: 默认按照最高价'high'来判断最高点
    :return:返回该时间段内的价格顶点
    取5个点的数据，判断中间点的数值是否是最大值，如是则判定为顶点

    '''
    query = "SELECT date,open,high,close,low,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
    cursor.execute(query, (start_time, end_time, stock_id))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'close', 'low', 'code'])
    df = df.sort_values(by=['date'], ascending=True)
    points_num = 5  # 默认在5个点中取顶点
    rolling_data_max = df[price_type].rolling(points_num, center=True).max()
    peak_point_time_list = []
    peak_point_value_list = []
    for i in range(int(points_num/2), len(df)-int(points_num/2)):
        middle_point = df.iloc[i]['date']
        middle_point_value = float(df.iloc[i][price_type])  #注意数据类型转换
        print(rolling_data_max[i],middle_point_value)
        if rolling_data_max[i] == middle_point_value:  # 判断是否顶点
            peak_point_time_list.append(middle_point)
            peak_point_value_list.append(middle_point_value)
    s = pd.Series(peak_point_value_list, index=peak_point_time_list)
    return s

# print(get_peak_points(start_time, end_time, stock_id))

def get_bottom_points(start_time, end_time, stock_id, period='day', price_type='low'):
    '''

    :param start_time: 时间段内的开始时间
    :param end_time: 时间段内的结束时间
    :param stock_id: 获取底点的股票
    :param period: 时间周期，以日、60分钟、30分钟、15分钟等为间隔
    :param price_type: 默认按照最低价'low'来判断最低点
    :return:返回该时间段内的价格底点
    取5个点的数据，判断中间点的数值是否是最小值，如是则判定为底点

    '''
    query = "SELECT date,open,high,close,low,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
    cursor.execute(query, (start_time, end_time, stock_id))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'close', 'low', 'code'])
    df = df.sort_values(by=['date'], ascending=True)
    points_num = 5  # 默认在5个点中取顶点
    rolling_data_min = df[price_type].rolling(points_num, center=True).min()
    bottom_point_time_list = []
    bottom_point_value_list = []
    for i in range(int(points_num/2), len(df)-int(points_num/2)):
        middle_point = df.iloc[i]['date']
        middle_point_value = float(df.iloc[i][price_type])  #注意数据类型转换
        print(rolling_data_min[i],middle_point_value)
        if rolling_data_min[i] == middle_point_value:  # 判断是否顶点
            bottom_point_time_list.append(middle_point)
            bottom_point_value_list.append(middle_point_value)
    s = pd.Series(bottom_point_value_list, index=bottom_point_time_list)
    return s

print(get_bottom_points(start_time, end_time, stock_id))

def get_kline_trend(start_time, end_time, stock_id, period='day', price_type='high'):
    '''

    :param start_time:时间段内的开始时间
    :param end_time:时间段内的结束时间
    :param stock_id:股票代码
    :param period:交易周期，日、60分钟、30分钟、15分钟
    :param price_type:按照哪个价格进行趋势线拟合
    :return:
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
    xt = x_data.reshape(-1, 1)  # 将x数据转化为n samples，1 feature 的数据，numpy array or sparse matrix of shape [n_samples,n_features]
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


# print(get_kline_trend(start_time,end_time,stock_id))

def draw_kline_macd_trend_line(start_time, end_time, stock_id, trend_points = None):
    '''
    :param start_time: 图像绘制的时间坐标轴上开始时间
    :param end_time: 图像绘制的时间坐标轴上的结束时间
    :param stock_id: 所绘图像的股票代码
    :param trend_points: 线性回归拟合后的趋势线段两端的顶点
    :return:绘出图像，返回空
    绘图步骤：
    1、绘制K线图
    2、绘制k线趋势线
    3、绘制macd指标图
    4、绘制macd指标变化趋势线
    '''
    # 重新选择k线图绘制的时间区间,注意顺序为date，open，close，high，low
    query = "SELECT date,open,close,high,low,volume,amount,lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s and code=%s"
    cursor.execute(query, (start_time, end_time, stock_id))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high',  'low', 'volume', 'amount', 'code'])
    qutotes = []
    for index,(d,o,c,h,l) in enumerate(
        zip(df.date, df.open, df.close, df.high, df.low )
    ):
        #蜡烛图的日期使用date2num转化为数值
        d = mpf.date2num(d)
        val = (d,o,c,h,l)
        qutotes.append(val)
    #绘制图表
    plt.rc('axes', grid=True)
    plt.rc('grid', color='0.75', linestyle='-', linewidth=0.5)
    left, width = 0.1, 0.8
    rect1 = [left, 0.3, width, 0.6]
    rect2 = [left, 0.1, width, 0.2]
    fig = plt.figure(figsize=(16,9),facecolor='white')
    axescolor = '#f6f6f6'  # the axes background color

    ax1 = fig.add_axes(rect1, axisbg=axescolor)  # left, bottom, width, height
    plt.title('Stock ID:'+stock_id+'   Data start from:'+str(start_time)+' to :'+str(end_time))
    ax2 = fig.add_axes(rect2, axisbg=axescolor, sharex=ax1)

    # plt.title('ssssss')
    #绘制k线图，使用candlestick_ochl函数绘图，ochl代表open，close，high，low
    mpf.candlestick_ochl(ax1, qutotes, width=0.6, colorup='red', colordown='green', alpha=0.75)
    ax1.autoscale_view()
    ax1.xaxis_date()
    # ax1.plot(trend_points.index,trend_points.values)
    #绘制macd图,因macd慢线由26天数据计算，将计算的数据期间拉长
    before_start_time = start_time-datetime.timedelta(days=60)
    macd_data = MACD_INDICATOR(before_start_time, end_time, stock_id, 'day')
    #开始时间与k线图上的开始时间对齐
    macd_data.stock_data.drop(macd_data.stock_data[macd_data.stock_data.index<start_time].index, inplace=True)
    kl_index = macd_data.stock_data.index
    dif = macd_data.stock_data['macd']
    dea = macd_data.stock_data['macds']
    bar = macd_data.stock_data['macdh']
    ax2.plot(kl_index, dif, label='macd dif')
    ax2.plot(kl_index, dea, label='dea')
    bar_red = np.where(bar > 0, bar, 0)
    bar_green = np.where(bar < 0, bar, 0)
    # 绘制bar>0的柱状图
    ax2.bar(kl_index, bar_red, facecolor='red', label='hist bar')
    # 绘制bar<0的柱状图
    ax2.bar(kl_index, bar_green, facecolor='green', label='hist bar')
    # ax2.legend(loc='left')
    #绘制macd顶点连线
    macd_peak_points = macd_data.get_peak_points()
    ax2.plot(macd_peak_points.index, macd_peak_points.values, color='red')
    #绘制k线趋势线
    for i in range(len(macd_peak_points)-1):
        trend_points = get_kline_trend(macd_peak_points.index[i],macd_peak_points.index[i+1], stock_id)
        ax1.plot(trend_points.index,trend_points.values)
    #输出股票每段趋势线的开始结束时间
    trendline_list = []
    for i in range(len(macd_peak_points)-1):
        trendline_list.append([stock_id,macd_peak_points.index[i],macd_peak_points.index[i+1],None])
    trendline_df = pd.DataFrame(trendline_list,columns=['stock_id','start_time','end_time','signal'])
    trendline_output_path = r'C:\smartkline\output\macd\\' + stock_id + '_'+str(start_time) + '_'+str(end_time) + '.csv'
    trendline_df.to_csv(trendline_output_path)
    #输出图表
    fig_output_path = r'C:\smartkline\output\macd\\' + stock_id + '_'+str(start_time) + '_'+str(end_time) + '.png'
    plt.savefig(fig_output_path)
    # plt.show()
    return

#测试数据
# draw_kline_macd_trend_line(start_time,end_time,stock_id)
#批量生成
# query1 = "SELECT DISTINCT lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s"
# cursor.execute(query1,(start_time, end_time))
# data = cursor.fetchall()
# # s1 = pd.Series(data)
# print(data)
# for i in range(3024, len(data)):
#     draw_kline_macd_trend_line(start_time,end_time, data[i][0])
