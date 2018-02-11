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
import matplotlib.ticker as ticker

from sklearn import linear_model


# start_time = datetime.date(2017,9,1)
# end_time = datetime.date(2018,2,2)

start_time = datetime.datetime(2018,1,1,10,0,0)
end_time = datetime.datetime(2018,2,5,15,0,0)
stock_id = '601998'
############
#数据库连接部分

cursor = cnx.cursor()

###########
###########
#指标数据源构造
# before_start_time = start_time-datetime.timedelta(days=60)  # 因MACD默认的慢线参数是26，如要获取准确的MACD值，尽量延长时间周期
before_start_time = start_time-datetime.timedelta(days=10)  #分钟粒度数据
# query = "SELECT date,open,high,close,low,lpad(code,6,'0') FROM stock_market_hist_kline_day WHERE date BETWEEN %s and %s and code=%s"
query = "SELECT date,open,high,close,low,lpad(code,6,'0') FROM smartk_demo.stock_mk_kline_sixty WHERE date BETWEEN %s and %s and code=%s"
cursor.execute(query, (before_start_time, end_time, stock_id))
data = cursor.fetchall()
df = pd.DataFrame(data, columns=['date', 'open', 'high', 'close', 'low', 'code'])
df = df.sort_values(by=['date'], ascending=True)
data_source = stockstats.StockDataFrame.retype(df)
data_source.get('macd')
data_source = data_source.drop(data_source[data_source.index < start_time].index)
# print(data_source)
##########

def get_ax_position(time_series):
    # 将时间转化为x轴上整型数位置
    ax_index = pd.Series(data_source.index)
    s1 = ax_index.where(ax_index.isin(time_series.values))
    s1.dropna(inplace = True)
    # print(s1)
    return s1.index.values

def get_kline_peak_points(data_source, fit_data_type='close', points_num=5):
    '''

    :param data_source: 数据源
    :param fit_data_type: 默认按照最高价'high'来判断最高点
    :return:返回该时间段内的价格顶点
    如果是30分钟周期，默认取7个点的数据，如果是60分钟或日为交易周期，默认取5个点的数据。判断中间点的数值是否是最大值，如是则判定为顶点

    '''

    rolling_data_max = data_source[fit_data_type].rolling(points_num, center=True).max()
    # print(rolling_data_max)
    peak_point_time_list = []
    peak_point_value_list = []
    last_max_point_pos = 0
    for i in range(int(points_num/2), len(data_source)-int(points_num/2)):
        middle_point_time = data_source.index[i]
        middle_point_value = float(data_source.iloc[i][fit_data_type])  #注意数据类型转换
        if (rolling_data_max[i] == middle_point_value) and ((i-last_max_point_pos)>=points_num):  # 判断是否顶点,顶点间隔大于取数点数
            last_max_point_pos = i
            peak_point_time_list.append(middle_point_time)
            peak_point_value_list.append(middle_point_value)
    s = pd.Series(peak_point_value_list, index=peak_point_time_list)
    return s

# print(get_kline_peak_points(data_source))



def get_kline_bottom_points(data_source, fit_data_type='low'):
    '''

    :param data_source: 数据源
    :param fit_data_type: 默认按照最低价'low'来判断最低点
    :return:返回该时间段内的价格底点
    取5个点的数据，判断中间点的数值是否是最小值，如是则判定为底点

    '''
    points_num = 5  # 默认在5个点中取顶点
    rolling_data_min = data_source[fit_data_type].rolling(points_num, center=True).min()
    bottom_point_time_list = []
    bottom_point_value_list = []
    last_min_point_pos = 0
    for i in range(int(points_num/2), len(data_source)-int(points_num/2)):
        middle_point_time = data_source.index[i]
        middle_point_value = float(data_source.iloc[i][fit_data_type])  #注意数据类型转换
        if (rolling_data_min[i] == middle_point_value) and ((i-last_min_point_pos)>=points_num):  # 判断是否顶点
            last_min_point_pos = i
            bottom_point_time_list.append(middle_point_time)
            bottom_point_value_list.append(middle_point_value)
    s = pd.Series(bottom_point_value_list, index=bottom_point_time_list)
    return s

# print(get_kline_bottom_points(data_source))

def get_macd_peak_points(data_source, fit_data_type='macd'):
    '''

    :param period:时间周期
    :param fit_data_type:顶点取数指标类型
    :return:
    '''
    macd_data = data_source[fit_data_type]
    points_num = 5  # 默认在5个点中取顶点
    rolling_data_max = data_source[fit_data_type].rolling(points_num, center=True).max()
    # if len(macd_data) < points_num:
    #     raise ValueError  # 点数不够，无法判断顶点
    peak_point_time_list = []
    peak_point_value_list = []
    for i in range(int(points_num/2), len(macd_data.index)-int(points_num/2)):
        middle_point = macd_data.index[i]
        middle_point_value = macd_data.loc[macd_data.index[i]]
        if middle_point_value == rolling_data_max.loc[middle_point]:  # 判断是否顶点
            peak_point_time_list.append(middle_point)
            peak_point_value_list.append(middle_point_value)
    s = pd.Series(peak_point_value_list, index=peak_point_time_list)
    return s

# print(get_macd_peak_points(data_source, fit_data_type='macd'))

def get_kline_trend(data_source, start_time, end_time, stock_id, period='day', fit_data_type='close'):
    '''

    :param start_time:时间段内的开始时间
    :param end_time:时间段内的结束时间
    :param stock_id:股票代码
    :param period:交易周期，日、60分钟、30分钟、15分钟
    :param fit_data_type:按照哪个价格进行趋势线拟合
    :return:
    传入开始时间和结束时间，交易周期（日、60分钟、30分钟），拟合的价格类
    获取时间段内的最高价/最低价/收盘价
    将时间坐标进行整型数转换，起点时间为0
    采用普通最小二乘法进行线性回归  Ordinary Least Squares，得到斜率和截距系数
    再将开始时间和结束时间代入线性回归模型，得到趋势线起点与终点预测值

    '''

    df = data_source
    df = df.drop(df[df.index < start_time].index)
    df = df.drop(df[df.index > end_time].index)
    time_map = pd.Series(df.index)  # 构造一个交易时间与X轴坐标的映射 ,取整型数
    x_data = time_map.index.values
    y_data = df[fit_data_type].values
    # print(x_data, y_data)
    X = x_data.reshape(-1, 1)  # 将x数据转化为n samples，1 feature 的数据，numpy array or sparse matrix of shape [n_samples,n_features]
    Y = y_data.reshape(-1, 1)  # 将y数据转化为n samles，1 target 的数据，numpy array of shape [n_samples, n_targets]
    reg = linear_model.LinearRegression()
    reg.fit(X, Y)
    # LinearRegression(copy_X=True, fit_intercept=True, n_jobs=1, normalize=False)
    y_start = reg.predict(x_data[0])
    y_end = reg.predict(x_data[-1])
    trend_points = pd.Series({time_map.iloc[0]: y_start[0][0], time_map.iloc[-1]: y_end[0][0]})
    # print(trend_points )
    return trend_points,reg.coef_[0][0]

# print(get_kline_trend(start_time,end_time,stock_id))

def get_kline_peak_points_trend(peak_points):
    '''

    :param peak_points:
    :return:
    返回kline顶点的趋势线
    '''
    peak_points = peak_points.sort_index(ascending=True)
    time_map = pd.Series(peak_points.index , index=get_ax_position(peak_points.index))
    x_data = time_map.index.values
    y_data = peak_points.values
    X = x_data.reshape(-1, 1)  # 将x数据转化为n samples，1 feature 的数据，numpy array or sparse matrix of shape [n_samples,n_features]
    Y = y_data.reshape(-1, 1)  # 将y数据转化为n samles，1 target 的数据，numpy array of shape [n_samples, n_targets]
    print(X,Y)
    reg = linear_model.LinearRegression()
    reg.fit(X, Y)
    y_start = reg.predict(x_data[0])
    y_end = reg.predict(x_data[-1])
    trend_points = pd.Series({time_map.iloc[0]: y_start[0][0], time_map.iloc[-1]: y_end[0][0]})
    # print(trend_points )
    return trend_points,reg.coef_[0][0]

# print(get_kline_peak_points_trend(get_kline_peak_points(data_source)[-3:-1]))


def get_macd_trend(data_source,start_time, end_time, fit_data_type='macd'):
    '''
    :param data_source:macd指标计算结果的数据源。
    :param start_time: macd趋势线绘制的时间坐标轴上开始时间
    :param end_time: macd趋势线绘制的时间坐标轴上结束时间
    :param stock_id: 所绘趋势线的股票代码
    :param period: 时间周期，日、60分钟、30分钟、15分钟
    :param macd_trend_type: 使用哪个指标值来拟合出趋势线，dif：'macd'，dea:'macds'，bar:'macdh'
    :return:
    '''
    #将数据源上的时间对齐开始时间和结束时间
    macd_trend_df = data_source.drop(data_source[data_source.index < start_time].index)
    macd_trend_df = macd_trend_df.drop(macd_trend_df[macd_trend_df.index > end_time].index)
    time_map = pd.Series(macd_trend_df.index)   # 构造一个交易时间与X轴坐标的映射 ,取整型数
    x_data = time_map.index.values
    y_data = macd_trend_df[fit_data_type].values
    X = x_data.reshape(-1,1)
    Y = y_data.reshape(-1,1)
    reg = linear_model.LinearRegression()
    reg.fit(X, Y)
    y_start = reg.predict([0])
    y_end = reg.predict(x_data[-1])
    trend_points = pd.Series({time_map.iloc[0]: y_start[0][0], time_map.iloc[-1]: y_end[0][0]})
    return trend_points, reg.coef_[0][0]

# print(get_macd_trend(data_source,start_time1, end_time1))

def get_kline_figure_trend_line(data_source, figure_tpye='1'):
    '''
    :param figure_start_time: 所绘图形上的坐标轴开始时间
    :param figure_end_time: 所绘图形上的坐标轴结束时间（最新时间）
    :param stock_id: 股票代码
    :param period: 时间周期
    :param figure_tpye:顶背离图形 ：1 ；底背离图形 ：0
    :return: [trend_line_start_time,trend_line_end_time] 所绘趋势线的开始时间和结束时间
    返回k线图上作趋势线段的两端顶点。
    顶背离图形采用趋势线斜率单调递减原则(底背离算法类似)。具体算法如下：
    *顶点按照最近时间到最早时间排序
    *开始时间设定为最近一个顶点。
    *计算趋势线的斜率k
    *如果趋势线斜率小于0，则固定结束时间，并将结束时间设定为最近的一个顶点，开始时间设定为上一个顶点，重新计算斜率
    *While k>0:
        增加前一个顶点。即开始时间设定为前一个顶点的时间。
        计算趋势线斜率k1
        If k1> k,则跳出while loop
        否则k=k1，继续循环
    *输出趋势线两端点和斜率

    '''
    if figure_tpye == '1':
        peak_points = get_kline_peak_points(data_source)
        peak_points.sort_index(ascending=False, inplace=True)
        peak_points_count = 0
        trend_line_start_time = peak_points.index[peak_points_count]
        trend_line_end_time = data_source.index[-1]
        trend_points, k = get_kline_trend(data_source, trend_line_start_time, trend_line_end_time, stock_id)
        if k<=0 :
            trend_line_end_time = peak_points.index[peak_points_count]
            trend_line_start_time = peak_points.index[peak_points_count+1]
            trend_points, k = get_kline_trend(data_source, trend_line_start_time, trend_line_end_time, stock_id)
        while (k>=0) and (peak_points_count<len(peak_points)-1):
            peak_points_count += 1
            trend_line_start_time = peak_points.index[peak_points_count]
            trend_points1, k1 = get_kline_trend(data_source, trend_line_start_time, trend_line_end_time, stock_id)
            # print('trend_points1 {}, k1: {}'.format(trend_points1, k1))
            if k1 > k:
                break
            elif k1>=0:
                trend_points, k = trend_points1,k1
            else:
                break
        # print(k)
        # print(trend_points)
        return  trend_points,k
    elif figure_tpye=='0':
        return
    else:
        return

# print(get_kline_figure_trend_line(data_source,figure_tpye='1'))



def get_macd_figure_trend_line(data_source, kline_trend_start_time, kline_trend_end_time, figure_tpye='1'):
    '''
    :param data_source: macd指标数据源
    :param kline_trend_start_time:  k线趋势线对应的开始时间
    :param kline_trend_end_time: k线趋势线对应的结束时间
    :param figure_tpye: 顶背离图形 ：1 ；底背离图形 ：0
    :return: [trend_line_start_time,trend_line_end_time] ，k 所绘趋势线的开始时间和结束时间以及斜率
    在k线趋势线的时间段内寻找macd值的顶点，然后作趋势线段。
    '''
    macd_trend_df = data_source
    peak_points = get_macd_peak_points(data_source)
    peak_points.sort_values(ascending=False, inplace=True)
    #截取时间段内数据
    macd_trend_df = macd_trend_df.drop(macd_trend_df[macd_trend_df.index < kline_trend_start_time].index)
    macd_trend_df = macd_trend_df.drop(macd_trend_df[macd_trend_df.index > kline_trend_end_time].index)
    peak_points.drop(peak_points[peak_points.index < kline_trend_start_time].index, inplace=True)
    peak_points.drop(peak_points[peak_points.index > kline_trend_end_time].index, inplace=True)
    if figure_tpye=='1':
        # print(peak_points)
        if len(peak_points)==0:  #没有顶点的处理
            peak_point_start_time = kline_trend_start_time
        else:
            peak_point_start_time = peak_points.index[0]
        trend_points,k = get_macd_trend(macd_trend_df,peak_point_start_time,kline_trend_end_time)
        # print(trend_points)
        return trend_points,k
    elif figure_tpye=='0':
        return
    else:
        return

# print(get_macd_figure_trend_line(data_source, start_time1, end_time1, figure_tpye='1'))

def format_date(x, pos=None):   #日期与整形数映射
    thisind = np.clip(int(x+0.5), 0, len(data_source)-1)
    return data_source.index[thisind].strftime('%Y-%m-%d')




# print(get_ax_position(get_macd_peak_points(data_source).index))

def draw_kline_macd_trend_line(data_source):
    '''
    绘图步骤：
    1、绘制K线图
    2、绘制k线趋势线
    3、绘制macd指标图
    4、绘制macd指标变化趋势线
    '''
    #先用整数作为下标，然后利用matplotlib.ticker.FuncFormatter改变x轴刻度的格式
    N = len(data_source)
    ind = np.arange(N)
    data_source['time_pos'] = ind
    # print(data_source)
    qutotes = []
    for index,(d,o,c,h,l) in enumerate(
        zip(data_source.time_pos, data_source.open, data_source.close, data_source.high, data_source.low )
    ):
        #蜡烛图的日期使用date2num转化为数值
        # d = mpf.date2num(d)
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
    #绘制k线图，使用candlestick_ochl函数绘图，ochl代表open，close，high，low
    mpf.candlestick_ochl(ax1, qutotes, width=0.6, colorup='red', colordown='green', alpha=0.75)
    ax1.autoscale_view()
    # ax1.xaxis_date()
    #绘制macd图
    # kl_index = data_source.index
    kl_index = data_source.time_pos
    dif = data_source['macd']
    dea = data_source['macds']
    bar = data_source['macdh']
    ax2.plot(kl_index, dif, label='macd dif')
    ax2.plot(kl_index, dea, label='dea')
    bar_red = np.where(bar > 0, bar, 0)
    bar_green = np.where(bar < 0, bar, 0)
    # 绘制bar>0的柱状图
    ax2.bar(kl_index, bar_red, facecolor='red', label='hist bar')
    # 绘制bar<0的柱状图
    ax2.bar(kl_index, bar_green, facecolor='green', label='hist bar')
    ax2.legend(loc='upper left')
    # 绘制k线趋势线
    kline_trend_points,_ = get_kline_figure_trend_line(data_source)
    parallel_distance_kline = float(data_source['close'].loc[kline_trend_points.index.values[0]]) - kline_trend_points[0] #计算线段平移距离
    ax1.plot(get_ax_position(kline_trend_points.index), kline_trend_points.values+parallel_distance_kline, color='blue', linewidth=4)
    #绘制macd趋势线
    macd_trend_points,_ = get_macd_trend(data_source, kline_trend_points.index[0],kline_trend_points.index[1])
    parallel_distance_macd = data_source['macd'].loc[macd_trend_points.index.values[0]] - macd_trend_points[0]
    ax2.plot(get_ax_position(macd_trend_points.index), macd_trend_points.values+parallel_distance_macd, color='blue', linewidth=4)
    # #输出图表
    # fig_output_path = r'C:\smartkline\output\macd\\' + stock_id + '_'+str(start_time) + '_'+str(end_time) + '.png'
    # plt.savefig(fig_output_path)
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    fig.autofmt_xdate()
    plt.show()
    return

#测试数据
draw_kline_macd_trend_line(data_source)
#批量生成
# query1 = "SELECT DISTINCT lpad(code,6,'0') FROM stock_market_hist_kline_nofuquan WHERE date BETWEEN %s and %s"
# cursor.execute(query1,(start_time, end_time))
# data = cursor.fetchall()
# # s1 = pd.Series(data)
# print(data)
# for i in range(3024, len(data)):
#     draw_kline_macd_trend_line(start_time,end_time, data[i][0])
