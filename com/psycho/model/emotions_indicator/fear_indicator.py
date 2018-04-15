#coding=utf-8
import numpy as np
from pylab import *
import pandas as pd
import mysql.connector
from mysql.connector import *
from _datetime import date

cursor=cnx.cursor()


def cal_stocks_fearIndicator(date):
    query = (
    "SELECT lpad(code,6,'0') as code,date,price_change,volume FROM smartk_demo.stock_market_hist_kline_day WHERE date=%(date)s and price_change<0")
    cursor.execute(query, {'date': date})
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['code', 'date', 'price_change', 'volume'])
    query1 = ("SELECT lpad(code,6,'0') as code,metaStimulus FROM inter_stocks_metastimulus WHERE date='2017-7-31'")
    cursor.execute(query1)
    data1 = cursor.fetchall()
    df1 = pd.DataFrame(data1, columns=['code', 'metaStimulus'])
    df = pd.merge(df, df1, how='left', on='code')
    #     print(df)
    # 这里的volume单位是手
    df['stimulus'] = abs(df['price_change']*df['volume']*100)
    df['stimulus'] = df['stimulus'].astype('float64')
    df['confidenceIndicator'] = log(df['stimulus']/df['metaStimulus'])
    print(df)


cal_stocks_fearIndicator('2018-04-09')
