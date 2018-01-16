# coding=utf-8
'''
一次性获取当前交易所有股票的行情数据
（如果是节假日，即为上一交易日，结果显示速度取决于网速）
'''
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts


def get_real_kline_all(tn=None):
    if tn is None:
        tn = "stock_market_kline_today"
    engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=20, echo=True)
    df = ts.get_today_all()
    if df is None or len(df) == 0:
        print  "real_kline result is NULL"
        return
    df.to_sql(tn, engine, if_exists='append', chunksize=1000, index=False)


get_real_kline_all()
