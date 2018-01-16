# coding=utf-8

import tushare as ts
from sqlalchemy import create_engine

'''
获取个股的基本相关信息
'''


def get_stock_basicsinfo():
    engine = create_engine('mysql://{param}/smartk_demo?charset=utf8', pool_size=1)
    df = ts.get_stock_basics()
    df.to_sql('stock_market_basics_info', engine, if_exists='append')


get_stock_basicsinfo()
