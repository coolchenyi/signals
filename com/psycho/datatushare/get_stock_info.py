# coding=utf-8

'''
获取各种股票行业分类信息
'''

import tushare as ts
from sqlalchemy import create_engine

engine = create_engine('mysql://{param}/smartk_demo?charset=utf8', pool_size=1)


# 按行业分类区分
def get_stock_byindustry_classified():
    df = ts.get_industry_classified()
    df.to_sql('stock_industry_classified', engine, if_exists='append')


# 按概念分类区分
def get_stock_byconcept_classified():
    df = ts.get_concept_classified()
    df.to_sql('stock_concept_classified', engine, if_exists='append')


# 获取ST股票
def get_st_classified():
    df = ts.get_st_classified()
    df.to_sql('stock_st_classified', engine, if_exists='append')


# test success!
get_stock_byindustry_classified()
get_stock_byconcept_classified()
# get_st_classified()
