# coding=utf-8
'''
处理日K线数据(按日、周、月、5分钟、15分钟、30分钟、60分钟) 使用最新接口
'''
from sqlalchemy import create_engine
import tushare as ts
import pandas as pd
import datetime
from sqlalchemy.types import String, Date
import logging
from com.psycho.datatushare import get_db_engine

'''
全新接口取数
qstart 开始时间
qend  结束时间
kt 数据类型默认是D，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
codeid 股票ID、指数ID及ETF基金
tn 表示表名，可选
autype 表示复权类型  qfq-前复权 hfq-后复权 None-不复权，默认为qfq
'''


def get_stock_kline_databytype(qstart=None, qend=None, kt='D', codeid=None, tn=None, autype=None, index=False):
    ccode = codeid
    # 拼接表名
    table_name_prefix = 'stock_mk_kline_'
    if kt == 'D':
        tn = table_name_prefix + "day"
    elif kt == 'W':
        tn = table_name_prefix + "week"
    elif kt == 'M':
        tn = table_name_prefix + "month"
    elif kt == '5':
        tn = table_name_prefix + "five"
    elif kt == '15':
        tn = table_name_prefix + "fifteen"
    elif kt == '30':
        tn = table_name_prefix + "thirty"
    elif kt == '60':
        tn = table_name_prefix + "sixty"
    db_engine = get_db_engine()
    if codeid is None:
        result = db_engine.execute("select DISTINCT(code) from stock_market_basics_info")
        ccode = result.fetchall()
        print "codelist_len=" + str(len(ccode))
        result.close()
        t_list = []
        for code_row in ccode:
            code_string = ("%06d" % code_row['code'])
            if code_string not in t_list:
                t_list.append(code_string)
    if isinstance(ccode, list):
        if len(ccode) == 0:
            return
        i = 0
        records = 0
        frames = []
        for current_code in t_list:
            i = i + 1
            print i
            print current_code
            begin = datetime.datetime.now()
            df = ts.get_k_data(current_code, start=qstart, end=qend, autype=autype, retry_count=10,
                               index=index, pause=6)
            end = datetime.datetime.now()
            print "执行时间＝" + str((end - begin).seconds)
            if df is None or len(df) == 0:
                print current_code + " result is NULL"
                continue
            frames.append(df)
            records += 1
            print current_code + " is OK!!!!"
        if len(frames) == 0:
            return
        try:
            df = pd.concat(frames)
            df.to_sql(tn, db_engine, if_exists='append', chunksize=1000, index=False)
            print 'records=%d' % (records)
            print 'save ' + tn + ' is OK!!!'
        except Exception as e:
            print "Exce=" + e.message
    else:
        df = ts.get_k_data(ccode, start=qstart, end=qend, ktype=kt, autype=autype, retry_count=4, index=index)
        df.to_sql(tn, db_engine, if_exists='append', chunksize=1000, index=False)
        print ccode + " is OK!!!!!"
