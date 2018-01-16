# coding=utf-8

'''
处理复权数据（分为历史复权、）
'''
from sqlalchemy import create_engine
import tushare as ts
import pandas as pd
import datetime
from sqlalchemy.types import String, Date, DECIMAL

'''
qstart 起始时间
qend 截止时间
type 取3个值：qfq 表示前复权 hfq表示后复权  None 表示不复权
codeid  股票ID
tn  插入表名
index 表示是否是大盘指数
'''


def get_hist_kline_fuquan_2(qstart=None, qend=None, type=None, codeid=None, tn=None, index=False):
    # if qstart is None or qend is None:
    #     return
    if tn is None:
        tn = "stock_market_hist_kline_nofuquan"
    ccode = codeid
    print "codeID=" + str(ccode)
    engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=20, echo=True)
    if codeid is None:
        # select a.code,b.timeToMarket from stock_industry_classified a left join stock_market_basics_info b on a.code=b.code;
        result = engine.execute(
            "select a.code,b.timeToMarket from stock_industry_classified a left join stock_market_basics_info b on a.code=b.code")
        ccode = result.fetchall()
        print "codelist_len=" + str(len(ccode))
        result.close()
        t_map = {}
        for code_row in ccode:
            orig_d = code_row['timeToMarket']
            t_map[("%06d" % code_row['code'])] = datetime.datetime.strptime(str(orig_d), '%Y%m%d').strftime(
                "%Y-%m-%d") if orig_d is not None else '1999-11-30'

    if isinstance(ccode, list):
        if len(ccode) == 0:
            return
        i = 0
        # frames = []
        for current_i in t_map:
            i = i + 1
            print i
            print current_i + ":" + t_map[current_i]
            begin = datetime.datetime.now()
            df = ts.get_h_data(current_i, start=t_map[current_i], end=qend, autype=type, retry_count=10, pause=3,
                               index=index)
            end = datetime.datetime.now()
            print "执行时间＝" + str((end - begin).seconds)
            if df is None or len(df) == 0:
                print current_i + " result is NULL"
                continue
            df['code'] = current_i
            # frames.append(df)
            try:
                df.to_sql(tn, engine, if_exists='append')
                print current_i + " is OK!!!!"
            except Exception as e:
                print "Exce=" + e.message
                # a_result = pd.concat(frames)
                # 2701
                # 603898
                # a_result.to_sql(tn, engine, if_exists='append', chunksize=1000)
    else:
        df = ts.get_h_data(ccode, start=qstart, end=qend, autype=type, retry_count=4, index=index)
        df['code'] = ccode
        df.to_sql(tn, engine, if_exists='append', chunksize=1000)
        print ccode + " is OK!!!!!"


def get_hist_kline_fuquan(qstart=None, qend=None, type=None, codeid=None, tn=None, index=False):
    # if qstart is None or qend is None:
    #     return
    if tn is None:
        tn = "stock_market_hist_kline_nofuquan"
    ccode = codeid
    print "codeID=" + str(ccode)
    engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=1, echo=True)
    if codeid is None:
        # select code from stock_industry_classified a where a.index>2700
        # select code from stock_industry_classified
        # select a.code,b.timeToMarket from stock_industry_classified a left join stock_market_basics_info b on a.code=b.code;
        # select distinct(code) from stock_market_hist_kline_nofuquan where date BETWEEN '2010-01-01' and '2013-12-31'
        # select code from stock_market_basics_info where timeToMarket < 20100101
        result = engine.execute("select DISTINCT(code) from stock_market_basics_info")
        ccode = result.fetchall()
        print "codelist_len=" + str(len(ccode))
        result.close()
        t_list = []
        for code_row in ccode:
            code_string = ("%06d" % code_row['code'])
            if code_string not in t_list:
                t_list.append(code_string)
                # result = engine.execute(
                #     "select distinct(code) from stock_market_hist_kline_nofuquan where date BETWEEN '2000-01-01' and '2009-12-31'")
                # ccode = result.fetchall()
                # print "codelist_len2=" + str(len(ccode))
                # result.close()
                # t_list2 = []
                # for code_row in ccode:
                #     t_list2.append(("%06d" % code_row['code']))
                #
                # t_list3 = []
                # for code_row in t_list:
                #     if code_row not in t_list2:
                #         t_list3.append(code_row)
                # print "codelist_len3=" + str(len(t_list3))
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
            df = ts.get_h_data(current_code, start=qstart, end=qend, autype=type, retry_count=10,
                               index=index, pause=6)
            end = datetime.datetime.now()
            print "执行时间＝" + str((end - begin).seconds)
            if df is None or len(df) == 0:
                print current_code + " result is NULL"
                continue
            df['code'] = current_code
            frames.append(df)
            records += 1
            print current_code + " is OK!!!!"
        if len(frames) == 0:
            return
        try:
            df = pd.concat(frames)
            engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=1,
                                   echo=True)
            df.to_sql(tn, engine, if_exists='append', chunksize=1000)
            print 'records=%d' % (records)
            print 'save hist_kline fuquan is OK!!!'
        except Exception as e:
            print "Exce=" + e.message
            # a_result = pd.concat(frames)
            # 2701
            # 603898
            # a_result.to_sql(tn, engine, if_exists='append', chunksize=1000)
    else:
        df = ts.get_h_data(ccode, start=qstart, end=qend, autype=type, retry_count=4, index=index)
        df['code'] = ccode
        df.to_sql(tn, engine, if_exists='append', chunksize=1000)
        print ccode + " is OK!!!!!"


# get_hist_kline_fuquan(codeid='600328', qstart='2016-01-01', qend='2017-01-13')
# 403    002644
# 002316
# get_hist_kline_fuquan(qstart='1992-01-01', qend='1995-12-31')
get_hist_kline_fuquan(qstart='2018-01-08', qend='2018-01-12')
# 600714,002617,000587 单独取一次 4月6号
# get_hist_kline_fuquan(codeid='600714', qstart='2014-01-01', qend='2017-03-22')
# get_hist_kline_fuquan(codeid='002617', qstart='2014-01-01', qend='2017-03-22')
# get_hist_kline_fuquan(codeid='000587', qstart='2014-01-01', qend='2017-03-22')
# 000587 需要重新入一次数据2010-01-01到2013-12-31
# get_hist_kline_fuquan(codeid='000587', qstart='2010-01-01', qend='2013-12-31')
