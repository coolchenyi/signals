# coding=utf-8

'''
处理历史日K线数据(按日、周、月、5分钟、15分钟、30分钟、60分钟)
'''
from sqlalchemy import create_engine
import tushare as ts
import pandas as pd
import datetime
from sqlalchemy.types import String, Date
import logging

logging.basicConfig()


def get_stockhist_byall(qstart=None, qend=None, kt='D', codeid=None, tn=None):
    # if qstart is None or qend is None:
    #    return
    if tn is None:
        tn = "stock_market_hist_kline_day"
    ccode = codeid
    print "codeID=" + str(ccode)
    engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=20, echo=True)
    if codeid is None:
        # select code from stock_industry_classified a where a.index>2658
        # select code from stock_industry_classified
        # select code from `stock_industry_classified`  where code not in(select distinct(code) as a1 from `stock_market_hist_kline_day` a) ;
        # SELECT DISTINCT(code) FROM stock_market_basics_info
        result = engine.execute("select DISTINCT(code) from stock_market_basics_info")
        ccode = result.fetchall()
        print "codelist_len=" + str(len(ccode))
        result.close()
        t_list = []
        for code_row in ccode:
            code_string = ("%06d" % code_row['code'])
            if code_string not in t_list:
                t_list.append(code_string)
    records = 0
    if isinstance(ccode, list):
        if len(ccode) == 0:
            return
        i = 0
        # frames = []
        for current_code in t_list:
            i = i + 1
            print i
            print current_code
            begin = datetime.datetime.now()
            df = ts.get_hist_data(current_code, start=qstart, end=qend, ktype=kt, retry_count=10)
            end = datetime.datetime.now()
            print "执行时间＝" + str((end - begin).seconds)
            if df is None:
                print current_code + " result is None"
                continue
            if len(df) == 0:
                print current_code + " result is zero"
                continue
            df['code'] = current_code
            # frames.append(df)
            # if len(frames) == 0:
            #     return
            try:
                # df = pd.concat(frames)
                df.to_sql(tn, engine, if_exists='append')
                # dtype={'date': Date, 'open': DECIMAL(14, 3), 'high': DECIMAL(14, 3), 'close': DECIMAL(14, 3),
                #        'low': DECIMAL(14, 3), 'volume': DECIMAL(14, 3), 'price_change': DECIMAL(14, 3),
                #        'p_change': DECIMAL(14, 3), 'ma5': DECIMAL(14, 3), 'ma10': DECIMAL(14, 3),
                #        'ma20': DECIMAL(14, 3), 'v_ma5': DECIMAL(14, 3), 'v_ma10': DECIMAL(14, 3),
                #        'v_ma20': DECIMAL(14, 3), 'turnover': DECIMAL(5, 3), 'code': String})
                # print 'records=%d' % (records)
                records += 1
                print current_code + " is OK!!!!"
            except Exception as e:
                print "Exce=" + e.message
                # a_result = pd.concat(frames)
                # a_result.to_sql(tn, engine, if_exists='append', chunksize=1000)
    else:
        df = ts.get_hist_data(ccode, start=qstart, end=qend, ktype=kt, retry_count=4)
        df['code'] = ccode
        df.to_sql(tn, engine, if_exists='append', chunksize=1000)
        print ccode + " is OK!!!!!"

    print 'records=' + str(records)


# 获取全部股票的时间范围
def my_job():
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    # now = '2017-12-25'
    get_stockhist_byall(qstart=now, qend=now)
    # get_stockhist_byall(qstart='2017-07-05', qend='2017-07-05')


from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == '__main__':
    sched = BlockingScheduler()
    # sched.add_job(my_job, 'interval', seconds=5)
    sched.add_job(my_job, 'cron', day_of_week='mon-fri', hour=15,
                  minute=20)
    print "scheduler kline is start !"
    sched.start()
    # get_stockhist_byall(qstart='2017-07-10', qend='2017-07-10')
# 获取单股的时间范围
# get_stockhist_byall(qstart='2014-01-01', qend='2016-10-13',codeid='000587')
# get_stockhist_byall(qstart='2014-01-01', qend='2016-10-13',codeid='002617')
# get_stockhist_byall(qstart='2014-01-01', qend='2016-10-13',codeid='600714')

# 获取某一日的数据
# get_stockhist_byall(qstart='2016-10-17', qend='2016-10-17', tn="stock_market_hist_kline_20161017")
# get_stockhist_byall(qstart='2017-01-14', qend='2017-01-18', tn="stock_market_hist_kline_day")
# get_stockhist_byall(qend='2014-12-31', tn="stock_market_hist_kline_day")
# get_stockhist_byall(tn="stock_market_hist_kline_day")

'''
engine = create_engine('mysql://root:stock_160909@123.56.218.96/stock_model?charset=utf8', pool_size=20)
df = ts.get_hist_data('600654', start='2014-01-01', end='2016-09-25', ktype='D', retry_count=4, pause=2)
df['code'] = '600654'
df.to_sql('stock_market_hist_kline_day', engine, if_exists='append', chunksize=1000)
'''
