# coding=utf-8


import tushare as ts
from sqlalchemy import create_engine

engine = create_engine('mysql://{param}/smartk_demo?charset=utf8', pool_size=1)


def _get_stock_his_ticks(codeid=None, trade_day=None):
    df = ts.get_tick_data(codeid, trade_day, retry_count=10, pause=3)
    if len(df) == 0:
        print "get history ticks of %s is NULL" % (codeid)
        return False
    if df.iloc[0, 0].find('当天没有数据') != -1:
        print "get history ticks of %s is NULL" % (codeid)
        return False
    df['code'] = codeid
    df['date'] = trade_day
    df.to_sql('stock_his_ticks', engine, if_exists='append', index=False)
    print 'save history ticks of %s successfully.' % (codeid)
    return True


def get_stocksHticks(trade_day=None):
    result = engine.execute("select code from stock_industry_classified")
    ccode = result.fetchall()
    print "codelist_len=" + str(len(ccode))
    result.close()
    t_list = []
    for code_row in ccode:
        code_string = ("%06d" % code_row['code'])
        if code_string not in t_list:
            t_list.append(code_string)

    if len(t_list) == 0:
        return
    i = 0
    records = 0
    for current_code in t_list:
        i = i + 1
        print i
        # if i < 1483:
        #     continue
        if _get_stock_his_ticks(current_code, trade_day):
            records += 1
    print 'records=%d' % (records)


get_stocksHticks('2017-07-11')
