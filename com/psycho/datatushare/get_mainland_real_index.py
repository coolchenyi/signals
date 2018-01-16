# coding=utf-8

from sqlalchemy import create_engine
import tushare as ts
import datetime
import time
import logging

logging.basicConfig()

engine = create_engine('mysql://{param}/smartk_demo?charset=utf8', pool_size=20, echo=True)

s_time_am = datetime.datetime.strptime('09:30:00', '%H:%M:%S')
e_time_am = datetime.datetime.strptime('11:29:59', '%H:%M:%S')
s_time_pm = datetime.datetime.strptime('13:00:00', '%H:%M:%S')
e_time_pm = datetime.datetime.strptime('14:59:59', '%H:%M:%S')

'''
获取大盘指数实时行情列表，以表格的形式展示大盘指数实时行情
'''


def get_mainland_index():
    df = ts.get_index()
    if len(df) == 0:
        return
    now = datetime.datetime.now()
    df['date'] = now.date()
    df['time'] = now.time()
    df.to_sql('stock_mainland_index', engine, if_exists='append', chunksize=1000, index=False)


# get_mainland_index()


def my_job():
    init = True
    while (True):
        now = datetime.datetime.now().time()
        format_time = datetime.datetime.strptime(now.strftime("%H:%M:%S"), '%H:%M:%S')
        if init == True:
            init = False
        else:
            time.sleep(15)
        if s_time_am.__le__(format_time) and e_time_am.__ge__(format_time):
            print 'executing now at am'
            get_mainland_index()
        elif s_time_pm.__le__(format_time) and e_time_pm.__ge__(format_time):
            print 'executing now at pm'
            get_mainland_index()
        elif e_time_am.__lt__(format_time) and s_time_pm.__gt__(
                format_time):
            print "stock market closed"
            init = True
            time.sleep(5)
        else:
            print 'my job is finished.'
            break


from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == '__main__':
    sched = BlockingScheduler()
    # sched.add_job(my_job, 'interval', seconds=5)
    sched.add_job(my_job, 'cron', day_of_week='mon-fri', hour=13,
                  minute=59)
    print "scheduler kline is start !"
    sched.start()
