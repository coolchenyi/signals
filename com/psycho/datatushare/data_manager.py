# coding=utf-8

from sqlalchemy import create_engine
import tushare as ts
import datetime
import time
import sqlalchemy as sa

dele_sql = sa.text(
    "delete from inter_stocks_price_anchoring_bias where code=:code and date=:date")
engine = create_engine('mysql://root:smartk123@120.26.72.215/smartk_demo?charset=utf8', pool_size=20, echo=True)

result = engine.execute("select code,date from tmp_copy")
rows = result.fetchall()
print "codelist_len=" + str(len(rows))
for row in rows:
    codeid, date = ("%06d" % row['code']), row['date']
    flag = engine.execute(dele_sql, code=codeid, date=date)
    print flag
