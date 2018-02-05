# coding=utf-8

from sqlalchemy import create_engine


def __init():
    global _all_engine
    _all_engine = create_engine('mysql://{params}@120.26.72.215/smartk_demo?charset=utf8',
                                pool_size=20,
                                echo=True)


__init()


def get_db_engine():
    return _all_engine
