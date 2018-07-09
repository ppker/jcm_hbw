#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import FinexServices as finex
import pymysql, time
import conf_main, conf_local, os, sys


class Finex(object):
    db = None
    cursor = None
    action = None

    use_conf = None
    cache_data = None

    def __init__(self):
        if len(sys.argv) < 2:
            print("your command is error")
            return

        self.action = sys.argv[1]
        self.use_conf = dict(conf_main.config)
        self.use_conf.update(conf_local.config)
        self.db = pymysql.connect(host="47.75.110.210", user="root", password=self.use_conf['db_password'], db="huobi",
                                  port=3306,
                                  charset="utf8")
        self.cursor = self.db.cursor()

    def get_trades(self):
        params = {
            'limit': '120',
            'sort': '-1',
        }
        data = finex.get_trades(params)
        print(data)

    def get_kline(self):
        params = {
            'limit': '160',
            'sort': '-1',
        }
        data = finex.get_kline(params)
        print(data)


if __name__ == '__main__':
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if len(sys.argv) < 2:
        print("your command is error")
    else:
        Fin = Finex()
        if 'get_trades' == Fin.action:
            Fin.get_trades()
            print("get_trades is running [" + time_now + ']')
        elif 'get_kline' == Fin.action:
            Fin.get_kline()
            print("get_kline is running [" + time_now + ']')
