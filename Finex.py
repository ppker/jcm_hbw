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

    def get_cache(self):
        cache_sql = '''select buy_code from finex_base_detail order by id desc limit 1200 '''
        self.cursor.execute(cache_sql)
        data = self.cursor.fetchall()
        if data is not None:
            cache_data = [x[0] for x in data]
            return cache_data
        return []

    def get_trades(self):
        params = {
            'limit': '1000',
            'sort': '-1',
        }
        data = finex.get_trades(params)
        if data is not None:
            # 加载缓存
            self.cache_data = self.get_cache()
            for item in data:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item[1] / 1000))))
                # 进行去重
                if str(item[0]) in self.cache_data:
                    continue
                else:
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into finex_base_detail (symbol, amount, price, ts_id, buy_code, ts, created_at, updated_at) 
values ('%s', %f, %f, %d, '%s', '%s', '%s', '%s')''' % (
                    'btc_usd', item[2], item[3], item[1], item[0], ts_time, now_time, now_time)
                    try:
                        self.cursor.execute(use_sql)
                    except Exception:
                        print('this sql has some error ' + item['tid'])
                    else:
                        pass
            self.db.commit()
            self.cache_data = []
        return 'ok'

    def to_strip(self, params):
        if params == {}:
            print("need some params")
            return False
        if 'get_kline' == params['action']:
            sql = '''select id from finex_data_kline where ts = "%s" limit 1 ''' % (params['ts'],)
        elif 'get_trades' == params['action']:
            pass

        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        if data is None:
            return True
        else:
            return False

    def get_kline(self):
        params = {
            'limit': '160',
            'sort': '-1',
        }
        data = finex.get_kline(params)
        if data is not None:
            for item in data:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item[0] / 1000))))
                # 进行去重
                if self.to_strip({'ts': ts_time, 'action': 'get_kline'}):
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into finex_data_kline (symbol, period, volume, open, close, low, high, 
ts, created_at, updated_at) values ('%s', '%s', %f, %f, %f, %f, %f, '%s', '%s', '%s') ''' % (
                        'btc_usd', '1min', float(item[5]), float(item[1]), float(item[2]), float(item[4]),
                        float(item[3]), ts_time, now_time, now_time)
                    try:
                        self.cursor.execute(use_sql)
                    except Exception:
                        print("this sql has some error " + use_sql)
                    else:
                        pass
            self.db.commit()
        return 'ok'


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
