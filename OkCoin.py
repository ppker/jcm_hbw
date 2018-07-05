#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import OkServices as ok
import pymysql, time
import conf_main, conf_local, os, sys


class OkCoin(object):
    db = None
    cursor = None
    action = None

    use_conf = None

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

    def get_ticker(self):
        params = {
            'symbol': 'btc_usd'
        }
        data = ok.get_ticker(params)
        print(data)

    def get_trades(self):
        params = {
            'symbol': 'btc_usd',
        }
        data = ok.get_trades(params)
        if data is not None:
            for item in data:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(item['date'])))
                # 进行去重
                if True or self.to_strip({'buy_code': item['tid'], 'action': 'get_trades'}):
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into ok_base_detail (symbol, amount, price, direction, ts_id, buy_code, ts, 
created_at, updated_at) values ('%s', %f, %f, '%s', %d, '%s', '%s', '%s', '%s') ''' % ('btc_usd', item['amount'],
                                                                                       item['price'], item['type'],
                                                                                       item['date_ms'],
                                                                                       int(item['tid']), ts_time,
                                                                                       now_time, now_time)
                    try:
                        self.cursor.execute(use_sql)
                    except IOError:
                        print('this sql has some error ' + item['tid'])
                    else:
                        pass
            self.db.commit()
        return 'ok'

    def get_kline(self):
        params = {
            'symbol': 'btc_usd',
            'type': '1min',
            'size': 200,
            # 'since': 111111111111
        }
        data = ok.get_kline(params)
        if data is not None:
            for item in data:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item[0] / 1000))))
                # 进行去重
                if self.to_strip({'ts': ts_time, 'action': 'get_kline'}):
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into ok_data_kline (symbol, period, amount, open, close, low, high, ts, 
created_at, updated_at) values ('%s', '%s', %f, %f, %f, %f, %f, '%s', '%s', '%s') ''' % (
                    'btc_usd', '1min', float(item[5]),
                    float(item[1]), float(item[4]), float(item[3]), float(item[2]), ts_time, now_time, now_time)
                    try:
                        self.cursor.execute(use_sql)
                    except IOError:
                        print("this sql has some error " + use_sql)
                    else:
                        pass
            self.db.commit()
        return 'ok'

    def to_strip(self, params={}):
        if params == {}:
            print("need some params")
            return False
        if 'get_trades' == params['action']:
            sql = '''select id from ok_base_detail where buy_code = %s limit 1 ''' % (params['buy_code'],)

        elif 'get_kline' == params['action']:
            sql = '''select id from ok_data_kline where ts = "%s" limit 1 ''' % (params['ts'],)
        else:
            sql = ''

        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        if data is None:
            return True
        else:
            return False


if __name__ == '__main__':
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if len(sys.argv) < 2:
        print("your command is error")
    else:
        OK = OkCoin()
        if 'get_ticker' == OK.action:
            OK.get_ticker()
            print("get_ticker is running [" + time_now + ']')
        elif 'get_trades' == OK.action:
            OK.get_trades()
            print("get_trades is running [" + time_now + ']')
        elif 'get_kline' == OK.action:
            OK.get_kline()
            print("get_kline is running [" + time_now + ']')
