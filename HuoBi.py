#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import HuobiServices as hb
import pymysql, time
import conf_main, conf_local, os, sys


class HuoBi(object):
    db = None
    cursor = None

    use_conf = None
    cache_data = None

    def __init__(self):
        self.use_conf = dict(conf_main.config)
        self.use_conf.update(conf_local.config)
        self.db = pymysql.connect(host="47.75.110.210", user="root", password=self.use_conf['db_password'], db="huobi",
                                  port=3306,
                                  charset="utf8")
        self.cursor = self.db.cursor()

    def test(self):
        data = hb.get_symbols()
        print(data)

    def get_history_trade(self):
        params = {
            'symbol': 'btcusdt',
            'size': 1000,
        }
        data = hb.get_history_trade(params)

        if data is not None:
            # 加载缓存
            self.cache_data = self.get_cache()

            for item in data['data']:
                item = item['data'][0]

                # 去重过滤
                # if self.trade_strip(item['id']):
                if str(item['id']) in self.cache_data:
                    continue
                else:
                    ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(str(item['ts'])[:-3]))) + '.' + str(
                        item['ts'])[-3:]
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into hb_base_detail (symbol, amount, price, direction, buy_code, ts_id, ts, created_at, updated_at) 
    values ('%s', %f, %f, '%s', '%s' , %d , '%s', '%s', '%s') ''' % ('btcusdt', item['amount'], item['price'],
                                                                     item['direction'], int(item['id']),
                                                                     int(item['ts']), ts_time, now_time, now_time)
                    try:
                        self.cursor.execute(use_sql)
                    except Exception:
                        print('this sql has some error ' + str(item['id']))
                        print(self.cache_data)

            self.db.commit()

            self.cache_data = None
        return 'ok'

    def get_cache(self):
        cache_sql = '''select buy_code from hb_base_detail order by id desc limit 1500 '''
        self.cursor.execute(cache_sql)
        data = self.cursor.fetchall()
        if data is not None:
            cache_data = [x[0] for x in data]
            return cache_data
        return []


    def trade_strip(self, buy_code):

        for code in self.cache_data:
            if buy_code == code:
                return False

        return True
        sql = '''select id from hb_base_detail where buy_code = %s limit 1 ''' % (buy_code,)
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        # print('this is strip')
        if data is None:
            return True
        else:
            return False

    def get_kline(self):
        data = hb.get_kline('btcusdt', '1min', 200)

        if data is not None:
            for item in data['data']:

                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(item['id'])))
                # 进行去重
                if self.to_strip(ts_time, '1min'):
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into hb_data_kline (symbol, period, amount, count, open, close, low, 
    high, vol, ts, created_at, updated_at) values ('%s', '%s', %f, %f, %f, %f, %f, %f, %f, '%s', '%s', '%s') ''' % (
                        'btcusdt', '1min', item['amount'], item['count'], item['open'], item['close'],
                        item['low'], item['high'], item['vol'], ts_time, now_time, now_time)

                    self.cursor.execute(use_sql)

            self.db.commit()
        return 'ok'

    def to_strip(self, p_ts, p_period):
        sql = '''select id from hb_data_kline where ts = '%s' and period = '%s' limit 1''' % (p_ts, p_period)
        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        # print('this is strip')
        if data is None:
            return True
        else:
            return False

    def get_trade(self):
        data = hb.get_trade('btcusdt')
        print(data)


if __name__ == '__main__':

    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if len(sys.argv) < 2:
        print("your command is error")
    else:
        Hb = HuoBi()
        if ('get_history_trade' == sys.argv[1]):
            Hb.get_history_trade()
            print("Huobi get_history_trade is running [" + time_now + ']')
        elif ('get_kline' == sys.argv[1]):
            Hb.get_kline()
            print("Huobi get_history_trade is running [" + time_now + ']')
        else:
            print('Huobi you command had some errors ' + '-'.join(sys.argv))
