#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import HuobiServices as hb
import pymysql, time
import conf_main, conf_local


class HuoBi(object):
    db = None
    cursor = None

    use_conf = None

    def __init__(self):
        self.use_conf = dict(conf_main.config)
        self.use_conf.update(conf_local.config)
        self.db = pymysql.connect(host="47.75.110.210", user="root", password=self.use_conf['db_password'], db="huobi", port=3306,
                                  charset="utf8")
        self.cursor = self.db.cursor()

    def test(self):
        data = hb.get_symbols()
        print(data)

    def get_history_trade(self):
        params = {
            'symbol': 'btcusdt',
            'size': 10,
        }
        data = hb.get_history_trade(params)
        if data is not None:
            for item in data['data']:
                item = item['data'][0]

                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(round(int(item['ts']) / 1000)))
                now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                use_sql = '''insert into hb_base_detail (symbol, amount, price, direction, ts, created_at, updated_at) 
values ('%s', %f, %f, '%s', '%s', '%s', '%s') ''' % ('btcusdt', item['amount'], item['price'],
                                                     item['direction'], ts_time, now_time, now_time)
                self.cursor.execute(use_sql)
                self.db.commit()

        return 'ok'

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
    Hb = HuoBi()
    # HuoBi().test()

    # data = Hb.get_history_trade()
    # print(data)
    Hb.get_kline()
    # Hb.get_trade()
