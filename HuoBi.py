#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import HuobiServices as hb
import pymysql, time

class HuoBi(object):

    db = None
    cursor = None
    def __init__(self):
        self.db = pymysql.connect(host="140.143.32.44", user="root", password="yata123", db="yata_data_01", port=3306, charset="utf8")
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
        if not None:
            for item in data['data']:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(item['ts'] / 1000))
                now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                use_sql = '''insert into hb_base_detail (symbol, amount, price, direction, ts, created_at, updated_at) 
values ('%s', %f, %f, '%s', '%s', '%s', '%s') ''' % ('btcusdt', item['amount'], item['price'],
                                                     item['direction'], ts_time, now_time)
                self.cursor.execute(use_sql)
                self.db.commit()

        return 'ok'


if __name__ == '__main__':
    Hb = HuoBi()
    # HuoBi().test()

    data = Hb.get_history_trade()
    print(data)
