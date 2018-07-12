#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from binance.client import Client
import ssl, time, sys
import conf_main, conf_local, pymysql

# 屏蔽掉https警告
ssl._create_default_https_context = ssl._create_unverified_context


class Bian(object):
    client = None
    action = None
    db = None
    cursor = None
    action = None

    use_conf = None
    cache_data = None
    proxies = {
        'http': 'http://127.0.0.1:1087',
        'https': 'http://127.0.0.1:1087',
    }

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

        self.client = Client('PBM4vgJTZ0aWKKaIeB720VqUm9TQDzcdW3yxnpdd0Urj9niLjT1D1YGpU65RUDaa',
                             'aMDicDTNI7J0Qhzh2RDmNj2jc7Y2fjsTvLM8Np5Cft0uAeqa4ZVHbg8JG8UFJctK',
                             {
                                 # "proxies": self.proxies, 
                                 "verify": True,
                                 "timeout": 20
                             })

    def test(self):
        # data = self.client.get_recent_trades(symbol='BNBBTC')
        # data = self.client.get_historical_klines("BNBBTC", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC")
        data = self.client.get_klines(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_1MINUTE)
        print(data)

    def get_cache(self):
        cache_sql = '''select buy_code from bian_base_detail order by id desc limit 800 '''
        self.cursor.execute(cache_sql)
        data = self.cursor.fetchall()
        if data is not None:
            cache_data = [x[0] for x in data]
            return cache_data
        return []

    def get_trades(self):
        data = self.client.get_recent_trades(symbol='BTCUSDT')
        if data is not None:
            # 加载缓存
            self.cache_data = self.get_cache()
            for item in data:
                ts_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item['time'] / 1000))))
                # 进行去重
                if str(item['id']) in self.cache_data:
                    continue
                else:
                    now_time = time.strftime("%Y-%m-%d %H-%M-%S", time.localtime(time.time()))
                    if True == item['isBuyerMaker']:
                        item['isBuyerMaker'] = 1
                    elif False == item['isBuyerMaker']:
                        item['isBuyerMaker'] = 0

                    if True == item['isBestMatch']:
                        item['isBestMatch'] = 1
                    elif False == item['isBestMatch']:
                        item['isBestMatch'] = 0

                    use_sql = '''insert into bian_base_detail (symbol, qty, price, ts_id, isBuyerMaker, isBestMatch, 
buy_code, ts, created_at, updated_at) values ('%s', %f, %f, %d, %d, %d, '%s', '%s', '%s', '%s') ''' % (
                    'btc_usdt', float(item['qty']),
                    float(item['price']), int(item['time']), int(item['isBuyerMaker']), int(item['isBestMatch']),
                    item['id'], ts_time, now_time, now_time)

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
            sql = '''select id from bian_data_kline where opentime = '%s' and closetime = '%s' limit 1 ''' % (
            params['opentime'], params['closetime'])
        elif 'get_trades' == params['action']:
            sql = ''
        else:
            sql = ''

        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        if data is None:
            return True
        else:
            return False

    def get_kline(self):
        data = self.client.get_klines(symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_1MINUTE)
        if data is not None:
            for item in data:
                open_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item[0] / 1000))))
                close_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(round(item[6] / 1000))))
                # 进行去重
                if self.to_strip({'opentime': open_time, 'closetime': close_time, 'action': 'get_kline'}):
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    use_sql = '''insert into bian_data_kline (symbol, period, opentime, open, high, low, close, volumn, closetime, 
quote_asset_volume, number_of_trades, buy_base_asset_volume, buy_quote_asset_volume, `ignore`, created_at, updated_at) values ('%s', '%s', '%s', 
%f, %f, %f, %f, %f, '%s', %f, %d, %f, %f, %f, '%s', '%s') ''' % (
                    'btc_usdt', '1min', open_time, float(item[1]), float(item[2]), float(item[3]),
                    float(item[4]), float(item[5]), close_time, float(item[7]), int(item[8]), float(item[9]),
                    float(item[10]), float(item[11]), now_time, now_time)

                    try:
                        self.cursor.execute(use_sql)
                    except Exception:
                        print("this sql has some error " + use_sql)
                    else:
                        pass

            self.db.commit()

        return 'ok'


if __name__ == '__main__':

    # data = Bian().client.get_exchange_info()
    # data = Bian().test()
    # print(data)

    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if len(sys.argv) < 2:
        print("your command is error")
    else:
        Bi = Bian()
        if 'get_ticker' == Bi.action:
            Bi.get_ticker()
            print("bian get_ticker is running [" + time_now + ']')
        elif 'get_trades' == Bi.action:
            Bi.get_trades()
            print("bian get_trades is running [" + time_now + ']')
        elif 'get_kline' == Bi.action:
            Bi.get_kline()
            print("bian get_kline is running [" + time_now + ']')
