#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import HuobiServices as hb
import pymysql

class HuoBi(object):

    def test(self):
        data = hb.get_symbols()
        print(data)

    def get_history_trade(self):
        params = {
            'symbol': 'btcusdt',
            'size': 10,
        }
        data = hb.get_history_trade(params)
        print(type(data))
        return data


if __name__ == '__main__':
    Hb = HuoBi()
    # HuoBi().test()

    data = Hb.get_history_trade()
    print(data)
