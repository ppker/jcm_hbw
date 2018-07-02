#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rest_hb import HuobiServices as hb


class HuoBi(object):

    def test(self):
        data = hb.get_symbols()
        print(data)


if __name__ == '__main__':
    Hb = HuoBi()
    # HuoBi().test()
    params = {
        'symbol': 'btcusdt',
        'size': 10,
    }
    data = Hb.get_history_trade(params)
    print(data)