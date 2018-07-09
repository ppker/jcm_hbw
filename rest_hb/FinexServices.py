#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from .Utils import *
import ssl

# 屏蔽掉https警告
ssl._create_default_https_context = ssl._create_unverified_context


# 获取finex交易信息
def get_trades(params={}):
    url = FINEX_URL + '/trades/tBTCUSD/hist'
    return http_get_request(url, params)


# 获取finex的K线数据
def get_kline(params={}):
    url = FINEX_URL + '/candles/trade:1m:tBTCUSD/hist'
    return http_get_request(url, params)