#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from .Utils import *
import ssl

# 屏蔽掉https警告
ssl._create_default_https_context = ssl._create_unverified_context


# 获取OKCoin行情
def get_ticker(params={}):
    url = OK_REST_URL + '/api/v1/ticker.do'
    return http_get_request(url, params)


# 获取OKCoin交易信息
def get_trades(params={}):
    url = OK_REST_URL + '/api/v1/trades.do'
    return http_get_request(url, params)


# 获取OKCoin的K线数据
def get_kline(params={}):
    url = OK_REST_URL + '/api/v1/kline.do'
    return http_get_request(url, params)