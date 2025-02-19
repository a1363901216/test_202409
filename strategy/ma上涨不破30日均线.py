# coding:utf-8
import concurrent
import copy
import math
import os

import numpy as np
import talib

from helper.tongdaxin.funcat import *
from helper.tongdaxin.funcat.funcat import MA, CROSS


# 最近3天都在5日线上，5日线最近10天超过至少两条其他均线
def do_get_signal(base, ext):
    OFFSET = 280
    close = base['close'].to_numpy()
    C = close
    if len(close) < OFFSET:
        return 1

    state_enum = ['can_buy', 'has_buy']
    state = 0
    total_money = 1
    buy_price = 0
    sma5 = talib.SMA(C, timeperiod=5)
    sma30 = talib.SMA(C, timeperiod=30)
    # sma5 = ext['sma5'].to_numpy()
    # sma10 = ext['sma10'].to_numpy()
    # sma20 = ext['sma20'].to_numpy()
    # sma60 = ext['sma60'].to_numpy()
    # sma120 = ext['sma120'].to_numpy()
    # sma250 = ext['sma250'].to_numpy()
    # pct_chg = base['pct_chg'].to_numpy()

    # 过去20天ma5持续上涨超30%
    # 5日线不破30日线
    offset = 20
    for i in range(OFFSET, len(close)):
        find_buy_signal = False
        find_sell_signal = False

        if state == 0:
            if (sma5[i] - sma5[i - offset]) / sma5[i - offset] > 0.3:
                find_buy_signal = True
                for j in range(i - offset, i + 1, 1):
                    if sma5[j] < sma30[j]:
                        find_buy_signal = False
            if find_buy_signal:
                state = 1
                buy_price = close[i]
        elif state == 1:
            find_sell_signal = sma5[i] < sma30[i]
            if find_sell_signal:
                total_money = total_money / buy_price * close[i]
                state = 0

    if state == 1:
        total_money = total_money / buy_price * close[len(close) - 1]
    return total_money
