# coding:utf-8
import concurrent
import copy
import math
import os

import numpy as np
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
    sma5 = ext['sma5'].to_numpy()
    sma10 = ext['sma10'].to_numpy()
    sma20 = ext['sma20'].to_numpy()
    sma60 = ext['sma60'].to_numpy()
    sma120 = ext['sma120'].to_numpy()
    sma250 = ext['sma250'].to_numpy()
    pct_chg = base['pct_chg'].to_numpy()

    # cross_signal = CROSS(sma5, sma10)

    for i in range(OFFSET, len(close)):
        find_buy_signal = False
        find_sell_signal = False
        find_buy_signal = sma5[i]>sma10[i] and sma5[i-1]<=sma10[i-1]

        if state == 0:
            if find_buy_signal:
                state = 1
                buy_price = close[i]
        elif state == 1:
            profit_over = close[i] > buy_price * 1.25
            profit_blow = close[i] < buy_price * 0.75
            if find_sell_signal or profit_over or profit_blow:
                total_money = total_money / buy_price * close[i]
                state = 0

    if state == 1:
        total_money = total_money / buy_price * close[len(close) - 1]
    return total_money
