# coding:utf-8
import concurrent
import copy
import math
import os

import numpy as np


# 最近3天都在5日线上，5日线最近10天超过至少两条其他均线
def do_get_signal(nums_df, open, close, emaList):
    if len(close) < 10:
        return 1

    state_enum = ['can_buy', 'has_buy']
    state = 0
    total_money = 1
    buy_price = 0
    ema5 = emaList[0]
    ema10 = emaList[1]
    ema20 = emaList[2]
    ema30 = emaList[3]
    ema60 = emaList[4]
    ema250 = emaList[5]
    rate = (close - open) / open

    OFFSET = 145
    for i in range(OFFSET, len(close)):
        backCount = 0
        count = 0
        find_buy_signal = False
        find_sell_signal = False
        back = 22
        tmp = rate[i + 1 - back:i + 1]
        ratePos = np.sum(tmp[tmp > 0])
        rateNag = np.sum(tmp[tmp < 0])
        if np.sum(tmp) > 0.30 and rateNag > -0.15:
            find_buy_signal = True

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
