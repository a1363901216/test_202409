# coding:utf-8
import concurrent
import copy
import math
import os

import numpy as np
import pandas as pd
from numba import njit, prange

from MyBroker import MyOwnBroker
# from backtrader_bokeh import bt

from helper.download_data import read_file

# from MyStrategy import *
import datetime
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
# from joblib import Parallel, delayed
import numba


# import talib


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

    OFFSET = 145
    for i in range(OFFSET, len(close)):
        backCount = 0
        count = 0
        find_buy_signal = False
        find_sell_signal = False
        for j in range(i - 20, i + 1):
            maxV = max(ema5[j], ema10[j], ema30[j], ema60[j])
            minV = min(ema5[j], ema10[j], ema30[j], ema60[j])
            diff = abs(maxV - minV)/ema5[j]
            if diff < 0.1:
                count = count + 1
            backCount = backCount +1
        if count / backCount > 0.9 and  ema5[i] > ema60[i] and  ema5[i] > ema10[i]:
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
