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
        bottom_count = 20
        bottom = np.partition(copy.copy(close[i-OFFSET:i+1]), bottom_count)[bottom_count]

        if state == 0:

            match_over_ema5 = ((close[i] > ema5[i]) and
                         (close[i - 1] > ema5[i - 1]) and
                         (close[i - 2] > ema5[i - 2]))
            # match_over_ema5 = True
            match_1 = close[i] < bottom

            if match_over_ema5 and match_1 and close[i] > ema60[i]:
                state = 1
                buy_price = close[i]
        elif state == 1:
            # match_below_ema5 = ((close[i] < ema5[i]) and
            #                    (close[i - 1] < ema5[i - 1]) and
            #                    (close[i - 2] < ema5[i - 2]))
            match_below_ema5 = False
            profit_over = close[i] > buy_price * 1.25
            profit_blow = close[i] < buy_price * 0.75
            if match_below_ema5 or profit_over or profit_blow:
                total_money = total_money / buy_price * close[i]
                state = 0

    if state == 1:
        total_money = total_money / buy_price * close[len(close)-1]
    return total_money
