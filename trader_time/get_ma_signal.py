# coding:utf-8
import concurrent
import copy
import math
import os

import pandas as pd
from numba import njit, prange

from MyBroker import MyOwnBroker
# from backtrader_bokeh import bt

from helper.download_data import read_file

from MyStrategy import *
import datetime
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from joblib import Parallel, delayed
import numba


# import talib

def get_signal(stock_dict):
    now = time.time()
    for code, value in stock_dict.items():
        value['buy_signal'], value['sell_signal'] = do_get_signal(value[['close_qfq', 'ema_qfq_5']].to_numpy())

    print("get_signal cost", (time.time() - now))
    return stock_dict

def do_get_signal(array):
    close = array[:, 0]
    close_1 = np.roll(close, 1)
    close_2 = np.roll(close, 2)
    close_3 = np.roll(close, 3)

    ema5 = array[:, 1]
    ema5_1 = np.roll(ema5, 1)
    ema5_2 = np.roll(ema5, 2)
    ema5_3 = np.roll(ema5, 3)

    buy_signal = (close_1[:] > ema5_1[:]) & \
          (close_2[:] > ema5_2[:]) & \
          (close_3[:] > ema5_3[:])
    buy_signal[0:3] = False

    sell_signal = (close_1[:] < ema5_1[:]) & \
          (close_2[:] < ema5_2[:]) & \
          (close_3[:] < ema5_3[:])
    sell_signal[0:3] = False

    return buy_signal, sell_signal

def buy_signal(stock_dict):
    now = time.time()
    for code, value in stock_dict.items():
        value['three_day_above_ma5'] = do_buy_signal(value[['close_qfq', 'ema_qfq_5']].to_numpy())

    print("buy_signal cost", (time.time() - now))
    return stock_dict

def sell_signal(stock_dict):
    now = time.time()
    for code, value in stock_dict.items():
        value['three_day_small_ma5'] = do_buy_signal(value[['close_qfq', 'ema_qfq_5']].to_numpy())

    print("sell_signal cost", (time.time() - now))
    return stock_dict

def do_sell_signal(array):
    close = array[:, 0]
    close_1 = np.roll(close, 1)
    close_2 = np.roll(close, 2)
    close_3 = np.roll(close, 3)

    ema5 = array[:, 1]
    ema5_1 = np.roll(ema5, 1)
    ema5_2 = np.roll(ema5, 2)
    ema5_3 = np.roll(ema5, 3)

    ret = (close_1[:] < ema5_1[:]) & \
          (close_2[:] < ema5_2[:]) & \
          (close_3[:] < ema5_3[:])
    ret[0:3] = False
    return ret



def do_buy_signal(array):
    close = array[:, 0]
    close_1 = np.roll(close, 1)
    close_2 = np.roll(close, 2)
    close_3 = np.roll(close, 3)

    ema5 = array[:, 1]
    ema5_1 = np.roll(ema5, 1)
    ema5_2 = np.roll(ema5, 2)
    ema5_3 = np.roll(ema5, 3)

    ret = (close_1[:] > ema5_1[:]) & \
          (close_2[:] > ema5_2[:]) & \
          (close_3[:] > ema5_3[:])
    ret[0:3] = False
    return ret


