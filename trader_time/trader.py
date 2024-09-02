# coding:utf-8
import concurrent
import copy
import math
import os
import time

import pandas as pd

from MyBroker import MyOwnBroker
# from backtrader_bokeh import bt

from helper.download_data import read_file

from MyStrategy import *
import datetime
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from joblib import Parallel, delayed
import numba

import get_ori_data, get_ma_signal

def convert_to_date_dict(stock_dict):
    now = time.time()
    stock_list = []
    for _, value in stock_dict.items():
        stock_list.append(value)
    df = pd.concat(stock_list)
    df['trade_date'] = df['trade_date'].astype(int)
    df = df.set_index(['trade_date'], drop=True)
    df = df.sort_index()
    print('convert_dict_to_time_index1 cost', time.time() - now)
    now = time.time()
    key_2_groups = df.groupby("trade_date")
    date_dict = {}
    date_stock_dict = {}
    for key, groups in key_2_groups:
        date_dict[key] = groups
        for row in groups.itertuples():
            date_stock_dict[row.ts_code + '_' + str(key)] = row.close_qfq
    print('convert_dict_to_time_index cost', time.time() - now)
    return date_dict, date_stock_dict

if __name__ == '__main__':
    # os.environ['NUMBA_NUM_THREADS'] = '16'
    # 获取原始数据
    print("start ...")
    ori_stock_dict, shangzheng = get_ori_data.load(isTest=True, reload_from_clickhouse=False)

    #获取每个股票的买点、卖点
    stock_dict_with_signal = get_ma_signal.get_signal(ori_stock_dict)

    #以时间维度，计算每天收益
    date_dict,date_stock_dict = convert_to_date_dict(stock_dict_with_signal)
    myOwnBroker = MyOwnBroker(date_dict, date_stock_dict, shangzheng)
    #打印股票收益曲线
    while myOwnBroker.next():
        # print(myOwnBroker.cur_date())
        pass
    myOwnBroker.checkout()
    myOwnBroker.plot_money(ori_stock_dict['000001.SZ'])
    print("end ...")
    # sell_signal = get_signal.sell_signal(all=stock_all, date='20170103', hold_stock_codes=['000008.SZ', '000001.SZ'])

