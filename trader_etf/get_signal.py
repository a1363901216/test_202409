
# coding:utf-8
import concurrent
import copy
import math
import os

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


# import talib


class MyStrategy():
    def __init__(self):
        # 加载数据
        self.ori_data = self.load_data()

        # 补充指标
        self.ori_data = self.fill_ex(self.ori_data)

        # 按时间加索引
        self.set_index()

        self.trade_dates = self.get_trade_dates(self.ori_data)

        self.date_index = -1

    def get_to_buy_list(code_2_detail):
        pass

    def get_to_sell_list(code_2_detail):
        pass

# def buy_signal(code_2_detail):
#     for code, df in code_2_detail.items():
#
#
#     return index
#
# def sell_signal(all, date, hold_stock_codes):
#     # all_date_df = pd.DataFrame(columns=all.columns)
#     list = []
#     for hold_stock_code in hold_stock_codes:
#         list.append(all.loc[(date, hold_stock_code)].to_frame().T)
#         # df = all[index]
#         # index[0] = False
#     all_cur_date_df = pd.concat(list)
#
#     buy_signal_index = all['close_qfq'] < 1.01 * all['ema_qfq_5']
#     # buy_signal =
#     return all_cur_date_df
#
# if __name__ == '__main__':
#     pass
