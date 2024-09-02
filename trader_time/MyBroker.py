# coding:utf-8
import copy
import math
import time, datetime, traceback, sys

import numpy as np

from echart_utils import do_plot
from helper.data_helper import *

import pandas

import backtrader as bt


class MyOrder():
    def __init__(self, code, price, is_buy):
        self.code = code
        self.is_buy = is_buy
        self.price = price

        self.real_price = price
        self.size = 0


class MyOwnBroker():
    def __init__(self):
        self.money = 1000000
        self.min_size = 100
        self.position = {
            # 'code': {
            #     'hold_size': 200,
            #     'buy_date': '20170101',
            # }
        }
        self.history = {
            # 'code': [
            # {
            #     'buy_date': '20170101',
            #     'buy_not_sell': True,
            #     'change_size': 200,
            # }
            # ]
        }

        self.money_history = {
            'x': [],
            'y': []
        }

    def order_stock(self, code, to_buy_price, close_price_per, next_open_price_per, last_trade_date):
        tmp = None
        if code in self.position:
            tmp = self.position[code]

        if to_buy_price < 1E-6:
            # todo sell
            if tmp is None:
                return
            self.money = self.money + next_open_price_per * tmp['hold_size']
            return

        buy_size = math.floor(to_buy_price / close_price_per) // self.min_size * self.min_size
        if tmp is None:
            tmp = self.position[code] = {
                'hold_size': buy_size,
                'buy_date': last_trade_date,
            }
        if buy_size == tmp['hold_size']:
            return
        elif buy_size > tmp['hold_size']:
            # todo buy
            self.money = self.money - (buy_size - tmp['hold_size']) * next_open_price_per
        else:
            # todo sell
            self.money = self.money + (tmp['hold_size'] - buy_size) * next_open_price_per
        return

    def next(self, my_orders_dict, trade_date, stock_data=None):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        new_postion_code_set = set([code for code, _ in my_orders_dict.items()])
        to_sells = ori_postion_code_set - new_postion_code_set

        to_buy_price = 0
        if len(my_orders_dict) > 0:
            to_buy_price = self.money * 0.95 // len(my_orders_dict)
        for to_sell_code in to_sells:
            self.order_stock(to_sell_code, 0.0, 0, 0, trade_date)
        for to_buy_code in new_postion_code_set:
            self.order_stock(to_buy_code, to_buy_price, my_orders_dict[to_buy_code].close_qfq,
                             my_orders_dict[to_buy_code].next_open, trade_date)

        self.money_history['x'].append(trade_date)
        self.money_history['y'].append(self.money)

    def checkout(self):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        for to_sell_code in ori_postion_code_set:
            self.order_stock(to_sell_code, 0.0, 0, 0, '')
        pass

    def plot_money(self, shangzheng):
        do_plot(data=self.money_history, shangzheng=shangzheng)


# coding:utf-8
import math
import time, datetime, traceback, sys

import numpy as np

from echart_utils import do_plot
from helper.data_helper import *

import pandas

import backtrader as bt


class MyOrder():
    def __init__(self, code, price, is_buy):
        self.code = code
        self.is_buy = is_buy
        self.price = price

        self.real_price = price
        self.size = 0


class MyOwnBroker():
    def __init__(self, date_dict, date_stock_dict, shangzheng):
        self.shangzheng = shangzheng
        self.date_stock_dict = date_stock_dict
        self.money = 1000000
        self.min_size = 100
        self.hold = {
            # 'code': {
            #     'buy_size': 100,
            #     'buy_date': '20170101',
            #     'buy_price_each': 1234,
            #     'buy_price_all': 123400,
            # }
        }
        self.orders = []
        self.history = {
            # 'code': [
            # {
            # 'buy_not_sell': True,
            #     'buy_size': 100,
            #     'buy_date': '20170101',
            #     'buy_price_each': 1234,
            #     'buy_price_all': 123400,
            #
            # }
            # ]
        }

        self.money_history = {
            'x': [],
            'y': []
        }

        self.date_dict = date_dict
        self.trade_date = date_dict.keys()

        self.trade_date = sorted(self.trade_date)
        self.date_index = 0

    def cur_date(self):
        return self.trade_date[self.date_index]

    def get_cur_signal(self, df):
        if df.shape[0] == 0:
            return {}, {}
        buy_signal = {}
        sell_signal = {}
        max_count = 30
        for index, value in df.iterrows():
            if value['buy_signal']:
                buy_signal[value['ts_code']] = value
            elif value['sell_signal']:
                sell_signal[value['ts_code']] = value
            max_count = max_count - 1
            if max_count <= 0:
                break
        return buy_signal, sell_signal

    def next(self):
        now = time.time()
        cur_date = self.trade_date[self.date_index]
        cur_df = self.date_dict[cur_date]
        cur_df = cur_df[cur_df['buy_signal'] | cur_df['sell_signal']]
        # print('next1 ', time.time() - now)
        now = time.time()

        stocks_dict_2_buy, stocks_dict_2_sell = self.get_cur_signal(cur_df)
        hold_dict = self.hold

        # print('next2 ', time.time() - now)
        now = time.time()

        # buy_fix_price = 100
        buy_fix_price = self.money
        # 持仓遇到卖信号就卖
        hold_dict_copy = copy.copy(hold_dict)
        for code, value in hold_dict_copy.items():
            to_sell_code = code
            if to_sell_code in stocks_dict_2_sell:
                row = stocks_dict_2_sell[to_sell_code]
                self.order_stock(to_sell_code, cur_date, 0.0, row, self.hold)
        # 持仓遇到买信号就买
        for code, value in stocks_dict_2_buy.items():
            to_buy_code = code
            if to_buy_code not in self.hold:
                row = stocks_dict_2_buy[to_buy_code]
                self.order_stock(to_buy_code, cur_date, buy_fix_price, row, self.hold)

        self.save_money_history(cur_date)
        # print('next3 ', time.time() - now)
        now = time.time()

        self.date_index = self.date_index + 1
        return self.date_index < len(self.trade_date)

    def save_money_history(self, cur_date):
        hold_money = 0
        date_stock_dict = self.date_stock_dict
        for key, value in self.hold.items():
            hold_money = hold_money + value['buy_size'] * date_stock_dict[key + '_' + str(cur_date)]

        self.money_history['x'].append(cur_date)
        self.money_history['y'].append(self.money + hold_money)
    def order_stock(self, code, trade_date, buy_price, cur_row, hold_info):
        # 预处理
        is_sell_cmd = True
        buy_size = 0
        open_price_each = 0
        if cur_row is not None:
            buy_size = buy_price / cur_row['open_qfq']
            open_price_each = cur_row['open_qfq']

        hold_size = 0
        if code in hold_info:
            hold_size = hold_info[code]['buy_size']
        if buy_size == hold_size:
            return
        is_sell_cmd = buy_size < hold_size
        # 买卖计算、io调用
        if is_sell_cmd:
            self.money = self.money + open_price_each * hold_size
            del self.hold[code]
            self.orders.append(((code, trade_date), (is_sell_cmd, cur_row)))
            print('trade_date %s, self.money %.2f, sell %s all %d '
                  f'size %.2f each %.2f' % (trade_date, self.money, code,
                                            open_price_each * hold_size, buy_size, open_price_each))

        #     todo 调接口实现订单
        else:
            buy_price_all = open_price_each * buy_size
            if self.money < buy_price_all:
                print('self.money < buy_price_all')
                return
            self.money = self.money - buy_price_all
            self.hold[code] = {
                'buy_size': buy_size,
                'buy_date': str(trade_date),
                'buy_price_each': open_price_each,
                'buy_price_all': buy_price_all,
            }
            self.orders.append(((code, trade_date), (is_sell_cmd, cur_row)))
            print('trade_date %s, self.money %.2f, buy %s all %d '
                  f'size %.2f each %.2f' % (trade_date, self.money, code,
                                            buy_price_all, buy_size, open_price_each))
        #     todo 调接口实现订单

    def checkout(self):
        hold_dict_copy = copy.copy(self.hold)
        for code, value in hold_dict_copy.items():
            self.order_stock(code, 'last', 0.0, None, hold_dict_copy)
        print("final money ", self.money)

    def plot_money(self, df):
        refer = self.shangzheng
        if df is not  None:
            refer = df
        do_plot(profit=self.money_history, base=refer)
