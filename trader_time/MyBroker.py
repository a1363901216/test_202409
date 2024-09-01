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

    def next(self, my_orders_dict, trade_date, stock_data = None):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        new_postion_code_set = set([code for code, _ in my_orders_dict.items()])
        to_sells = ori_postion_code_set - new_postion_code_set

        to_buy_price = 0
        if len(my_orders_dict) >0:
            to_buy_price = self.money * 0.95 // len(my_orders_dict)
        for to_sell_code in to_sells:
            self.order_stock(to_sell_code, 0.0, 0, 0,trade_date)
        for to_buy_code in new_postion_code_set:
            self.order_stock(to_buy_code, to_buy_price, my_orders_dict[to_buy_code].close_qfq,
                             my_orders_dict[to_buy_code].next_open, trade_date)

        self.money_history['x'].append(trade_date)
        self.money_history['y'].append(self.money)

    def checkout(self):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        for to_sell_code in ori_postion_code_set:
            self.order_stock(to_sell_code, 0.0, 0, 0,'')
        pass


    def plot_money(self, shangzheng):
        do_plot(data = self.money_history, shangzheng= shangzheng)
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

    def next(self, my_orders_dict, trade_date, stock_data = None):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        new_postion_code_set = set([code for code, _ in my_orders_dict.items()])
        to_sells = ori_postion_code_set - new_postion_code_set

        to_buy_price = 0
        if len(my_orders_dict) >0:
            to_buy_price = self.money * 0.95 // len(my_orders_dict)
        for to_sell_code in to_sells:
            self.order_stock(to_sell_code, 0.0, 0, 0,trade_date)
        for to_buy_code in new_postion_code_set:
            self.order_stock(to_buy_code, to_buy_price, my_orders_dict[to_buy_code].close_qfq,
                             my_orders_dict[to_buy_code].next_open, trade_date)

        self.money_history['x'].append(trade_date)
        self.money_history['y'].append(self.money)

    def checkout(self):
        ori_postion_code_set = set([code for code, _ in self.position.items()])
        for to_sell_code in ori_postion_code_set:
            self.order_stock(to_sell_code, 0.0, 0, 0,'')
        pass

    def plot_money(self, shangzheng):
        do_plot(data = self.money_history, shangzheng= shangzheng)
