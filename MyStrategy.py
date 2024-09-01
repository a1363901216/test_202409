# coding:utf-8
import time, datetime, traceback, sys

import numpy as np

from helper.data_helper import *

import pandas

import backtrader as bt
class MyBroker(bt.BackBroker):
    def __init__(self):
        super(MyBroker, self).__init__()

    def next(self):
        super(MyBroker, self).next()

    def get_notification(self):
        return super(MyBroker, self).get_notification()

    def buy(self, strategy, data, **kwargs):
        ## 此处对接券商交易系统
        return super(MyBroker, self).buy(strategy, data, **kwargs)

    def sell(self, strategy, data, **kwargs):
        ## 此处对接券商交易系统
        return super(MyBroker, self).sell(strategy, data, **kwargs)


# 回测策略
class StockSelectStrategy(bt.Strategy):
    '''多因子选股 - 基于调仓表'''

    def __init__(self, buy_list):
        # self.buy_stock = pd.read_csv("./Backtrader/data/trade_info.csv", parse_dates=['trade_date'])
        # 读取调仓日期，即每月的最后一个交易日，回测时，会在这一天下单，然后在下一个交易日，以开盘价买入
        # self.trade_dates = pd.to_datetime(self.buy_stock['trade_date'].unique()).tolist()
        self.order_list = []  # 记录以往订单，方便调仓日对未完成订单做处理
        self.buy_stocks_pre = []  # 记录上一期持仓
        self.index = -1
        self.buy_list = buy_list

    # def set_buy_list(self, buy_list):
    #     self.buy_list = buy_list
    def log(self, txt, dt=None):
        ''' 策略日志打印函数'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def next(self):
        self.index = self.index + 1
        date = self.datas[0].datetime.date(0).strftime("%Y%m%d")
        self.buy(size=100)
        self.sell(size=100)
        a=1
        # if date not in self.buy_list:
        #     return True
        # buy_list = self.buy_list[date]
        # # 在调仓之前，取消之前所下的没成交也未到期的订单
        # if len(self.order_list) > 0:
        #     for od in self.order_list:
        #         self.cancel(od)  # 如果订单未完成，则撤销订单
        #     self.order_list = []  # 重置订单列表
        # # 如果是调仓日，则进行调仓操作
        # # if dt in self.trade_dates:
        #
        # # 提取当前调仓日的持仓列表
        # print('buy_list ', buy_list)
        # # 对现有持仓中，调仓后不再继续持有的股票进行卖出平仓
        # sell_stock = [i for i in self.buy_stocks_pre if i not in buy_list]
        # print('sell_stock', sell_stock)  # 打印平仓列表
        # if len(sell_stock) > 0:
        #     print("-----------对不再持有的股票进行平仓--------------")
        #     for stock in sell_stock:
        #         data = self.getdatabyname(stock)
        #         if self.getposition(data).size > 0:
        #             od = self.close(data=data)
        #             self.order_list.append(od)  # 记录卖出订单
        # # 买入此次调仓的股票：多退少补原则
        # print("-----------买入此次调仓期的股票--------------")
        # if len(sell_stock) == 0 and len(self.buy_stocks_pre) == len(buy_list):
        #     return True
        # w = 1.0/len(buy_list)
        # for stock in buy_list:
        #     data = self.getdatabyname(stock)
        #     order = self.order_target_percent(data=data, target=w * 0.95)  # 为减少可用资金不足的情况，留 5% 的现金做备用
        #     self.order_list.append(order)
        #
        # self.buy_stocks_pre = buy_list  # 保存此次调仓的股票列表


    def notify_order(self, order):
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已经处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                    (order.ref,  # 订单编号
                     order.executed.price,  # 成交价
                     order.executed.value,  # 成交额
                     order.executed.comm,  # 佣金
                     order.executed.size,  # 成交量
                     order.data._name))  # 股票名称
            else:  # Sell
                self.log('SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                         (order.ref,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm,
                          order.executed.size,
                          order.data._name))

