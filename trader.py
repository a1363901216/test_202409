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

def filter_df(data):
    data = data[(data['can_trade'])]

    data = data[(data['pb'] > 0) &
                (data['roa'] > 0) & (data['roa'] > 1) &
                (not data['isST'])]

    data = data[(data['close_qfq'] < 1.0999 * data['pre_close_qfq']) &
                (data['close_qfq'] > 0.9001 * data['pre_close_qfq'])]

    return data


def build_buy_list(stock_all):
    date2stock = {}
    for stock, value in stock_all.items():
        value = filter_df(value)
        for row in value.itertuples():
            if row.cal_date not in date2stock:
                stock_set = set()
                date2stock[row.cal_date] = stock_set
            date2stock[row.cal_date].add(row.ts_code)
    return date2stock


def prepare_feed(data, cerebro):
    shangzheng = read_file(filename='./data/shangzheng.pkl')
    cur_data = pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest'])
    cur_data['open'] = shangzheng['open']
    cur_data['high'] = shangzheng['high']
    cur_data['low'] = shangzheng['low']
    cur_data['close'] = shangzheng['close']
    cur_data['volume'] = shangzheng['vol']
    cur_data['openinterest'] = shangzheng['pct_chg']
    cur_data['datetime'] = pd.to_datetime(shangzheng["cal_date"])

    cur_data.set_index(['datetime'], inplace=True, drop=False)
    cur_data.sort_index(inplace=True)
    datafeed = bt.feeds.PandasData(dataname=cur_data, fromdate=datetime.datetime(2017, 1, 3),
                                   todate=datetime.datetime(2024, 6, 28))
    cerebro.adddata(datafeed, name='0000001.SH')

    # i = -1
    # stock_list = np.unique(data['ts_code'])
    #
    # for stock, value in data.items():
    #     i = i + 1
    #     if i % 100 == 0:
    #         print('prepare_feed ', i)
    #     cur_data = pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest'])
    #     tmp = value
    #     cur_data['open'] = tmp['open_qfq']
    #     cur_data['high'] = tmp['high_qfq']
    #     cur_data['low'] = tmp['low_qfq']
    #     cur_data['close'] = tmp['close_qfq']
    #     cur_data['volume'] = tmp['vol']
    #     cur_data['openinterest'] = tmp['pct_change']
    #     cur_data['datetime'] = pd.to_datetime(tmp["cal_date"])
    #     cur_data.set_index(['datetime'], inplace=True, drop=False)
    #     cur_data.sort_index(inplace=True)
    #     datafeed = bt.feeds.PandasData(dataname=cur_data, fromdate=datetime.datetime(2017, 1, 3),
    #                                    todate=datetime.datetime(2024, 6, 28))
    #
    #     cerebro.adddata(datafeed, name=stock)


def small_data_if_need(data):
    stocks = data['ts_code'].unique()
    used_stock_set = set()
    for i in range(len(stocks)):
        if i > 10:
            break
        stock = stocks[i]
        used_stock_set.add(stock)

    return data[data['ts_code'].isin(used_stock_set)]


def set_sangzheng(cerebro):
    shangzheng = read_file(filename='./data/shangzheng.pkl')
    cur_data = pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest'])
    cur_data['open'] = shangzheng['open']
    cur_data['high'] = shangzheng['high']
    cur_data['low'] = shangzheng['low']
    cur_data['close'] = shangzheng['close']
    cur_data['volume'] = shangzheng['vol']
    cur_data['openinterest'] = shangzheng['pct_chg']
    cur_data['datetime'] = pd.to_datetime(shangzheng["cal_date"])

    cur_data.set_index(['datetime'], inplace=True, drop=False)
    cur_data.sort_index(inplace=True)
    datafeed = bt.feeds.PandasData(dataname=cur_data, fromdate=datetime.datetime(2017, 1, 3),
                                   todate=datetime.datetime(2024, 6, 28))
    cerebro.adddata(datafeed, name='0000001.SH')
    cerebro.addobserver(bt.observers.Benchmark, data=datafeed)


def prepare_one_buy_list(code, data, col_2_index):
    if code.startswith('68') or code.startswith('4') or code.startswith('8'):
        return None

    data = data[(data[:, col_2_index['pb']] > 0) &
                (data[:, col_2_index['pb']] < 1) & (data[:, col_2_index['roa']] > 1) &
                (data[:, col_2_index['isST']] < 0)]

    data = data[(data[:, col_2_index['close_qfq']] < 1.0999 * data[:, col_2_index['pre_close_qfq']]) &
                (data[:, col_2_index['close_qfq']] > 0.9001 * data[:, col_2_index['pre_close_qfq']])]
    if data.shape[0] == 0:
        return None
    return data[:, col_2_index['trade_date']]


def do_buy(stock_all, buy_date_dict):
    pass


def do_sell(to_sell_dict, cur_position, stock_all, current_da):
    pass
    # for code in to_sell_dict:
    #     code
    #     cur_position['cash'] = cur_position['cash']


def do_trade(stock_all, buy_date_dict):
    columns = stock_all['columns']
    col_2_index = {key: index for index, key in enumerate(columns)}

    cur_position = {
        "cash": 1000000,
        "stock_position": {}
        # "code"->{
        #     "size":100
        #     "price":10000
        # }
    }

    trade_cal = stock_all['trade_cal']
    for i in range(len(trade_cal)):
        if trade_cal not in buy_date_dict:
            continue
        cur_buy_list = buy_date_dict[trade_cal]
        cur_buy_set = set(cur_buy_list)

        to_sell_dict = {key: value for key, value in cur_position['stock_position'].items() if key not in cur_buy_set}

        cash = cur_position["cash"]
        avg_stock_cash = cash * 0.95 / len(cur_buy_list)
        target_stock_position = {
            "cash": cash,
            "stock_position": {}
        }
        for j in range(len(cur_buy_list)):
            code = cur_buy_list[j]
            stock_info = stock_all['stock_dict'][code]
            next_open = stock_all[col_2_index['next_open']]
            cur_close = stock_all[col_2_index['close_qfq']]
            size = math.floor(avg_stock_cash / cur_close / 100) * 100.0
            target_stock_position['stock_position'][code] = cur_position

        to_sell_dict = {}


def prepare_buy_list(stock_all):
    columns = stock_all['columns']
    col_2_index = {key: index for index, key in enumerate(columns)}
    # [('cal_date', '<i4'), ('ts_code', 'S20'), ('open_qfq', '<f4'), ('high_qfq', '<f4'), ('low_qfq', '<f4'),
    #  ('close_qfq', '<f4'), ('pre_close_qfq', '<f4'), ('vol', '<f4'), ('pct_change', '<f4'), ('isST', '?'),
    #  ('roe', '<f4'), ('roa', '<f4'), ('bps', '<f4'), ('turnover_rate', '<f4'), ('volume_ratio', '<f4'), ('pe', '<f4'),
    #  ('pb', '<f4'), ('total_share', '<f4'), ('total_mv', '<f4'), ('ema_qfq_5', '<f4'), ('ema_qfq_10', '<f4'),
    #  ('ema_qfq_20', '<f4'), ('ema_qfq_30', '<f4'), ('ema_qfq_60', '<f4'), ('ema_qfq_250', '<f4'),
    #  ('taq_down_qfq', '<f4'), ('taq_mid_qfq', '<f4'), ('taq_up_qfq', '<f4'), ('can_trade', '?'), ('next_open', '<f4')]

    stock_buy_time = [{
        "code": code,
        "buy_dates": prepare_one_buy_list(code=code, data=value, col_2_index=col_2_index)}
        for code, value in stock_all['stock_dict'].items()]

    date_2_buy_list = {}
    for i in range(len(stock_buy_time)):
        code, buy_dates = stock_buy_time[i]["code"], stock_buy_time[i]["buy_dates"]
        if buy_dates is None:
            continue
        for date in buy_dates:
            if date not in date_2_buy_list:
                date_2_buy_list[date] = []
            date_2_buy_list[date].append(code)
    stock_all = stock_all.reshape(stock_all.shape[0])
    stock_all = stock_all[np.lexsort([stock_all['ts_code'], stock_all['roa']], axis=0)]
    # id1 = stock_all['ts_code'].argsort()
    # id2 = id1[stock_all['cal_date'].argsort(kind='stable')]
    # stock_all = stock_all[id2]
    stock_all = stock_all.reshape(stock_all.shape[0], 1)

    data = filter_df(stock_all)
    # data = data.reshape(len(data),1)

    return data


def get_buy_data(data):
    return data[(data['isST'] < 0) &
                (data['pb'] > 0) & (data['pb'] < 1) &
                (data['roa'] > 1) &
                (data['close_qfq'] < 1.0999 * data['pre_close_qfq']) &
                (data['close_qfq'] > 0.9001 * data['pre_close_qfq'])
                ]


def get_hold_data(data):
    try:
        hold_data = data[(data['isST'] < 0) &
                         (data['pb'] > 0) & (data['pb'] < 1) &
                         (data['roa'] > 1) &
                         (data['close_qfq'] < 1.0999 * data['pre_close_qfq']) &
                         (data['close_qfq'] > 0.9001 * data['pre_close_qfq'])
                         ]
        if hold_data.shape[0] == 0:
            return None
    except Exception as e:
        print(f"Error get_hold_data {data}: {e}")
    # ret = {}
    # for row in data.itertuples():
    #     print(getattr(row, 'column_name'))
    return hold_data


def get_one_buy_info(code_datas_batch):
    ret = []
    try:
        for code_data in code_datas_batch:
            # Your processing code here
            code = code_data[0]
            data = code_data[1]
            hold_data = get_hold_data(data)

            ret.append((code, hold_data))
    except Exception as e:
        print(f"Error processing {code_datas_batch}: {e}")

    return ret


def get_buy_info(stock_all):
    code_datas = [(code, data) for code, data in stock_all.items()]
    cpus = os.cpu_count()
    batch_size = len(code_datas) // cpus
    code_datas_batches = []
    for i in range(cpus):
        begin = i * batch_size
        end = min((i + 1) * batch_size, len(code_datas))
        code_datas_batches.append(code_datas[begin:end])
    # for v in code_datas:
    #     get_one_buy_info(v)

    results = Parallel(n_jobs=-1)(
        delayed(get_one_buy_info)(code_datas_batch) for code_datas_batch in code_datas_batches)
    print(results)
    return results


if __name__ == '__main__':
    now = time.time()
    stock_all = read_file(filename='./data/tushare_stock_dict_small.pkl')
    # stock_all = read_file(filename='./data/tushare_stock_dict.pkl')
    print(stock_all.columns)
    print("get input cost ", time.time() - now)

    now = time.time()
    stock_all = stock_all.sort_values(by='trade_date')
    buy_data = get_buy_data(stock_all)
    # stock_all = stock_all.sort_values(by='trade_date')
    my_broker = MyOwnBroker()
    i = 0
    for trade_date, group in buy_data.groupby('trade_date'):
        dict = {row.ts_code: row for row in group.itertuples()}
        my_broker.next(dict, group, trade_date)
        i = i + 1
        if i % 100 == 0:
            print("buy_data.groupby ", trade_date)
    my_broker.checkout()
    print("my_broker.money ", my_broker.money)
    print("my_broker.money cost", time.time() - now)

    my_broker.plot_money()
