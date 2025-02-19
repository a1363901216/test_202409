# coding:utf-8
import pickle

import numpy as np
import pandas as pd
import redis
import talib
import time

from ck_2_redis import init_cache
from helper import consts

from strategy import ma金叉买入, ma上涨不破30日均线


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


def get_ret(stock_base, stock_base_ext):
    now = time.time()
    all_ret = []
    count = 0
    for code, base in stock_base.items():
        ext = stock_base_ext[code]
        if count % 100 == 0:
            print("processing ", count)
        count = count + 1
        now1 = time.time()
        # ret = ma金叉买入.do_get_signal(stock_base, o, c, [sma5, sma10, sma20, sma60, sma120, sma250])
        ret = ma上涨不破30日均线.do_get_signal(base, ext)
        all_ret.append(ret)
        # print("compute one cost", time.time() - now1)
    # print(ret)

    print("compute cost", time.time() - now)
    return all_ret


def load():
    with redis.Redis(host='localhost', port=6379, db=0) as r:
        r.config_set('proto-max-bulk-len', '9073741824')
        now = time.time()
        stock_base = r.get(consts.redis_key_a_base)
        stock_base_ext = r.get(consts.redis_key_a_ext)
        print("read_redis cost", time.time() - now)

        now = time.time()
        stock_base = pickle.loads(stock_base)
        stock_base_ext = pickle.loads(stock_base_ext)
        print("read_redis dump cost", time.time() - now)
        return stock_base, stock_base_ext, 1


if __name__ == '__main__':
    # os.environ['NUMBA_NUM_THREADS'] = '16'
    # 获取原始数据
    print("start ...")
    init_cache()
    stock_base, stock_base_ext, shangzheng = load()

    all_ret = get_ret(stock_base, stock_base_ext)

    print("final", np.mean(np.array(all_ret)))
