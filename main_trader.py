# coding:utf-8
import pickle

import pandas as pd
import redis

from helper import consts
from old.MyStrategy import *


import helper.get_ori_data


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

def get_ret(stock_dict):
    now = time.time()
    all_ret = []
    count = 0
    for code, value in stock_dict.items():
        if count % 500 == 0:
            print("processing ", count)
        count = count + 1

        trader_date = value['trade_date']
        nums_df = value[value.columns[2:]]
        nums_np = nums_df.to_numpy()
        # Index(['open_qfq', 'close_qfq', 'vol', 'ema_qfq_5', 'ema_qfq_10',
        #           'ema_qfq_20', 'ema_qfq_30', 'ema_qfq_60', 'ema_qfq_250'],
        #       dtype='object')
        open = nums_np[:, 0]
        close = nums_np[:, 1]
        ema5 = nums_np[:, 3]
        ema10 = nums_np[:, 4]
        ema20 = nums_np[:, 5]
        ema30 = nums_np[:, 6]
        ema60 = nums_np[:, 7]

        ema250 = nums_np[:, 8]

        # ret = 超过5日线.do_get_signal(nums_df, open, close, [ema5, ema10,ema20,ema30,ema60,ema250])
        ret = 近期涨幅超过30.do_get_signal(nums_df, open, close, [ema5, ema10,ema20,ema30,ema60,ema250])
        all_ret.append(ret)
        # print(ret)

    return all_ret

def load():
    with redis.Redis(host='localhost', port=6379, db=0) as r:
        now = time.time()
        stock_base = r.get(consts.redis_key_a_base)
        stock_base_ext = r.get(consts.redis_key_a_ext)
        stock_base_ref = r.get(consts.redis_key_a_ref)
        print("read_redis cost", time.time() - now)

        now = time.time()
        stock_base = pickle.loads(stock_base)
        stock_base_ext = pickle.loads(stock_base_ext)
        stock_base_ref = pickle.loads(stock_base_ref)
        print("read_redis dump cost", time.time() - now)
        return stock_base, stock_base_ext, stock_base_ref

if __name__ == '__main__':
    # os.environ['NUMBA_NUM_THREADS'] = '16'
    # 获取原始数据
    print("start ...")
    stock_base, stock_dict_ext, shangzheng = load()

    all_ret = get_ret(stock_base)

    print("final", np.mean(np.array(all_ret)))
