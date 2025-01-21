import copy
import pickle
import time

import pandas as pd
import numpy as np
import redis
import talib

from helper.consts import redis_key_a_base, redis_key_a_ext
from helper.download_data import write_file, read_file
from helper import clickhouse_util, consts

start_date = '20170101'
end_date = '20240630'

file_name_test = 'data/tushare_stock_dict_only_price_small.pkl'
file_name = 'data/tushare_stock_dict_only_price.pkl'


def fill_fina_indicator(df, ref_times):
    # 并集填充
    ret = pd.DataFrame(columns=['ann_date'])
    ret['ann_date'] = ref_times
    ret = pd.merge(ret, df, on='ann_date', how='outer')
    ret = ret.ffill(axis=0).bfill(axis=0)
    # 非交易日删除
    tmp = pd.DataFrame(columns=['ann_date'])
    tmp['ann_date'] = ref_times
    ret = pd.merge(tmp, ret, on='ann_date', how='left')
    ret = ret.drop(columns=['ts_code'])

    # tmp = pd.DataFrame(columns=['ann_date', 'tmp'])
    # tmp['ann_date'] = ref_times
    # tmp['tmp'] = ref_times
    # ret = pd.merge(ret, tmp, on='ann_date', how='outer')
    # ret = ret.fillna(value='')
    # ret = ret[ret['tmp'] != '']
    # ret = ret.drop(columns=['tmp', 'ts_code'])
    return ret


def fill_ST(df, ref_times):
    # 并集填充
    ret = pd.DataFrame(columns=['trade_date', 'isST'])
    ret['trade_date'] = ref_times
    ret['isST'] = -np.ones((len(ref_times)), dtype='float32')
    df = df.sort_values(by='end_date', ascending=True).drop_duplicates(['ts_code', 'start_date'], keep='first')
    df.loc[df['end_date'] == '', 'end_date'] = '99999999'
    for i, row in df.iterrows():
        cur = row['name']
        start_date = row['start_date']
        end_date = row['end_date']
        if 'ST' in cur:
            ret.loc[(ret['trade_date'] >= start_date) & (ret['trade_date'] <= end_date), 'isST'] = 1.0
    ret['isST'].fillna(-1.0)
    # ret = ret.drop(columns=['tmp', 'ts_code'])
    return ret


def pre_dapan(trade_cal):
    merged = clickhouse_util.from_table("SELECT * FROM zhishu where ts_code='000001.SH'")
    merged = merged.ffill(axis=0).bfill(axis=0)

    merged['can_trade'] = True
    merged = pd.merge(trade_cal, merged, left_on='cal_date', right_on='trade_date', how='left')
    merged = merged.drop(columns=['cal_date'])
    merged['can_trade'] = merged['can_trade'].infer_objects(copy=False).fillna(False)
    merged = merged.infer_objects(copy=False).ffill(axis=0).infer_objects(copy=False).bfill(axis=0)
    merged['close_qfq'] = merged['close']
    merged.set_index(['trade_date'], inplace=True, drop=False)
    merged.sort_index(inplace=True)

    write_file(filename='data/shangzheng.pkl', value=merged)
    return merged


def compute_ext_info(merged_ext):
    c = merged_ext['close'].to_numpy()
    merged_ext['sma5'] = talib.SMA(c, timeperiod=5)
    merged_ext['sma10'] = talib.SMA(c, timeperiod=10)
    merged_ext['sma20'] = talib.SMA(c, timeperiod=20)
    merged_ext['sma60'] = talib.SMA(c, timeperiod=60)
    merged_ext['sma120'] = talib.SMA(c, timeperiod=120)
    merged_ext['sma250'] = talib.SMA(c, timeperiod=250)
    return merged_ext


# @numba.jit(nopython=True)
def load_2_redis():
    isTest = consts.isTest
    now = time.time()

    pd.set_option("future.no_silent_downcasting", True)
    clickhouse_util.optimize('trade_cal')
    clickhouse_util.optimize('stock_basic')
    clickhouse_util.optimize('stk_factor_pro')
    clickhouse_util.optimize('stk_factor')
    clickhouse_util.optimize('fina_indicator')
    clickhouse_util.optimize('namechange')
    clickhouse_util.optimize('stock_basic')
    clickhouse_util.optimize('suspend_d')
    clickhouse_util.optimize('zhishu')

    trade_cal = clickhouse_util.from_table('SELECT * FROM trade_cal order by cal_date')
    stock_basic = clickhouse_util.from_table('SELECT ts_code FROM stock_basic order by ts_code')
    # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
    # stock_basic = pd.DataFrame(['600823.SH', '002122.SZ'], columns=['ts_code'])
    # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
    # stock_basic = pd.DataFrame(['000001.SZ'], columns=['ts_code'])
    shangzheng = pre_dapan(trade_cal)

    list = []
    stock_dict = {}
    stock_dict_ext = {}
    now = time.time()
    for i in range(stock_basic.values.shape[0]):
        if i > consts.test_code_count and isTest:
            break
        now1 = time.time()
        # 因子-专业版
        stock_code = stock_basic.values[i][0]
        # query = (f"SELECT ts_code as code,trade_date as date,open_qfq as open,close_qfq as close,pct_change,"
        #          f"high_qfq as high, low_qfq as low, vol, turnover_rate, volume_ratio, pe, pb,dv_ratio"
        #          f" FROM stk_factor_pro where ts_code = '{stock_code}'")
        # dv_ratio: 股息率
        query = (f"SELECT ts_code,trade_date,open_qfq,close_qfq,pct_chg,"
                 f"high_qfq, low_qfq, turnover_rate, vol, pe, pb, dv_ratio, total_mv"
                 f" FROM stk_factor_pro where ts_code = '{stock_code}'")
        merged = clickhouse_util.from_table(query)

        # 明日开盘价
        if merged.shape[0] == 0:
            print("stock_code len is 0", stock_code, len(list))
            continue
        # merged['next_open'] = copy.deepcopy(merged['open_qfq'].shift(-1))
        # merged.loc[merged.index[-1], 'next_open'] = copy.deepcopy(merged.loc[merged.index[-1], 'close_qfq'])

        # merged['last_close'] = copy.deepcopy(merged['close_qfq'].shift(1))
        # merged.loc[0, 'last_close'] = copy.deepcopy(merged.loc[0, 'open_qfq'])

        # list.append(merged)
        # stock_dict[stock_code] = merged
        # list.append(merged)
        # merged = merged.drop(columns=['ts_code'])
        # merged = merged.set_index(['trade_date'])
        # merged = merged.sort_index()

        # 'open': np.random.random(100),
        # 'high': np.random.random(100),
        # 'low': np.random.random(100),
        # 'close': np.random.random(100),
        # 'volume': np.random.random(100)

        merged['trade_date'] = merged['trade_date'].astype(int)
        merged = merged.rename(columns={'ts_code': 'code', 'trade_date': 'date',
                                        'open_qfq': 'open', 'close_qfq': 'close',
                                        'high_qfq': 'high', 'low_qfq': 'low',
                                        'vol': 'volume'})
        stock_dict[stock_code] = merged.loc[:, merged.columns[:2]]
        merged_ext = copy.deepcopy(merged[['code', 'date', 'close']])
        merged_ext = compute_ext_info(merged_ext)
        stock_dict_ext[stock_code] = merged_ext
        print("stock_code", stock_code, len(stock_dict), time.time() - now1)
    # all = pd.concat(list)
    # all = all.set_index(['ts_code'])
    # all = all.sort_index()
    # if isTest:
    #     write_file(filename=file_name_test, value=stock_dict)
    # else:
    #     write_file(filename=file_name, value=stock_dict)

    # print('finish')
    print("read_ck cost", time.time() - now)
    return stock_dict, stock_dict_ext, shangzheng


def init_cache():
    force_update = consts.force_update_redis
    with redis.Redis(host='localhost', port=6379, db=0) as r:
        r.config_set('proto-max-bulk-len', '9073741824')
        if force_update or not r.exists(redis_key_a_base):
            now = time.time()
            # r.flushdb()
            stock_base, stock_dict_ext, shangzheng = load_2_redis()
            r.set(redis_key_a_base, pickle.dumps(stock_base))
            r.set(redis_key_a_ext, pickle.dumps(stock_dict_ext))
            r.save()
            print("load_2_redis cost", time.time() - now)


if __name__ == '__main__':
    init_cache()
