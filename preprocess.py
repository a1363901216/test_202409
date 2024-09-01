import copy
import time

import pandas as pd
import numpy as np

from helper.download_data import write_file
import clickhouse_util

start_date = '20170101'
end_date = '20240630'


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
    merged = merged.drop(columns=['trade_date'])
    merged['can_trade'] = merged['can_trade'].infer_objects(copy=False).fillna(False)
    merged = merged.infer_objects(copy=False).ffill(axis=0).infer_objects(copy=False).bfill(axis=0)
    merged.set_index(['cal_date'], inplace=True, drop=False)
    merged.sort_index(inplace=True)

    write_file(filename='./data/shangzheng.pkl', value=merged)


# @numba.jit(nopython=True)
def pre_process():
    pd.set_option("future.no_silent_downcasting", True)
    clickhouse_util.optimize('trade_cal')
    clickhouse_util.optimize('stock_basic')
    clickhouse_util.optimize('stk_factor_pro')
    clickhouse_util.optimize('stk_factor')
    clickhouse_util.optimize('fina_indicator')
    clickhouse_util.optimize('namechange')
    clickhouse_util.optimize('stock_basic')
    clickhouse_util.optimize('suspend_d')---+
    clickhouse_util.optimize('zhishu')

    trade_cal = clickhouse_util.from_table('SELECT * FROM trade_cal order by cal_date')
    stock_basic = clickhouse_util.from_table('SELECT ts_code FROM stock_basic order by ts_code')
    # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
    # stock_basic = pd.DataFrame(['600823.SH', '002122.SZ'], columns=['ts_code'])
    # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
    pre_dapan(trade_cal)

    list = []
    stock_dict = {}
    isTest = False
    for i in range(stock_basic.values.shape[0]):
        if i >100 and isTest:
            break

        now = time.time()
        stock_code = stock_basic.values[i][0]
        query = (f"SELECT ts_code,trade_date,open_qfq,high_qfq,low_qfq,close_qfq,"
                 f"pre_close_qfq,vol,pct_change"
                 f" FROM stk_factor where ts_code = '{stock_code}' order by ts_code,trade_date")
        merged = clickhouse_util.from_table(query)
        if merged.shape[0] == 0:
            continue
        # ST
        query = (f"SELECT *"
                 f" FROM namechange where ts_code = '{stock_code}' ")
        tmp = clickhouse_util.from_table(query)
        tmp = fill_ST(tmp, trade_cal['cal_date'].values)
        merged = pd.merge(merged, tmp, left_on='trade_date', right_on='trade_date', how='left')

        # roa roe
        query = (f"SELECT ts_code,ann_date,roe,roa,bps FROM fina_indicator where ts_code = '{stock_code}'")
        tmp = clickhouse_util.from_table(query)
        tmp_align = fill_fina_indicator(tmp, trade_cal['cal_date'].values)
        merged = pd.merge(merged, tmp_align, left_on='trade_date', right_on='ann_date', how='left')
        merged.drop(columns=['ann_date'], inplace=True)

        # 因子-专业版
        stock_code = stock_basic.values[i][0]
        query = (f"SELECT ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,total_share"
                 f",total_mv,ema_qfq_5,ema_qfq_10,ema_qfq_20,ema_qfq_30,ema_qfq_60,ema_qfq_250"
                 f",taq_down_qfq,taq_mid_qfq,taq_up_qfq"
                 f" FROM stk_factor_pro where ts_code = '{stock_code}'")
        tmp = clickhouse_util.from_table(query)
        tmp.drop(columns=['ts_code'], inplace=True)
        merged = pd.merge(merged, tmp, left_on='trade_date', right_on='trade_date', how='left')
        merged = merged.infer_objects(copy=False).ffill(axis=0).infer_objects(copy=False).bfill(axis=0)

        # 明日开盘价

        merged['next_open'] = copy.deepcopy(merged['open_qfq'].shift(-1))
        merged.loc[merged.index[-1], 'next_open'] = copy.deepcopy(merged.loc[merged.index[-1], 'open_qfq'])

        # list.append(merged)
        # stock_dict[stock_code] = merged
        list.append(merged)
        print("stock_code", stock_code, len(list), time.time() - now)
    all = pd.concat(list)
    if not isTest:
        write_file(filename='./data/tushare_stock_dict_only_price.pkl', value=all)
    else:
        write_file(filename='./data/tushare_stock_dict_only_price_small.pkl', value=all)

    print('finish')


if __name__ == '__main__':
    pre_process()
