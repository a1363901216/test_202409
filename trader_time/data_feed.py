import copy
import time

import pandas as pd
import numpy as np

from helper.download_data import write_file, read_file
import clickhouse_util

start_date = '20170101'
end_date = '20240630'

file_name_test = '../data/tushare_stock_dict_only_price_small.pkl'
file_name = '../data/tushare_stock_dict_only_price.pkl'


class MyDataFeed():
    def __init__(self, isTest, reload_from_clickhouse):
        # 加载数据
        self.ori_data, self.shangzheng = self.load_data(isTest, reload_from_clickhouse)

        # 每个股票补充指标到当日，合并所有股票到一个dataframe按时间索引
        self.data_ex = self.fill_ex(self.ori_data)
        self.data_ex = self.merge_stock(self.data_ex)

        # 按时间加索引
        self.set_index()

        self.trade_dates = []

        self.date_index = -1

    def load_data(self, isTest, reload_from_clickhouse):
        now = time.time()
        if reload_from_clickhouse:
            self.do_reload_from_clickhouse(isTest)
            print("do_reload_from_clickhouse ", time.time() - now)

        now = time.time()
        if isTest:
            stock_all = read_file(filename=file_name_test)
        else:
            stock_all = read_file(filename=file_name)
        print("read_file ", time.time() - now)
        shangzheng = read_file(filename='../data/shangzheng.pkl')
        return stock_all, shangzheng

    def next(self):
        self.date_index = self.date_index + 1
        trade_dates = self.trade_dates[self.date_index]

        # ret
        # df[df['trade_dates'] == trade_dates]
        return 0

    def fill_ex(self, ori_data):
        for key, df in ori_data.items():
            df['last_close'] = df['close'].shift(1)
            df['last_ema5'] = df['ema5'].shift(1)
        return ori_data

    def merge_stock(self, data):
        list = []
        for key, df in data.items():
            list.append(df)
        merged = pd.concat(list, ignore_index=True)
        merged = merged.set_index(['trade_date'], drop=False)
        merged = merged.sort_index()
        return merged

    def do_reload_from_clickhouse(self, isTest):
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

        self.trade_cal = trade_cal = clickhouse_util.from_table('SELECT * FROM trade_cal order by cal_date')
        stock_basic = clickhouse_util.from_table('SELECT ts_code FROM stock_basic order by ts_code')
        # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
        # stock_basic = pd.DataFrame(['600823.SH', '002122.SZ'], columns=['ts_code'])
        # stock_basic = pd.DataFrame(['002122.SZ'], columns=['ts_code'])
        self.pre_dapan(trade_cal)

        list = []
        stock_dict = {}
        for i in range(stock_basic.values.shape[0]):
            if i > 5 and isTest:
                break

            now = time.time()
            # 因子-专业版

            stock_code = stock_basic.values[i][0]
            query = (f"SELECT ts_code,trade_date,open_qfq as open,close_qfq as close,"
                     f"vol,ema_qfq_5 as ema5,ema_qfq_10 as ema40,ema_qfq_20 as ema20,ema_qfq_30 as ema30"
                     f" FROM stk_factor_pro where ts_code = '{stock_code}' order by trade_date")
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
            # merged = merged.set_index(['trade_date'], drop=False)
            # merged = merged.sort_index()
            stock_dict[stock_code] = merged
            print("stock_code", stock_code, len(stock_dict), time.time() - now)
        # all = pd.concat(list)
        # all = all.set_index(['ts_code'])
        # all = all.sort_index()
        if isTest:
            write_file(filename=file_name_test, value=stock_dict)
        else:
            write_file(filename=file_name, value=stock_dict)

        print('finish')

    def pre_dapan(self, trade_cal):
        merged = clickhouse_util.from_table("SELECT * FROM zhishu where ts_code='000001.SH'")
        merged = merged.ffill(axis=0).bfill(axis=0)

        merged['can_trade'] = True
        merged = pd.merge(trade_cal, merged, left_on='cal_date', right_on='trade_date', how='left')
        merged = merged.drop(columns=['trade_date'])
        merged['can_trade'] = merged['can_trade'].infer_objects(copy=False).fillna(False)
        merged = merged.infer_objects(copy=False).ffill(axis=0).infer_objects(copy=False).bfill(axis=0)
        merged.set_index(['cal_date'], inplace=True, drop=False)
        merged.sort_index(inplace=True)

        write_file(filename='../data/shangzheng.pkl', value=merged)
