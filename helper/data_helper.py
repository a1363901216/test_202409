# coding:utf-8
import time, datetime, traceback, sys

import numpy
import numpy as ny



class StockData:
    def __init__(self, ori_history_prices):  # 构造函数
        self.ori_history_prices = ori_history_prices
        # self.ori_financial_data = ori_financial_data
        self.code_2_index = {}
        self.index_2_code = {}
        self.times = {}
        self.end_time = {}
        self.col_2_index = {}
        self.index_2_col = {}

        self.prices = {}
        self.financial = {}

        # self.build_data(ori_history_prices, ori_financial_data)

    def build_data(self, ori_history_prices, ori_financial_data):
        #################### 价格

        self.prices = Array3(ori_history_prices, None)

        #################### 财务
        financial_tables = ["Balance", "Income", "CashFlow", "Capital", "PershareIndex"]
        financial = {}
        for _, tableName in enumerate(financial_tables):
            financial[tableName] = Array3(ori_history_prices, self.prices)
        self.financial = financial


def get_pb(history_prices, history_net_assets):
    price_times = history_prices['002762.SZ'].values[:, 0]
    prices = history_prices['002762.SZ'].values[:, 1:]
    print(history_prices)
    print(history_net_assets)


# def get_pb(history_prices, history_net_assets):
#     print(history_prices)
#     history_prices.
#     print(history_net_assets)

class Array3:
    def __init__(self, dict_code_df, ref_array3):  # 构造函数
        self.ori_data = dict_code_df
        self.array3 = None

        self.stocks = None  # 第1维 股票
        self.col_2_index1 = {}

        self.times = None  # 第2维 时间
        self.col_2_index2 = {}

        self.cols = None  # 第3维 属性
        self.col_2_index3 = {}

        if ref_array3 is None:
            self.convert_dict()
        else:
            self.convert_dict_fill(ref_array3)

    def convert_dict(self):
        ori_data = self.ori_data

        code_list = ny.zeros(len(ori_data), dtype='U10')
        index = 0
        for code in ori_data:
            code_list[index] = code
            index = index + 1
        ny.sort(code_list)
        self.stocks = code_list
        index = 0
        for code in code_list:
            self.col_2_index1[code] = index
            index = index + 1

        for code in ori_data:
            self.cols = columns = ori_data[code].columns.values
            for i in range(len(columns)):
                self.col_2_index3[columns[i]] = i
            self.times = times = ori_data[code].values[:, 0]
            for i in range(len(times)):
                self.col_2_index2[times[i]] = i
            break

        array3 = ny.zeros((len(self.stocks), len(self.times), len(self.cols) - 1), dtype='float64')
        for code, dt in ori_data.items():
            array3[self.col_2_index1[code]] = dt.values[:, 1:]
        self.array3 = array3

    def convert_dict_fill(self, ref_array3):
        ori_data = self.ori_data
        code_list = ny.zeros(len(ori_data), dtype='U10')
        index = 0
        for code in ori_data:
            code_list[index] = code
            index = index + 1
        ny.sort(code_list)
        index = 0
        for code in code_list:
            self.col_2_index1[code] = index
            index = index + 1

        for code in ori_data:
            columns = ori_data[code].columns.values
            for i in range(len(columns)):
                col = columns[i]
                self.col_2_index3[col] = i
            break
        array3 = ny.zeros((len(ref_array3.stocks), len(ref_array3.times), len(self.col_2_index3) - 1), dtype='float64')

        id_cur = 0
        for code in ori_data:
            table = ori_data[code].values
            id_ref = 0
            for ref_time in ref_array3.times:
                if id_cur == 0 and ref_time < table[0, 0]:
                    array3[ref_array3.col_2_index1[code]][id_ref] = table[id_cur, 1:]
                elif id_cur == len(table[:, 0]) - 1:
                    array3[ref_array3.col_2_index1[code]][id_ref] = table[id_cur, 1:]
                elif ref_time < table[id_cur + 1, 0]:
                    array3[ref_array3.col_2_index1[code]][id_ref] = table[id_cur, 1:]
                else:
                    array3[ref_array3.col_2_index1[code]][id_ref] = table[id_cur, 1:]
                    if id_cur < len(table[:, 0])-1:
                        id_cur = id_cur + 1
                id_ref = id_ref + 1
        self.array3 = array3
        self.times = ref_array3.times
        self.col_2_index2 = ref_array3.col_2_index2
