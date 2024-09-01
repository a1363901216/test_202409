import time

import tushare as ts
from sqlalchemy import create_engine
import threading

import clickhouse_util

ts.set_token('0d9223de9a848ebf6f2e268039b1762919bfbcb1826df44246220d4c')

pro = ts.pro_api()

start_date = '20170101'
end_date = '20240630'

username = 'root'
password = 'a000'
host = '127.0.0.1'
database = 'tushare'

# 创建与MySQL数据库的连接
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')

stk_factor_pro_all = {}
lock = threading.Lock()
count = 0

fields = 'ts_code,trade_date,open_qfq,high_qfq,low_qfq,close_qfq,pre_close,pct_chg,vol,turnover_rate,volume_ratio,pe,pb,dv_ratio,dv_ttm,total_share,total_mv,asi_qfq,asit_qfq,atr_qfq,bbi_qfq,bias1_qfq,bias2_qfq,bias3_qfq,boll_lower_qfq,boll_mid_qfq,boll_upper_qfq,brar_ar_qfq,brar_br_qfq,cci_qfq,cr_qfq,dfma_dif_qfq,dfma_difma_qfq,dmi_adx_qfq,dmi_adxr_qfq,dmi_mdi_qfq,dmi_pdi_qfq,downdays,updays,dpo_qfq,madpo_qfq,ema_qfq_10,ema_qfq_20,ema_qfq_250,ema_qfq_30,ema_qfq_5,ema_qfq_60,ema_qfq_90,emv_qfq,maemv_qfq,expma_12_qfq,expma_50_qfq,kdj_qfq,kdj_d_qfq,kdj_k_qfq,ktn_down_qfq,ktn_mid_qfq,ktn_upper_qfq,lowdays,topdays,ma_qfq_10,ma_qfq_20,ma_qfq_250,ma_qfq_30,ma_qfq_5,ma_qfq_60,ma_qfq_90,macd_qfq,macd_dea_qfq,macd_dif_qfq,mass_qfq,ma_mass_qfq,mfi_qfq,mtm_qfq,mtmma_qfq,obv_qfq,psy_qfq,psyma_qfq,roc_qfq,maroc_qfq,rsi_qfq_12,rsi_qfq_24,rsi_qfq_6,taq_down_qfq,taq_mid_qfq,taq_up_qfq,trix_qfq,trma_qfq,vr_qfq,wr_qfq,wr1_qfq,xsii_td1_qfq,xsii_td2_qfq,xsii_td3_qfq,xsii_td4_qfq'
fields_no_pro = 'ts_code,trade_date,close,open,high,low,pre_close,pct_change,vol,open_qfq,close_qfq,high_qfq,low_qfq,pre_close_qfq'


def get_stk_factor(cur_date):
    for _ in range(3):
        try:
            now = time.time()
            stk_factor = pro.stk_factor(trade_date=cur_date, fields='ts_code,trade_date,pct_change')
            stk_factor.fillna(value=-1000000, inplace=True)
            clickhouse_util.to_table(stk_factor, 'stk_factor')
            print('get_stk_factor ok for ', cur_date, time.time() - now)
            break
        except Exception as e:
            time.sleep(1)
            print('================get_stk_factor_pro fail===========', cur_date, e)


def get_fina_indicator(cur_stock):
    for _ in range(3):
        try:
            # cur_stock='600823.SH'
            now = time.time()
            fina_indicator = pro.fina_indicator(ts_code=cur_stock, fields='', start_date=start_date, end_date=end_date)
            fina_indicator = fina_indicator.groupby('end_date').nth(0)
            fina_indicator.fillna(value=-1000000.0, inplace=True)
            clickhouse_util.to_table(fina_indicator, 'fina_indicator')
            print('fina_indicator ok for ', cur_stock, time.time() - now)
            break
        except Exception as e:
            time.sleep(1)
            print('================get_fina_indicator fail===========', cur_stock, e)


def get_stk_factor_pro(dates):
    for i, _ in enumerate(dates):
        cur_date = trade_cal.values[i][0]
        for _ in range(3):
            try:
                now = time.time()
                stk_factor_pro = pro.stk_factor_pro(start_date=cur_date, end_date=cur_date, trade_date=cur_date,
                                                    fields=fields)
                gap1 = time.time() - now
                now = time.time()
                stk_factor_pro.fillna(value=-1000000, inplace=True)
                gap2 = time.time() - now
                now = time.time()
                #                       index=False)
                clickhouse_util.to_table(stk_factor_pro, 'stk_factor_pro')
                print('stk_factor_pro ok for ', cur_date, gap1, gap2, time.time() - now)
                break
            except Exception as e:
                time.sleep(1)
                print('================get_stk_factor_pro fail===========', cur_date, e)


def get_stk_factor(dates):
    for i, _ in enumerate(dates):
        cur_date = trade_cal.values[i][0]
        for _ in range(3):
            try:
                now = time.time()
                stk_factor = pro.stk_factor(start_date=cur_date, end_date=cur_date, trade_date=cur_date,
                                            fields=fields_no_pro)
                stk_factor.fillna(value=-1000000, inplace=True)
                #                       index=False)
                clickhouse_util.to_table(stk_factor, 'stk_factor')
                print('stk_factor ok for ', cur_date, time.time() - now)
                break
            except Exception as e:
                time.sleep(1)
                print('================stk_factor fail===========', cur_date, e)


def sleep_milliseconds(milliseconds):
    start = time.time()
    end = start + milliseconds / 1000
    while time.time() < end:
        pass


path = './data/'
if __name__ == '__main__':

    clickhouse_util.optimize('trade_cal')
    clickhouse_util.optimize('stock_basic')
    clickhouse_util.optimize('stk_factor_pro')
    # 交易日期
    trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    # clickhouse_util.to_table(trade_cal, 'trade_cal')
    # # 股票列表
    # stock_basic = pro.stock_basic(exchange='', list_status='L', fields='')
    # stock_basic.fillna(value='', inplace=True)
    # clickhouse_util.to_table(stock_basic, 'stock_basic')
    #
    # clickhouse_util.optimize('trade_cal')
    # clickhouse_util.optimize('stock_basic')
    # # clickhouse_util.optimize('stk_factor_pro')
    # # 股票因子专业
    # get_stk_factor_pro(trade_cal.values)
    # get_stk_factor(trade_cal.values)

    # 历史名字 st 记录
    # clickhouse_util.optimize('namechange')
    # stock_basic = pro.stock_basic(exchange='', list_status='L', fields='')
    # for i, _ in enumerate(stock_basic.values[:, 0]):
    #     cur_stock = stock_basic.values[i][0]
    #     namechange = pro.namechange(ts_code=cur_stock)
    #     if namechange.shape[0] > 0:
    #         namechange = namechange.drop(columns=['ann_date'])
    #         namechange.fillna(value='', inplace=True)
    #         aaa = clickhouse_util.to_table(namechange, 'namechange')
    #         sleep_milliseconds(50)

    # 停牌
    # trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    # for i, _ in enumerate(trade_cal.values):
    #     cur_date = trade_cal.values[i][0]
    #     suspend_d = pro.suspend_d(start_date=cur_date, end_date=cur_date, fields='ts_code,trade_date,suspend_timing,suspend_type')
    #     suspend_d.fillna(value='', inplace=True)
    #     clickhouse_util.to_table(suspend_d, 'suspend_d')
    #     sleep_milliseconds(100)

    # 股价上下界
    # trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    # for i, _ in enumerate(trade_cal.values):
    #     cur_date = trade_cal.values[i][0]
    #     stk_limit = pro.stk_limit(trade_date=cur_date)
    #     stk_limit.fillna(value='', inplace=True)
    #     clickhouse_util.to_table(stk_limit, 'stk_limit')
    #     sleep_milliseconds(30)

    # # 普通因子,前复权
    # trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    # for i, _ in enumerate(trade_cal.values):
    #     now = time.time()
    #     cur_date = trade_cal.values[i][0]
    #     get_stk_factor(cur_date)
    #     sleep_milliseconds(20)

    ## 财经 pb roe roa
    # stock_basic = pro.stock_basic(exchange='', list_status='L', fields='')
    # for i, _ in enumerate(stock_basic.values[:,0]):
    #     cur_stock = stock_basic.values[i][0]
    #     # cur_stock = '000035.SZ'
    #     get_fina_indicator(cur_stock)
    #     sleep_milliseconds(20)

    # 大盘指数

    # trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')
    # for i, _ in enumerate(trade_cal.values):
    #     cur_date = trade_cal.values[i][0]
    #     index_dailybasic = pro.index_dailybasic(trade_date=cur_date,start_date=cur_date, end_date=cur_date, fields='')
    #     index_dailybasic.fillna(value='', inplace=True)
    #     clickhouse_util.to_table(index_dailybasic, 'index_dailybasic')
    #     sleep_milliseconds(100)

    shangzheng = pro.index_daily(ts_code='000001.SH', start_date=start_date, end_date=end_date)
    shangzheng.fillna(value='', inplace=True)
    clickhouse_util.to_table(shangzheng, 'zhishu')