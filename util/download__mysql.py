import time

import tushare as ts
from sqlalchemy import create_engine
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def get_stk_factor(cur_date):
    for _ in range(3):
        try:
            now = time.time()
            stk_factor = pro.stk_factor(trade_date=cur_date, fields='ts_code,trade_date,pct_change')
            stk_factor.to_sql(name='stk_factor', con=engine, if_exists='append',
                              index=False)

            print('get_stk_factor ok for ', cur_date, time.time() - now)
            break
        except Exception as e:
            time.sleep(1)
            print('================get_stk_factor_pro fail===========', cur_date, e)

def get_fina_indicator(cur_stock):
    for _ in range(3):
        try:
            now = time.time()
            fina_indicator = pro.fina_indicator(ts_code=cur_stock, fields='', start_date=start_date, end_date=end_date)
            fina_indicator.to_sql(name='fina_indicator', con=engine, if_exists='append',
                                  index=False)

            print('fina_indicator ok for ', cur_stock, time.time() - now)
            break
        except Exception as e:
            time.sleep(1)
            print('================get_stk_factor_pro fail===========', cur_stock, e)


def add_one(cur_date):
    global count
    for _ in range(3):
        try:
            now = time.time()
            stk_factor_pro = pro.stk_factor_pro(start_date=cur_date, end_date=cur_date, trade_date=cur_date,
                                                fields=fields)
            gap1 = time.time() - now
            now = time.time()

            # with lock:
            #     stk_factor_pro_all[cur_date] = stk_factor_pro
            #     count = count + 1
            #     if count % 100 == 0:
            #         write_file(filename='../rj/data/tushare/stk_factor_pro_all' + str(count) + '.pkl',
            #                    value=stk_factor_pro_all)
            #         stk_factor_pro_all.clear()

            stk_factor_pro.to_sql(name='stk_factor_pro', con=engine, if_exists='append',
                                  index=False)
            print('get_stk_factor_pro ok for ', cur_date, count, gap1, time.time() - now)
            break
        except Exception as e:
            time.sleep(1)
            print('================get_stk_factor_pro fail===========', cur_date, e)


def get_stk_factor_pro(trade_cal):
    executor = ThreadPoolExecutor(max_workers=5)
    futures = []
    for i, _ in enumerate(trade_cal):
        # add_one(trade_cal[i][0])
        future = executor.submit(add_one, trade_cal[i][0])
        futures.append(future)

    for future in as_completed(futures):
        pass


def sleep_milliseconds(milliseconds):
    start = time.time()
    end = start + milliseconds / 1000
    while time.time() < end:
        pass


if __name__ == '__main__':
    # 交易日期
    trade_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date, fields='cal_date')

    # 股票列表
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='')


    stk_factor = pro.stk_factor(ts_code='600519.SH',start_date='20170101', end_date='20240630')

    a=1