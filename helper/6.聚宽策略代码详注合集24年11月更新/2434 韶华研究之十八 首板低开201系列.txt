# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址克隆自聚宽文章：https://www.joinquant.com/post/44907
# 标题：韶华研究之十八，201系列
# 作者：韶华不负

##策略介绍
##思路，采取N天M涨停，结合人气前排，和2N天涨幅限制，再观察低开高开的收益区分
##2.10，信号收集分析后，采取201，5-2/3两种类型的优化过滤，按20日涨幅升序排序，取竞价低开的
##竞价符合条件后买入，次日开盘如果收益大于1.05卖出，否则等尾盘非停即出
##2.11 对比尾卖/五刻卖/分钟卖，尾卖最好，因为图的是再板的超额利益

##2.12，信号收集分析后，采取201，低开低涨类型，半仓轮动
##2.12，采用201低位首板低开上，次日尾盘不板卖的策略，并发布
##23/3/18,加入放量倍量过滤，回测显示20天5倍量胜率相对高效果更好，发布
##23/7/22，卖出加入量能控制，回测显示120D0.9V效果最好，发布

# 回测时间需要选择为  分钟

# 导入函数库
# 导入聚宽数据接口库，提供获取股票、基金、期货等金融产品数据的接口
from jqdata import *
# 导入聚宽策略向导库，提供策略编写过程中常用的工具和函数
# 注意：该库不能和technical_analysis库共存，可能会有冲突
from kuanke.wizard import *
# 导入six库中的BytesIO类，用于在Python 2和Python 3之间实现IO的兼容性
from six import BytesIO
# 导入聚宽技术分析库，提供一系列技术分析指标的计算
from jqlib.technical_analysis import *
# 导入聚宽因子库，提供获取因子数据的接口
from jqfactor import get_factor_values
# 导入sklearn库中的线性回归模型
from sklearn.linear_model import LinearRegression
# 导入numpy库，提供多维数组对象及一系列操作
import numpy as np
# 导入pandas库，提供高性能的数据结构和数据分析工具
import pandas as pd
# 导入time库，提供各种与时间相关的函数
import time

# 初始化函数，设定基准等等
def after_code_changed(context):
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    unschedule_all()  # 取消所有定时函数
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    set_params()    #1 设置策略参数
    set_variables() #2 设置中间变量
    set_backtest()  #3 设置回测条件

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='7:00')
      # 竞价时运行
    run_daily(call_auction, time='09:26')
      # 开盘时运行
    #run_daily(market_run, time='09:30')
    #run_daily(market_run, time='10:30')
    #run_daily(market_run, time='13:30')
      # 尾盘运行
    run_daily(market_run, time='14:55')
      # 收盘后运行
    #run_daily(after_market_close, time='20:00')
          # 收盘后运行
    #run_daily(after_market_analysis, time='21:00')

#1 设置策略参数
def set_params():
    #设置全局参数
    g.index ='all'          #all-zz-300-500-1000
    g.auction_open_highlimit = 0.985  #竞价开盘上限
    g.auction_open_lowlimit = 0.945 #竞价开盘下限
    g.profit_line = 1.05    #盘中的止盈门槛
    
    #买前量能过滤参数
    g.volume_control = 2    #0-默认不控制，1-周期放量控制,2-周期倍量控制,3,倍量控制(相对昨日),4-放量(240-0.9)加倍量(20-5)的最佳回测叠加
    g.volume_period = 20   #放量控制周期，240-120-90-60
    g.volume_ratio = 5    #放量控制和周期最高量的比值，0.9/0.8
    
    #持仓量能过滤参数
    g.sell_mode = 0     #0-默认尾盘非板卖，11-T日天量(240),12-T日倍量(相对周期)，13-T日倍量(相对D日)
    g.sell_vol_period = 120   #放量控制周期，240-120-90-60
    g.sell_vol_ratio = 0.9    #放量控制和周期最高量的比值，0.9/0.8
#2 设置中间变量
def set_variables():
    #暂时未用，测试用全池
    g.stocknum = 0              #持仓数，0-代表全取
    g.poolnum = 1*g.stocknum    #参考池数

    
#3 设置回测条件
def set_backtest():
    # 设定g.index作为基准
    if g.index == 'all':
        set_benchmark('000001.XSHG')
    else:
        set_benchmark(g.index)
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    #set_option("avoid_future_data", True)
    #显示所有列
    pd.set_option('display.max_columns', None)
    #显示所有行
    pd.set_option('display.max_rows', None)
    log.set_level('order', 'error')    # 设置报错等级
    
## 开盘前运行函数
def before_market_open(context):
    # 输出当前时间，用于记录函数的运行时间
    log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))
    # 获取当前日期和前一个交易日的日期
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    
    # 获取三天前的第一个交易日日期
    befor_date = get_trade_days(end_date=today_date, count=3)[0]
    # 获取当前所有股票的市场数据
    all_data = get_current_data()
    # 初始化全局变量，用于存储股票池和卖出列表
    g.poolist = []
    g.sell_list = []
    
    num1,num2,num3,num4,num5,num6=0,0,0,0,0,0    # 初始化用于过程追踪的变量
    
    # 构建基准指数股票池，进行去停牌、去ST、去次新股等过滤
    start_time = time.time()
    if g.index == 'all':
        # 如果选择的是全市场股票，获取所有股票代码
        stocklist = list(get_all_securities(['stock']).index)
    elif g.index == 'zz':
        # 如果选择的是中证系列指数，获取多个指数的成分股
        stocklist = get_index_stocks('000300.XSHG', date=None) + get_index_stocks('000905.XSHG', date=None) + get_index_stocks('000852.XSHG', date=None)
    else:
        # 获取指定指数的成分股
        stocklist = get_index_stocks(g.index, date=None)
    
    # 记录原始股票数量
    num1 = len(stocklist)    
    # 过滤停牌、ST、次新股等股票
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].paused]
    stocklist = [stockcode for stockcode in stocklist if not all_data[stockcode].is_st]
    stocklist = [stockcode for stockcode in stocklist if'退' not in all_data[stockcode].name]
    stocklist = [stockcode for stockcode in stocklist if stockcode[0:3] != '688']
    stocklist = [stockcode for stockcode in stocklist if (today_date-get_security_info(stockcode).start_date).days>365]
    num2 = len(stocklist)  # 记录过滤后的股票数量
    # 记录结束时间并计算构建股票池耗时
    end_time = time.time()
    print('Step0,基准%s,原始%d只,四去后共%d只,构建耗时:%.1f 秒' % (g.index,num1,num2,end_time-start_time))
    
    # 进行N天M次涨停过滤
    start_time = time.time()
    poollist = get_up_filter_jiang(context,stocklist,lastd_date,1,1,0)
    list_201 = get_up_filter_jiang(context,poollist,lastd_date,20,1,0)
    
    g.poollist = optimize_filter(context,list_201,'L') # 对过滤后的股票进行优化处理
    
    # 记录结束时间并计算过滤耗时
    end_time = time.time()
    print('Step0,N天M次涨停过滤共%d只,板型过滤共%d只,构建耗时:%.1f 秒' % (len(list_201),len(g.poollist),end_time-start_time))
    log.info(g.poollist)
    
    # 如果设置了成交量控制参数，进行天量/爆量过滤
    if g.volume_control !=0:
        g.poollist = get_highvolume_filter(context,g.poollist,g.volume_control,g.volume_period,g.volume_ratio)
        log.info(g.poollist)    
    #stock_analysis(context,list_201)
    
    # 如果设置了卖出模式，并且当前有持仓，进行卖出控制
    if g.sell_mode !=0 and len(context.portfolio.positions) !=0:
        stocklist = list(context.portfolio.positions)
        g.sell_list = get_highvolume_filter(context,stocklist,g.sell_mode,g.sell_vol_period,g.sell_vol_ratio)
        log.info('今日早盘卖出:')
        log.info(g.sell_list)

# 定义 call_auction 函数，用于在集合竞价阶段执行交易逻辑
def call_auction(context):
    log.info('函数运行时间(Call_auction)：'+str(context.current_dt.time()))
    # 获取当前市场数据
    current_data = get_current_data()
    # 获取当前日期和前一交易日的日期
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    # 初始化买入股票列表
    buy_list = []
    # 获取集合竞价阶段的数据，包括时间、当前价、成交量和成交额
    df_auction = get_call_auction(g.poollist,start_date=today_date,end_date=today_date,fields=['time','current','volume','money'])
    # 检查是否有计划卖出的股票
    if len(g.sell_list) ==0:
        log.info('今日早盘无卖信')
    else:
        # 遍历当前持仓股票
        for stockcode in context.portfolio.positions:
            # 如果股票停牌，则跳过
            if current_data[stockcode].paused == True:
                continue
            # 如果股票在卖出列表中，则执行卖出操作
            if (stockcode in g.sell_list):# and (stockcode not in g.buylist):
                sell_stock(context, stockcode,0)
    
    # 遍历集合竞价阶段的数据
    for i in range(len(df_auction)):
        stockcode = df_auction.code.values[i]
        price = df_auction.current.values[i]
        # 获取过去5天的收盘价数据
        df_price = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['close'], count=5)
        # 根据开盘价与前一日收盘价的比例判断是否买入
        if price/df_price.close[-1] <g.auction_open_highlimit and  price/df_price.close[-1] >g.auction_open_lowlimit:
            buy_list.append(stockcode)
    
    # 检查是否有计划买入的股票
    if len(buy_list) ==0:
        log.info('今日无买信')
        return
    else:
        log.info('今日买信共%d只:' % len(buy_list))
        log.info(buy_list)

    # 计算总资产和每只股票的买入金额
    total_value = context.portfolio.total_value
    buy_cash = 0.5 * total_value / len(buy_list)
    # 遍历买入股票列表，执行买入操作
    for stockcode in buy_list:
        if stockcode in list(context.portfolio.positions.keys()):
            continue
        buy_stock(context,stockcode,buy_cash)
    
    return
"""        
#按bar运行        
def handle_data(context,data):
    #log.info('函数运行时间(market_open_bar):'+str(context.current_dt.time()))
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    current_data = get_current_data()
    
    #判断票池和仓位，全为空则轮空
    if len(context.portfolio.positions) ==0:
        log.info('空仓，今天休息')
        return
    
    #判断仓位，若持仓则挂卖单
    for stockcode in context.portfolio.positions:
        if current_data[stockcode].paused == True:
            continue
        if context.portfolio.positions[stockcode].closeable_amount ==0:
            continue
        #止盈出
        cost = context.portfolio.positions[stockcode].avg_cost
        price = current_data[stockcode].last_price
        
        if price/cost > g.profit_line:
            log.info('止盈即出%s' % stockcode)
            sell_stock(context,stockcode,0)
    
    #14：55执行平仓
    hour = context.current_dt.hour
    minute = context.current_dt.minute
    if hour == 14 and minute == 55:
        for stockcode in context.portfolio.positions:
            if current_data[stockcode].paused == True:
                continue
            if context.portfolio.positions[stockcode].closeable_amount ==0:
                continue
            log.info('尾盘平出%s' % stockcode)
            sell_stock(context,stockcode,0)
"""

# 收盘时运行函数
def market_run(context):
    log.info('函数运行时间(market_close):'+str(context.current_dt.time()))
    # 获取当前日期和前一交易日的日期
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    # 获取当前市场数据
    current_data = get_current_data()
    
    # 检查当前时间，确保在收盘时执行卖出操作
    # 假设收盘时间为14:59，可以根据实际情况调整
    hour = context.current_dt.hour
    minute = context.current_dt.minute
    
    for stockcode in context.portfolio.positions:
        if current_data[stockcode].paused == True:   # 如果股票停牌，则跳过不处理
            continue
        if context.portfolio.positions[stockcode].closeable_amount ==0:  # 如果股票的可卖出数量为0（即已经全部卖出或停牌），则跳过不处理
            continue
        """
        #止盈出
        cost = context.portfolio.positions[stockcode].avg_cost
        price = current_data[stockcode].last_price
        
        if price/cost > g.profit_line and hour !=14:
            log.info('止盈即出%s' % stockcode)
            sell_stock(context,stockcode,0)
        elif hour == 14:
            log.info('尾盘即出%s' % stockcode)
            sell_stock(context,stockcode,0)
        else:
            log.info('%s留到尾盘' % stockcode)
            
        """
        # 如果股票的收盘价不等于涨停价，即股票未涨停，则执行卖出操作
        if current_data[stockcode].last_price != current_data[stockcode].high_limit:
            log.info('非涨停即出%s' % stockcode)
            sell_stock(context,stockcode,0)   # 调用 sell_stock 函数，卖出股票
            continue
        
## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    # 获取当前日期和前一交易日的日期
    today_date = context.current_dt.date()
    last_date = context.previous_date

"""
---------------------------------函数定义-主要策略-----------------------------------------------
"""
#蒋的方法，N天M涨停过滤
def get_up_filter_jiang(context,stocklist,check_date,check_duration,up_num,direction):
    # 记录函数开始运行的时间
    log.info('函数运行时间(get_up_filter_jiang)：'+str(context.current_dt.time()))
    #0，预置，今天是D日
    all_data = get_current_data()
    poollist=[]  # 初始化一个空列表，用于存放最终筛选出的股票代码
    
    # 获取基准日期往前check_duration天的所有交易日
    trd_days = get_trade_days(end_date=check_date, count=check_duration)  # array[datetime.date]
    # 将交易日转换成pandas的Series对象，方便后续操作
    s_trd_days = pd.Series(range(len(trd_days)), index=trd_days)  # Series[index:交易日期，value:第几个交易日]
    back_date = trd_days[0]
    
    #2，形态过滤，一月内两次以上涨停(盘中过10%也算)
    start_time = time.time()
     # 包括前收盘价、开盘价、收盘价、最高价、涨停价、跌停价和是否停牌
    df_price = get_price(stocklist,end_date=check_date,frequency='1d',fields=['pre_close','open','close','high','high_limit','low_limit','paused']
    ,skip_paused=False,fq='pre',count=check_duration,panel=False,fill_paused=True)
    
    # 过滤出涨停的股票，按time索引
    df_up = df_price[(df_price.close == df_price.high_limit) & (df_price.paused == 0)].set_index('time')
    # 在df_up中添加一列，表示涨停发生在哪个交易日
    df_up['ith'] = s_trd_days
    # 将涨停股票的代码放入一个集合中，方便后续去重
    code_set = set(df_up.code.values)
    # 根据direction参数确定筛选条件
    if direction ==1:
        # 筛选出涨停次数多于up_num的股票
        poollist =[stockcode for stockcode in code_set if ((len(df_up[df_up.code ==stockcode]) > up_num))]
    elif direction ==-1:
        # 筛选出涨停次数少于up_num的股票
        poollist =[stockcode for stockcode in code_set if ((len(df_up[df_up.code ==stockcode]) < up_num))]
    else:
        # 筛选出涨停次数等于up_num的股票
        poollist =[stockcode for stockcode in code_set if ((len(df_up[df_up.code ==stockcode]) == up_num))]
    # 记录函数结束的时间，并计算函数运行耗时
    end_time = time.time()
    log.info('---%d天(%s--%s)%d次涨停过滤出%d只标的,构建耗时:%.1f 秒' % (check_duration,back_date,check_date,up_num,len(poollist),end_time-start_time))        
    #log.info(poollist)

    return poollist   # 返回最终筛选出的股票列表

#对1-m板后的形态过滤，1和m是针对20-22年的过滤方案，l-是针对18-22年的一板过滤方案
#去除T日是一字/T字/尾盘封板弱
def optimize_filter(context,stocklist,filt_type):
    # 获取当前日期和前一个交易日的日期
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    # 获取当前所有股票的数据
    all_data = get_current_data()
    # 初始化一个空列表，用于存放最终筛选出的股票代码
    poollist = []
    
    # 遍历股票池中的每只股票
    for stockcode in stocklist:
        # 过滤掉一字板（开盘即涨停）和T字板（开盘一字板，盘中开板）
        df_lastd = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['open', 'close', 'high', 'high_limit', 'low_limit'], count=1)
        if (df_lastd['open'][0] == df_lastd['high_limit'][0] and df_lastd['close'][0] == df_lastd['high_limit'][0]):
            continue
        # 过滤掉尾盘封板弱的股票（即收盘价不是涨停价的股票）
        df_last30 = get_bars(stockcode, count=60, unit='1m', fields=['open','close','high','low'],include_now=True,df=True)
        if (df_last30['low'][:].min() != df_lastd['high_limit'][0]) & (df_last30['high'][:].max() == df_lastd['high_limit'][0]):
            continue
        # 获取股票的流通市值
        df_value = get_valuation(stockcode, end_date=lastd_date, count=1, fields=['circulating_market_cap']) #先新后老
        cirm_cap = df_value['circulating_market_cap'].values[0]
        # 获取股票过去20个交易日的价格和成交量数据
        df_price = get_price(stockcode,end_date=lastd_date,frequency='1d',fields=['open','close','high','low','paused','volume'],skip_paused=False,fq='pre',count=20,panel=False,fill_paused=True)
        # 计算股票过去5个交易日和20个交易日的涨幅
        change_5 = df_price['close'][-1] / df_price['close'][-5]
        change_20 = df_price['close'][-1] / df_price['close'].values.min()
        # 计算股票最近一个交易日的成交量与过去20个交易日成交量均值的比值
        vol_lvs20 = df_price['volume'][-1] / df_price['volume'].values.mean()
        # 根据 filt_type 指定的过滤条件进行筛选
        if filt_type == 1:
            # 条件1：最新价格大于8元，20日涨幅小于1.3倍，成交量比值小于4
            if all_data[stockcode].last_price >8 and change_20 <1.3 and vol_lvs20 <4:
                poollist.append(stockcode)
        elif filt_type =='M':
            # 条件M：最新价格大于15元，流通市值小于50亿元
            if all_data[stockcode].last_price >15 and cirm_cap <50:
                poollist.append(stockcode)
        elif filt_type =='L':
            # 条件L：5日涨幅小于1.1倍
            if change_5 <1.1:
                poollist.append(stockcode)
            
    return poollist    # 返回最终筛选出的股票列表

# 过滤N天内M倍最高量，X-买入前量能过滤，1X-为持仓的量能过滤
def get_highvolume_filter(context,stocklist,control_mode,check_dura,volume_ratio):
    # 获取前一个交易日的日期
    lastd_date = context.previous_date
    # 初始化一个空列表，用于存放最终筛选出的股票代码
    poollist = []
    
    for stockcode in stocklist:
        if control_mode == 1:
            # 获取过去 check_dura 天的成交量数据
            df_price = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['volume'], count=check_dura)
            # 如果最后一天的成交量大于过去成交量最大值的 volume_ratio 倍，则过滤掉
            if df_price['volume'][-1] > volume_ratio * df_price['volume'].max():
                continue
            # 将符合条件的股票加入 poollist
            poollist.append(stockcode)
        elif control_mode ==2:
            # 获取过去 check_dura 天的成交量数据
            df_price = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['volume'], count=check_dura)
            # 如果最后一天的成交量大于过去成交量均值的 volume_ratio 倍，则过滤掉
            if df_price['volume'][-1] > volume_ratio * df_price['volume'].mean():
                continue
            poollist.append(stockcode)
        elif control_mode ==3:
            # 获取过去 check_dura 天的成交量数据
            df_price = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['volume'], count=check_dura)
            # 如果最后一天的成交量大于前一个交易日成交量的 volume_ratio 倍，则过滤掉
            if df_price['volume'][-1] > volume_ratio * df_price['volume'][-2]:
                continue
            poollist.append(stockcode)
        elif control_mode ==4:
            # 获取过去 240 天的成交量数据
            df_price = get_price(stockcode, end_date=lastd_date, frequency='daily', fields=['volume'], count=240)
            # 如果最后一天的成交量大于过去成交量最大值的 90%，或者大于过去 20 天成交量均值的 5 倍，则过滤掉
            if df_price['volume'][-1] > 0.9 * df_price['volume'].max() or df_price['volume'][-1] > 5 * df_price['volume'][-20:].mean():
                continue
            poollist.append(stockcode)
        # ... (其他 control_mode 的条件类似，只是条件相反，用于筛选成交量小于某个比率的股票)
        elif control_mode ==11:
            df_price = get_price(stockcode,end_date=lastd_date,frequency='daily',fields=['volume'],count=check_dura)
            if df_price['volume'][-1] < volume_ratio*df_price['volume'].max():
                continue
            poollist.append(stockcode)
        elif control_mode ==12:
            df_price = get_price(stockcode,end_date=lastd_date,frequency='daily',fields=['volume'],count=check_dura)
            if df_price['volume'][-1] < volume_ratio*df_price['volume'].mean():
                continue
            poollist.append(stockcode)
        elif control_mode ==13:
            df_price = get_price(stockcode,end_date=lastd_date,frequency='daily',fields=['volume'],count=check_dura)
            if df_price['volume'][-1] < volume_ratio*df_price['volume'][-2]:
                continue
            poollist.append(stockcode)
        elif control_mode ==14:
            df_price = get_price(stockcode,end_date=lastd_date,frequency='daily',fields=['volume'],count=240)
            if df_price['volume'][-1] < 0.9*df_price['volume'].max():
                continue
            if df_price['volume'][-1] < 5*df_price['volume'][-20:].mean():
                continue
            poollist.append(stockcode)
    
    print('---量能控制%d-%d天放%.1f量过滤后共%d只' % (control_mode,check_dura,volume_ratio,len(poollist)))
    return poollist      # 返回最终筛选出的股票列表
"""
---------------------------------函数定义-次要过滤-----------------------------------------------
"""

"""
---------------------------------函数定义-辅助函数-----------------------------------------------
"""
##买入函数
def buy_stock(context,stockcode,cash):
    today_date = context.current_dt.date()  # 获取当前日期
    current_data = get_current_data()        # 获取当前所有股票的数据
    
    if stockcode[0:3] == '688':  # 过滤股票
        last_price = current_data[stockcode].last_price
        if order_target_value(stockcode,cash,MarketOrderStyle(1.1*last_price)) != None: #科创板需要设定限值
            log.info('%s买入%s' % (today_date,stockcode))
    else:
        if order_target_value(stockcode, cash) != None:
            log.info('%s买入%s' % (today_date,stockcode))
            
##卖出函数
def sell_stock(context,stockcode,cash):
    today_date = context.current_dt.date()  # 获取当前日期
    current_data = get_current_data()        # 获取当前所有股票的数据
    
    if stockcode[0:3] == '688':    # 过滤股票
        last_price = current_data[stockcode].last_price
        if order_target_value(stockcode,cash,MarketOrderStyle(0.9*last_price)) != None: #科创板需要设定限值
            log.info('%s卖出%s' % (today_date,stockcode))
    else:
        if order_target_value(stockcode,cash) != None:
            log.info('%s卖出%s' % (today_date,stockcode))

# 个股盘中分析信后的未来走势，带未来
def stock_analysis(context,stocklist):
    # 获取当前日期和前一个交易日的日期
    today_date = context.current_dt.date()
    lastd_date = context.previous_date
    # 获取当前所有股票的数据
    all_data = get_current_data()
    
    #pop = CYF(stocklist, check_date=lastd_date, N=20)
    for stockcode in stocklist:
        # 获取股票的名称
        stockname = get_security_info(stockcode).display_name
        # 获取股票的流通市值
        df_value = get_valuation(stockcode, end_date=lastd_date, count=1, fields=['circulating_market_cap']) #先新后老
        cirm_cap = df_value['circulating_market_cap'].values[0]
        # 获取股票过去20个交易日的价格和成交量数据
        df_price = get_price(stockcode,end_date=lastd_date,frequency='1d',fields=['open','close','high','low','paused','volume'],skip_paused=False,fq='pre',count=20,panel=False,fill_paused=True)
        # 计算5日和20日的价格变化率
        change_5 = df_price['close'][-1]/df_price['close'][-5]
        change_20 = df_price['close'][-1]/df_price['close'][0]
        change_5v20 = change_5/change_20
        # 计算最近一日和过去5日成交量的平均值的比率，以及过去5日和过去20日成交量平均值的比率
        vol_lvs5 = df_price['volume'][-1]/df_price['volume'][-5:].mean()
        vol_5vs20 = df_price['volume'][-5:].mean()/df_price['volume'].values.mean()
        # 获取最近两个交易日的资金流向数据
        df_money = get_money_flow(stockcode, end_date=lastd_date, count=2, fields=['net_amount_main'])  #先新后老
        #存在昨日负流入或为零的状况，计算资金流向的变化率
        net_main_lbvsb = (df_money['net_amount_main'].values[0]-df_money['net_amount_main'].values[-1])/df_money['net_amount_main'].values[0]
        
        # 获取未来1-5个交易日的日期列表
        fut_date_list = get_trade_days(start_date = today_date,end_date = '2022-02-11')
        # 确定未来第5个交易日和下一个交易日的日期
        if len(fut_date_list) >=5:
            fut5_date = get_trade_days(start_date= today_date)[4]
            next_date = get_trade_days(start_date= today_date)[1]
        else:
            fut5_date = get_trade_days(start_date= today_date)[-1]
            next_date = get_trade_days(start_date= today_date)[1]
        # 获取未来5个交易日的价格数据
        future5_price = get_price(stockcode, start_date=today_date, end_date=fut5_date, frequency='daily', fields=['open','close','high','low'])
        # 获取下一个交易日的价格数据
        next_price = get_price(stockcode, start_date=today_date, end_date=next_date, frequency='daily', fields=['pre_close','open','close','high','low','avg'])
        # 计算买入当天开盘价、未来两日开盘价、未来两日均价、未来两日收盘价和未来五日收盘价的比率
        price_D = next_price['open'].values[0]  #今天9:30买入
        catch = price_D/next_price['pre_close'].values[0]
        d2_open = next_price['open'].values[-1]/price_D
        d2_avg = next_price['avg'].values[-1]/next_price['avg'].values[0]
        d2_close = next_price['close'].values[-1]/price_D
        d5_close = future5_price['close'].values[-1]/price_D
        # 将分析结果写入文件
        write_file('201.csv', str('%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.3f,%.2f,%.2f,%.2f,%.2f\n' % (today_date,stockcode,stockname,cirm_cap,change_5
        ,change_20,change_5v20,vol_lvs5,vol_5vs20,net_main_lbvsb,price_D,catch,d2_open,d2_close,d2_avg,d5_close)),append = True)
