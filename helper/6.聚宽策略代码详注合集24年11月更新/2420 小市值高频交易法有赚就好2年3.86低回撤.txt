# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/43965
# 标题：有赚就好-小市值剥头皮改进 2年3.86%回撤
# 作者：CMA

import talib            # 导入talib库
import numpy as np
import pandas as pd     # 将 pandas库导入当前环境，并给它指定一个别名 pd

def initialize(context):
    set_option('use_real_price', True)    # 用真实价格交易
    set_option("avoid_future_data", True)    # 打开防未来函数
    set_slippage(FixedSlippage(0.02))      # 将滑点设置为0.02
    # 设置交易成本，包括买入成本（万分之三）、卖出成本（万分之十三）和最小交易成本（5元）
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    set_benchmark('399303.XSHE')  # 设定基准
    g.choice = 500                      # 用于后续定义股票选择的数量
    g.amount = 7                        # 用于定义每次交易的数量或金额
    g.muster = []                       # 用于存储某种筛选后的股票列表
    g.bucket = []                       # 用于存储另一种筛选后的股票列表
    g.summit = {}                       # 用于存储策略运行中的某些统计数据
    log.set_level('order', 'warning')  # 设置日志级别为warning，这意味着只有警告及以上级别的日志会被记录和显示
    # 设置交易时间，每天运行
    run_daily(buy, time='9:30', reference_security='399303.XSHE')
    run_daily(sell, time='10:30', reference_security='399303.XSHE')

#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()         # 获取当前所有股票的数据
	return [stock for stock in stock_list     # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
			if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name]

# 2-3 过滤科创北交创业股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板，北交所和创业板股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68' or stock[:2] == '30':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

#2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()  # 使用列表推导式，返回不在涨停状态或已持有的股票列表
			or last_prices[stock][-1] < current_data[stock].high_limit]

#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
	current_data = get_current_data() # 获取当前所有股票的数据
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()   # 使用列表推导式，返回不在跌停状态或已持有的股票列表
			or last_prices[stock][-1] > current_data[stock].low_limit]

#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过375日股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=375)]

# 定义 before_trading_start 函数，该函数在每个交易日开始前运行
def before_trading_start(context):
    log.info('------------------------------------------------------------')
    # 使用 get_fundamentals 函数获取市值数据，并按照市值升序排序，只获取市值最小的 g.choice 数量的股票
    # valuation.code 获取股票代码，valuation.market_cap 获取股票的市值
    fundamentals_data = get_fundamentals(query(valuation.code, valuation.market_cap).order_by(valuation.market_cap.asc()).limit(g.choice))
    stocks = list(fundamentals_data['code'])    # 从获取的财务数据中提取股票代码列表
    current_data = get_current_data()    # 获取当前的股票数据，包括价格、涨跌幅等信息
    # 筛选股票：1. 非停牌股票；2. 非ST股票；3. 股票名称中不包含'*'；4. 股票名称中不包含'退'；5. 当日有正常交易区间
    g.muster = [s for s in stocks if not current_data[s].paused and not current_data[s].is_st and 'ST' not in current_data[s].name and '*' not in current_data[s].name and '退' not in current_data[s].name and current_data[s].low_limit < current_data[s].day_open < current_data[s].high_limit]
    g.muster = filter_paused_stock(g.muster)    # 调用自定义函数 filter_paused_stock 过滤停牌股票
    g.muster = filter_st_stock(g.muster)    # 调用自定义函数 filter_st_stock 过滤ST股票
    g.muster = filter_kcbj_stock(g.muster)    # 调用自定义函数 filter_kcbj_stock 过滤科创板股票
    g.muster = filter_limitup_stock(context, g.muster)    # 调用自定义函数 filter_limitup_stock 过滤涨停股票
    g.muster = filter_limitdown_stock(context, g.muster)    # 调用自定义函数 filter_limitdown_stock 过滤跌停股票

# 定义 sell 函数，用于卖出当前持有的所有股票
def sell(context):
    data_today = get_current_data()    # 获取当前交易日的实时数据
    for s in context.portfolio.positions:    # 遍历当前投资组合中持有的所有股票
        print("sell:"+s)       # 打印将要卖出的股票代码
        order_target(s, 0)        # 使用 order_target 函数将指定股票的持仓目标数量设置为 0，即全部卖出

# 定义 buy 函数，用于执行买入股票的操作
def buy(context):
    data_today = get_current_data()    # 获取当前交易日的实时数据
    available_slots = g.amount - len(context.portfolio.positions)    # 计算可用的仓位数量，由全局变量 g.amount 指定的总仓位减去当前持有的仓位数量
    if available_slots <= 0:     # 如果没有可用的仓位，则打印信息并返回
        print("no position")
        return
    allocation = context.portfolio.cash / available_slots  # 计算每个仓位可以分配的资金，即总资金除以可用的仓位数量
    for s in g.muster:  # 遍历全局变量 g.muster 中的股票列表，该列表包含了待买入的股票
        if len(context.portfolio.positions) == g.amount:     # 如果当前持有的仓位数量已经达到了 g.amount 指定的总仓位，则停止买入
            break
        if (history(5, '1d', 'paused', s).max().values[0] == 0):     # 获取最近5天的停牌数据，如果存在停牌则 max().values[0] 不为 0
            # 过滤A杀（A杀通常指股票快速下跌）
            low = history(4, '1d', 'low', s).min().values[0]    # 获取近4天的最低价
            high = history(4, '1d', 'high', s).max().values[0]  # 获取近4天的最高价
            precent = (high - low)/low*100    # 计算近4天的价格波动百分比
            if(precent<=10):    # 如果价格波动百分比小于等于10%，则认为股票没有A杀
                open_price_today = data_today[s].day_open     # 获取今天的开盘价
                prev_close = get_price(s, count=1, end_date=context.previous_date).iloc[-1]['close']   # 获取昨天的收盘价
                his = history(60, '1d', 'close', s)    # 获取过去60天的收盘价数据
                ema = talib.EMA(his.values.flatten(), timeperiod=5)[-1]    # 计算过去60天收盘价的5日指数移动平均（EMA）
                if(prev_close > ema):      # 如果昨天的收盘价高于5日EMA，则认为股票表现较好
                    # 检查昨天的最低价是否高于今天的开盘价
                    if (get_price(s, count=1, end_date=context.previous_date).iloc[-1]['low'] > open_price_today):
                        order(s, int(allocation/open_price_today))         # 执行买入操作，买入数量为分配的资金除以今天的开盘价
