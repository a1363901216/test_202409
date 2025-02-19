# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/45110
# 标题：高增股池小市值轮动
# 作者：倚A听风雨

"""
V1.0: 
    高增股小市值轮动
V1.1: 
    4月空仓
V1.2: 
    涨停等股票过滤
    放开科创板
v1.3: 
    (废弃)PE,PEG等采用扣非净利润计算
v1.4: 
    涨停开板卖出以及卖出后一定时间内不再买入
"""
# 导入函数库
from jqdata import *  # 从jqdata模块导入所有内容
import datetime
import pandas as pd    # 将 pandas库导入当前环境，并给它指定一个别名 pd

# 初始化函数，设定基准等等
def initialize(context):
    set_benchmark('000300.XSHG')    # 设定基准
    set_option('use_real_price', True)    # 用真实价格交易
    set_option("avoid_future_data", True)    # 打开防未来函数
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    ### 股票相关设定 ###
    set_slippage(FixedSlippage(0.02))
    # 股票类每笔交易时的手续费是：买入时佣金万分之2，卖出时佣金万分之2加千分之一印花税, 每笔交易佣金最低扣0.01块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0002, close_commission=0.0002, min_commission=0.01), type='stock')
     # 持仓数量
    g.total_stock_num = 10
    # 持仓股列表
    g.hold_list = []
    # 需要买的股票列表
    g.buy_list = []
    # 昨日涨停的持仓股
    g.high_limit_list = []
    g.limit_up_list = []       # 记录持仓中涨停的股票
    g.history_hold_list = []   # 过去一段时间内持仓过的股票
    g.not_buy_again_list = []  # 最近买过且涨停过的股票一段时间内不再买入
    g.limit_days = 20          # 不再买入的时间段天数
    g.is_empty_position = False    # 是否空仓
    # 设置交易时间，每天或每周运行
    run_daily(before_market_open, time='09:25', reference_security='000300.XSHG')
    run_weekly(market_opened, weekday=1, time='09:30', reference_security='000300.XSHG')
    run_daily(check_limit_up, time='14:40', reference_security='000300.XSHG')
    run_daily(clear_account, '14:50')
## 开盘前运行函数
def before_market_open(context):
    # 显示全部
    # pd.set_option('display.max_columns',None)
    # 判断4月空仓信号
    g.is_empty_position = today_is_between(context, '04-01', '04-30')
    # 获取历史持有列表
    get_history_hold_list(context)
    # 获取昨日涨停列表
    get_yesterday_limit_up_stocks(context)
    
## 开盘时运行函数
def market_opened(context):
    yesterday = context.previous_date    # 获取前一个交易日的日期
    if g.is_empty_position == False:     # 如果g.is_empty_position为False，表示当前有持仓
        adjustment(context,yesterday)    # 调用adjustment函数进行持仓调整
## 调仓
def adjustment(context,yesterday):
    all_stocks = list(get_all_securities(types=['stock'], date=yesterday).index)
    # 过滤次新,停牌等股票
    g.buy_list = basic_filters(context,all_stocks)
    # 筛选高增股池,多5倍数量防止涨停后数量不够
    g.buy_list = get_high_growth_stocks(context,g.buy_list)[:g.total_stock_num*2]
    #过滤最近涨停过的股票
    recent_limit_up_list = get_recent_limit_up_stock(context, g.buy_list, g.limit_days)
    # 过滤最近买过且涨停的股票
    black_list = list(set(g.not_buy_again_list).intersection(set(recent_limit_up_list)))
    g.buy_list = [stock for stock in g.buy_list if stock not in black_list]
    # 过滤涨停,计算买入数量 写在下面保证性能
    g.buy_list = filter_limitup_stock(context, g.buy_list)[:min(g.total_stock_num,len(g.buy_list))]
    # 不在买入列表中则卖出
    all_positions = context.portfolio.positions
    for stock in all_positions:
        # 不在买入列表且不在昨日涨停列表则卖出
        if (stock not in g.buy_list) and (stock not in g.high_limit_list):
            # 获取当前价
            current_data = get_current_data()   # 获取当前所有股票的数据
            limit_price = current_data[stock].last_price*0.9  # 获取股票的最新成交价格并打九折
            order_target(stock,0,MarketOrderStyle(limit_price))  # 创建一个市价单的订单风格，但是指定了尝试以不超过 limit_price 的价格成交
            log.info("日常调仓卖出[%s]" % (stock))
        else:
            log.info("日常调仓,继续持有[%s]" % (stock))
    # 可用资金除以需要买入的股票数量得出每只股票的买入金额
    no_hold_target_num = g.total_stock_num - len(context.portfolio.positions)  # 表示还需要买入的股票数量
    if no_hold_target_num > 0:   # 如果还有股票需要买入，执行买入操作
        # 计算每只股票可以分配到的现金金额
        cash_per_stock = context.portfolio.available_cash / no_hold_target_num
        for stock in g.buy_list:
            if stock not in g.hold_list:   # 检查股票是否已经在持仓列表 g.hold_list 中
                current_data = get_current_data()        # 获取当前的股票市场数据
                limit_price = current_data[stock].last_price*1.1   # 设置一个买入价格上限，这里设置为当前股票的最后成交价格的110%
                order_target_value(stock, cash_per_stock,MarketOrderStyle(limit_price)) # 执行买入操作，尽可能以不高于 limit_price 的价格买入股票
## 根据财报筛选高增股票
def get_high_growth_stocks(context,stock_codes):
    yesterday = context.previous_date # 获取上个交易日的日期
    # 正式报告
    # 当季财报符合条件
    q = query(
        income.code,
        income.operating_revenue, # 营业收入
        indicator.adjusted_profit, # 扣非净利润
        valuation.pe_ratio, # PE-TTM
        valuation.market_cap, # 总市值
        valuation.circulating_market_cap # 流通市值
        ).filter(
            income.code.in_(stock_codes),
            valuation.pe_ratio <= 30,# PE-TTM 小于等于40
            valuation.pe_ratio > 0, # PE-TTM不为负,
            indicator.adjusted_profit > 0, # 当季扣非净利润不为负
            )
    now_df = get_fundamentals(q,date=yesterday)
    # 将符合条件的code转为list
    filtered_codes = now_df['code'].values.tolist()
    day = yesterday.day
    # 计算去年同期的财报日期
    if yesterday.month == 2 and yesterday.day == 29:
        day = 28
    lastyear_same_day = datetime.date(yesterday.year-1,yesterday.month,day)
    lastyear_q = query(
        income.code,
        income.operating_revenue,
        indicator.adjusted_profit,
        valuation.pe_ratio
        ).filter(
            income.code.in_(filtered_codes)
            )
    lastyear_df = get_fundamentals(lastyear_q,date=lastyear_same_day)
    # 合并两个df
    merged_df = pd.merge(now_df,lastyear_df,on = ['code'],suffixes=['','_lastyear'])
    # 营收同比
    merged_df['growth_operating_revenue'] = (merged_df['operating_revenue'] - merged_df['operating_revenue_lastyear']) / abs(merged_df['operating_revenue_lastyear'])
    # 扣非净利润同比
    merged_df['growth_adjusted_profit'] = (merged_df['adjusted_profit'] - merged_df['adjusted_profit_lastyear']) / abs(merged_df['adjusted_profit_lastyear']) 
    # 计算扣非PE-TTM
    # 即期PEG
    merged_df['peg'] = merged_df['pe_ratio'] / (merged_df['growth_adjusted_profit']*100)
    # 最终筛选
    df = merged_df.loc[(merged_df['peg']<=1)&(merged_df['growth_adjusted_profit']>0)&(merged_df['growth_operating_revenue']>=0.15),:]
    df = df.sort_values(by='market_cap')
    buy_list = list(df['code'])  # 获得股票代码列表
    return buy_list

## 获取最进N个交易日内有涨停的股票
def get_recent_limit_up_stock(context, stock_list, recent_days):
    stat_date = context.previous_date  # 获取策略运行前一天的日期
    new_list = []   # 初始化一个空列表，用于存储有过涨停记录的股票代码
    for stock in stock_list:
        # 使用get_price函数获取每只股票在最近N个交易日内的收盘价和涨停价
        df = get_price(stock, end_date=stat_date, frequency='daily', fields=['close','high_limit'], count=recent_days, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]     # 筛选出收盘价等于涨停价的记录，即股票涨停的日子
        # 如果筛选后的DataFrame不为空，说明在最近N个交易日内该股票至少有一天是涨停的
        if len(df) > 0:
            new_list.append(stock)    # 将有过涨停记录的股票代码添加到新列表中
    return new_list    # 函数返回包含有过涨停记录的股票代码的列表
## 获取最近一段时间持有的股票
def get_history_hold_list(context):
    g.hold_list= []    # 初始化全局变量g.hold_list为空列表，用于存储当前持有的股票
    # 遍历当前投资组合中所有持仓的股票
    for position in list(context.portfolio.positions.values()):
        # 获取持仓股票的代码
        stock = position.security
        # 将股票代码添加到g.hold_list列表中
        g.hold_list.append(stock)
    # 将当前持有的股票列表添加到全局变量g.history_hold_list中
    g.history_hold_list.append(g.hold_list)
    # 检查g.history_hold_list的长度是否达到或超过了g.limit_days指定的天数
    # g.limit_days是一个全局变量，表示要检查的过去天数
    if len(g.history_hold_list) >= g.limit_days:
        # 如果超过了指定的天数，则保留最近g.limit_days天的数据
        g.history_hold_list = g.history_hold_list[-g.limit_days:]
    # 初始化一个临时集合temp_set，用于存储过去一段时间内持有过的所有不同的股票
    temp_set = set()
    for hold_list in g.history_hold_list:
        # 遍历每个列表中的每只股票
        for stock in hold_list:
            # 将股票添加到临时集合temp_set中
            temp_set.add(stock)
    # 将临时集合temp_set转换为列表，并赋值给全局变量g.not_buy_again_list
    # g.not_buy_again_list用于存储过去一段时间内持有过的股票，防止短期内重复买入
    g.not_buy_again_list = list(temp_set)
## 获取昨日涨停的持仓股
def get_yesterday_limit_up_stocks(context):
     #获取昨日涨停列表
    if g.hold_list != []:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        g.high_limit_list = list(df.code)
    else:
        g.high_limit_list = []
## 检查持仓股是否涨停
def check_limit_up(context):
    now_time = context.current_dt    # 获取当前的日期和时间
    if g.high_limit_list != []:    # 检查全局变量g.high_limit_list是否不为空，这个列表包含了昨日涨停的股票
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.high_limit_list:
            # 获取当前股票的最新分钟级别的数据，包括收盘价和涨停价
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
             # 检查当前股票的收盘价是否低于涨停价的1.1倍（即判断是否打开涨停）
            if current_data.iloc[0,0] < current_data.iloc[0,1]/1.1*1.1:
                log.info("[%s]涨停打开，卖出" % (stock))    # 如果涨停打开，记录日志信息，表示准备卖出
                limit_price = current_data.iloc[0,0]*0.8    # 设置一个卖出价格限制，为当前股票的收盘价的80%
                order_target(stock,0,MarketOrderStyle(limit_price))     # 执行卖出操作，目标是将该股票的持仓数量减少到0
            else:
                log.info("[%s]涨停，继续持有" % (stock))      # 如果股票仍然处于涨停状态，记录日志信息，表示继续持有
## 基础过滤,ST,停牌股,次新等
def basic_filters(context,stock_list):
    yesterday = context.previous_date       # 获取上一交易日的日期
    current_data = get_current_data()          # 获取当前所有股票的数据
    return [stock for stock in stock_list         # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
	        if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name
			and yesterday - get_security_info(stock).start_date > datetime.timedelta(days=375)
			and not current_data[stock].paused]
## 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    current_data = get_current_data()        # 获取当前所有股票的数据
    return [stock for stock in stock_list
            if current_data[stock].last_price < current_data[stock].high_limit]
            
# 	last_prices = history(1, unit='1m', field='close', security_list=stock_list)
# 	current_data = get_current_data()
# 	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
# 			or last_prices[stock][-1] < current_data[stock].high_limit]
## 清仓
def clear_account(context):
    current_data = get_current_data()      # 获取当前所有股票的数据
    if g.is_empty_position == True:    # 检查全局变量g.is_empty_position是否为True，这个变量用于标记当前是否为空仓
        for stock in context.portfolio.positions:
            limit_price = current_data[stock].last_price*0.9      # 获取当前股票的最新成交价格，并计算出以90%的价格作为卖出限价
            # 执行卖出操作，目标是将该股票的持仓数量减少到0，使用市价单但限定了卖出价格
            # 如果市场价格低于限价，则订单不会成交
            order_target(stock,0,MarketOrderStyle(limit_price))
## 交易日判断
def today_is_between(context, start_date, end_date):
    today = context.current_dt.strftime('%m-%d')    # 获取当前的日期，格式化为月-日的形式
    # 判断当前日期是否在start_date和end_date之间
    # 首先比较当前日期是否大于等于start_date
    if (start_date <= today):
        # 然后再比较当前日期是否小于等于end_date
        return True
    else:
        return False
    