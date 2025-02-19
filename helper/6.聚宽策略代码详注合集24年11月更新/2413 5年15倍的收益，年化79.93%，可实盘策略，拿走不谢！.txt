# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/45510
# 标题：5年15倍的收益，年化79.93%，可实盘，拿走不谢！
# 作者：langcheng999

import pandas as pd   # 导入pandas库并设置别名pd
from jqdata import *  # 从jqdata模块导入所有内容
from jqfactor import get_factor_values
import redis
import json

def initialize(context):
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 设置是否开启避免未来数据模式
    set_option('avoid_future_data', True)
    # 设置基准
    set_benchmark('000300.XSHG')
    # 设置滑点
    set_slippage(FixedSlippage(0.02))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='fund')
    # strategy
    #初始化全局变量
    g.no_trading_today_signal = False    # 标记是否停止交易
    g.stock_num = 10  # 设置最大持仓股票数量
    g.choice = []  # 股票池
    g.just_sold = []  # just_sold标记本月涨停过的
    g.limit_days = 30  # 限制天数N天
    g.hold_list = []  # 已持有股票列表
    g.history_hold_list = []  # 存放N天持有过的股票，二维数组
    g.not_buy_again_list = []  # N天买过的股票,不再买入的黑名单，一维数组
    
    # 准备昨日涨停且正在持有的股票列表
    run_daily(prepare_high_limit_list, time='9:05', reference_security='000300.XSHG') 
    # 每天调整昨日涨停股票
    run_daily(check_limit_up, time='14:00') 
    # 每月最后一个交易日选股
    run_monthly(my_Trader, -1 ,time='9:30', force=True) 
    # 每月最后一个交易日调仓一次
    run_monthly(go_Trader, -1 ,time='14:55', force=True) 
    # 每天 14:30 运行
    run_daily(close_account, '14:30')
    # 收盘后运行
    # run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

# 每月选股
def my_Trader(context):
    dt_last = context.previous_date    # 获取前一天的日期
    stocks = get_all_securities('stock', dt_last).index.tolist()  # 获取上一交易日所有上市股票的代码列表
    stocks = filter_kcbj_stock(stocks)    # 过滤掉科创板及创业板股票
    #2 股息率筛选排序
    # stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.25)  
    # stocks = get_factor_filter_list(context, stocks, 'ROAEBITTTM', False, 0, 0.2)
    choice = filter_st_stock(stocks)   # 过滤掉ST股
    choice = filter_paused_stock(choice)    # 过滤掉停牌股票
    choice = filter_new_stock(context, choice)    # 过滤掉次新股
    choice = filter_limitup_stock(context,choice)    # 过滤掉涨停股票
    choice = filter_limitdown_stock(context,choice)    # 过滤掉跌停股票
    choice = filter_highprice_stock(context,choice)  # 过滤高价股，保留低价股
    choice = get_peg(context,choice)    # 基本面筛选，根据PEG值进行筛选，并根据小市值排序
    # 获取最近N个交易日内涨停过的股票列表
    recent_limit_up_list = get_recent_limit_up_stock(context, choice, g.limit_days)
    # black_list = list((set(g.not_buy_again_list).intersection(set(recent_limit_up_list))).union(set(g.just_sold)))
    black_list = list(set(g.not_buy_again_list).intersection(set(recent_limit_up_list)))    # 确定黑名单，即最近买过且涨停过的股票
    target_list = [stock for stock in choice if stock not in black_list]    # 从筛选后的股票池中进一步排除黑名单中的股票
    log.info('过滤完黑名单的数量', len(target_list))    # 记录并打印过滤完黑名单后的股票数量
    choice = target_list[:min(g.stock_num, len(target_list))]    # 根据设定的最大持仓数，截取相应数量的股票作为最终的选择
    g.choice = choice[:g.stock_num]    # 更新全局变量g.choice为最终选择的股票列表

#1-1 根据特定的量化因子对股票列表进行筛选和排序
def get_factor_filter_list(context,stock_list,jqfactor,sort,p1,p2):
    yesterday = context.previous_date    # 获取策略运行前一天的日期，用于获取历史数据
    # 使用 get_factor_values 函数获取指定股票列表和因子在前一天的值
    score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1)[jqfactor].iloc[0].tolist()
    df = pd.DataFrame(columns=['code','score'])    # 创建一个空的 DataFrame，用于存储股票代码和对应的因子得分
    # 将股票列表和对应的因子得分分别赋值给 DataFrame 的 'code' 和 'score' 列
    df['code'] = stock_list  
    df['score'] = score_list
    df = df.dropna()  # 使用 dropna 方法去除含有缺失值的行，确保数据的完整性
    df.sort_values(by='score', ascending=sort, inplace=True)    # 根据 'score' 列的值对 DataFrame 进行排序，sort 参数决定排序的方式，True 表示升序，False 表示降序
    filter_list = list(df.code)[int(p1*len(df)):int(p2*len(df))]  # 根据 p1 和 p2 两个参数确定筛选股票的范围，p1 和 p2 是筛选比例，例如 p1=0.1 和 p2=0.2 表示筛选得分从 10% 到 20% 之间的股票
    return filter_list   # 返回筛选后的股票列表

# 每月调仓一次
def go_Trader(context):
    if g.no_trading_today_signal == False:    # 检查是否设置了今日不交易的信号，如果没有则执行调仓
        # g.just_sold = [] #每月清零一次 g.just_sold 防止其中内容一直膨胀
        cdata = get_current_data()     # 获取当前市场数据
        choice = g.choice    # 获取当前选择的股票池
        # Sell，仍在选出的股票池中，则不卖
        for s in context.portfolio.positions:    # 遍历当前投资组合中的每一只股票
            if (s not in choice and (not cdata[s].paused)) :    # 如果股票不在当前选择的股票池中并且股票没有停牌，则卖出该股票
                log.info('Sell', s, cdata[s].name)     # 记录卖出信息
                order_target(s, 0)     # 下单将该股票的目标持股数设置为0，即全部卖出
                g.just_sold.append(s)     # 将卖出的股票添加到最近卖出列表中
                # 如果最近卖出列表中的股票数量超过了限制天数，则截断列表，保留最新的 g.stock_num 数量的股票
                if len(g.just_sold) >= g.limit_days:
                    g.just_sold = g.just_sold[-g.stock_num:]
        # buy，根据资金买入相应的金额
        position_count = len(context.portfolio.positions)     # 获取当前投资组合中的股票数量
        if g.stock_num > position_count:      # 如果设定的最大持仓数大于当前持仓数，则进行买入操作
            psize = context.portfolio.available_cash/(g.stock_num - position_count)    # 计算每只股票应该买入的金额，即剩余资金平均分配
            for s in choice:     # 遍历股票池中的每只股票
                if s not in context.portfolio.positions:      # 如果股票不在当前投资组合中，则买入该股票
                    log.info('buy', s, cdata[s].name)     # 记录买入信息
                    order = order_value(s, psize)         # 下单买入股票，买入金额为计算出的 psize
                    if len(context.portfolio.positions) == g.stock_num:       # 如果当前投资组合中的股票数量达到了设定的最大持仓数，则停止买入
                        break

# 获取并记录当前持仓股票的市值和股价信息
def cap(context):
    current_data = get_current_data()       # 获取当前市场数据
    hold_stocks = context.portfolio.positions.keys()    # 获取当前投资组合中所有持仓股票的列表
    for s in hold_stocks:        # 遍历持仓股票列表
        q = query(valuation).filter(valuation.code == s)    # 构建查询对象，查询特定股票的财务数据
        df = get_fundamentals(q)   # 执行查询，获取特定股票的财务数据
        # log.info(s,current_data[s].name,'流值',df['circulating_market_cap'][0],'亿')
        log.info(s,current_data[s].name,'市值',df['market_cap'][0],'亿')
        log.info(s,current_data[s].name,'股价',current_data[s].last_price,'元')
        
#2-3 获取最近N个交易日内有涨停的股票
def get_recent_limit_up_stock(context, stock_list, recent_days):
    stat_date = context.previous_date    # 获取策略运行前一天的日期，即上一个交易日
    new_list = []    # 初始化一个空列表，用于存储有涨停记录的股票代码
    for stock in stock_list:
        # 使用 get_price 函数获取每只股票在最近 recent_days 个交易日内的收盘价和涨停价
        df = get_price(stock, end_date=stat_date, frequency='daily', fields=['close','high_limit'], count=recent_days, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]    # 筛选出收盘价等于涨停价的记录，即股票涨停的日子
        if len(df) > 0:    # 如果筛选后的 DataFrame 不为空，说明该股票在最近有涨停记录，将其加入到 new_list 列表中
            new_list.append(stock)
    return new_list    # 返回包含有涨停记录的股票列表

# 基本面筛选，并根据小市值排序
def get_peg(context,stocks):
    # 获取基本面数据，创建一个查询对象 q，用于查询股票的基本面数据
    q = query(valuation.code,         # 股票代码
                valuation.pe_ratio,   # 市盈率(PE, TTM)
                indicator.inc_net_profit_year_on_year,   # 净利润同比增长率(%)
                valuation.pe_ratio / indicator.inc_net_profit_year_on_year,  # 计算 PEG 值
                indicator.roe / valuation.pb_ratio, # 收益率指标：净资产收益率ROE(%)/PB特别适合于周期类、成长性一般企业的估值分析
                indicator.roe,   #  净资产收益率ROE(%)
                indicator.roa,   # 总资产净利率ROA(%)
                valuation.pb_ratio   # 市净率(PB)
                ).filter(
                    # valuation.pe_ratio > 0,
                    # indicator.inc_net_profit_year_on_year > 0,
                    # valuation.pe_ratio / indicator.inc_net_profit_year_on_year<1,
                    # valuation.pb_ratio < 3,
                    # indicator.roe / valuation.pb_ratio > 3.2,   #国债收益率
                    indicator.roe > 0.15,
                    indicator.roa > 0.10,
                    valuation.code.in_(stocks))
    df_fundamentals = get_fundamentals(q, date = None)   # 执行查询，获取基本面数据
    stocks = list(df_fundamentals.code)    # 从查询结果中提取股票代码列表
    # 再次使用 get_fundamentals 函数查询这些股票的市值信息，并按照市值进行升序排序
    df = get_fundamentals(query(valuation.code).filter(valuation.code.in_(stocks)).order_by(valuation.market_cap.asc()))
    choice = list(df.code)    # 从查询结果中提取股票代码列表作为最终选择的股票
    return choice   # 返回最终选择的股票列表

#1-1 根据最近一年分红除以当前总市值计算股息率并筛选排序    
def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date    # 获取策略运行前一天的日期，作为查询的结束日期
    time0 = time1 - datetime.timedelta(days=365)    # 计算一年前的日期，用于查询分红数据的时间范围
    # 获取分红数据，由于finance.run_query最多返回4000行，以防未来数据超限，最好把stock_list拆分后查询再组合
    interval = 1000  # 某只股票可能一年内多次分红，导致其所占行数大于1，所以interval不要取满4000
    list_len = len(stock_list)   # 获取股票列表的长度
    #截取不超过interval的列表并查询
    q = query(                # 构建查询对象 q，查询股票的分红数据
        finance.STK_XR_XD.code,   # 股票代码
        finance.STK_XR_XD.a_registration_date,   # 股权登记日
        finance.STK_XR_XD.bonus_amount_rmb  # 每股分红金额（人民币）
    ).filter(
        finance.STK_XR_XD.a_registration_date >= time0,  # 股权登记日在一年前的日期之后
        finance.STK_XR_XD.a_registration_date <= time1,  # 股权登记日在结束日期之前
        finance.STK_XR_XD.code.in_(stock_list[:min(list_len, interval)]))  # 股票代码在传入的股票列表中
    df = finance.run_query(q)    # 执行查询，获取分红数据
    #对interval的部分分别查询并拼接
    if list_len > interval:
        df_num = list_len // interval       # 计算需要查询的批次数
        for i in range(df_num):       # 遍历每个批次，构建查询对象并执行查询
            q = query(
                finance.STK_XR_XD.code,
                finance.STK_XR_XD.a_registration_date,
                finance.STK_XR_XD.bonus_amount_rmb
            ).filter(
                finance.STK_XR_XD.a_registration_date >= time0,
                finance.STK_XR_XD.a_registration_date <= time1,
                finance.STK_XR_XD.code.in_(stock_list[interval*(i+1):min(list_len,interval*(i+2))]))
            temp_df = finance.run_query(q)
            df = df.append(temp_df)      # 将每次查询的结果合并到 df 中
    dividend = df.fillna(0)    # 对分红数据进行处理，填补缺失值为 0
    dividend = dividend.set_index('code')    # 设置股票代码为索引
    dividend = dividend.groupby('code').sum()    # 按股票代码分组并求和，得到每只股票的总分红金额
    temp_list = list(dividend.index)     # 获取有分红信息的股票列表
    #获取市值相关数据
    q = query(valuation.code,valuation.market_cap).filter(valuation.code.in_(temp_list))    # 构建查询对象，查询有分红信息的股票的市值数据
    cap = get_fundamentals(q, date=time1)    # 执行查询，获取市值数据
    cap = cap.set_index('code')    # 设置股票代码为索引
    # 合并分红数据和市值数据，计算股息率
    DR = pd.concat([dividend, cap] ,axis=1, sort=False)
    DR['dividend_ratio'] = (DR['bonus_amount_rmb']/10000) / DR['market_cap']
    DR = DR.sort_values(by=['dividend_ratio'], ascending=sort)      # 根据股息率进行排序
    final_list = list(DR.index)[int(p1*len(DR)):int(p2*len(DR))]    # 根据给定的比例 p1 和 p2 筛选股票列表
    return final_list    # 返回筛选后的股票列表
    
# 准备昨日涨停且正在持有的股票列表
def prepare_high_limit_list(context):
    g.high_limit_list = []    # 初始化全局变量 high_limit_list 为空列表，用于存储昨日涨停的股票
    hold_list = list(context.portfolio.positions)    # 获取当前投资组合中所有持仓的股票列表
    if hold_list:    # 如果当前有持仓股票
        df = get_price(hold_list, end_date=context.previous_date, frequency='daily',     # 获取这些股票在上一交易日的收盘价和涨停价
                       fields=['close', 'high_limit'],
                       count=1, panel=False)
        g.high_limit_list = df[df['close'] == df['high_limit']]['code'].tolist()        # 筛选出上一交易日收盘价等于涨停价的股票，即昨日涨停的股票
    g.no_trading_today_signal =  False    # 判断今天是否为账户资金再平衡的日期，空仓期一个月
    # g.no_trading_today_signal = today_is_between(context, '04-01', '04-30')
    g.hold_list= []    # 初始化全局变量 hold_list 为空列表，用于存储当前持有的股票
    for position in list(context.portfolio.positions.values()):    # 遍历当前投资组合中的每一只股票
        # 获取股票的代码，并添加到 hold_list 列表中
        stock = position.security
        g.hold_list.append(stock)
    g.history_hold_list.append(g.hold_list)    # 将当前持有的股票列表添加到 history_hold_list 中，用于跟踪历史持仓
    if len(g.history_hold_list) >= g.limit_days:    # 如果 history_hold_list 中的列表数量超过了限制天数 g.limit_days
        g.history_hold_list = g.history_hold_list[-g.limit_days:]    # 保留最近 g.limit_days 天的持仓记录
    temp_set = set()   # 初始化一个空集合 temp_set，用于存储最近一段时间内持有过的股票
    for hold_list in g.history_hold_list:    # 遍历 history_hold_list 中的每一个持仓列表
        for stock in hold_list:    # 遍历列表中的每只股票，并将其添加到 temp_set 集合中
            temp_set.add(stock)
    g.not_buy_again_list = list(temp_set)    # 将 temp_set 集合转换为列表，并赋值给全局变量 not_buy_again_list

#  调整昨日涨停股票
def check_limit_up(context):
    if g.no_trading_today_signal == False:    # 检查是否设置了今日不交易的信号，如果没有则执行以下逻辑
        current_data = get_current_data()        # 获取当前市场数据
        if g.high_limit_list:       # 如果存在昨日涨停的股票
            for stock in g.high_limit_list:     # 遍历昨日涨停的股票列表
                # 涨停的票，涨不动了就卖掉
                if current_data[stock].last_price < current_data[stock].high_limit:      # 检查当前股票的最新价格是否低于涨停价
                    order_target(stock, 0)       # 涨停打开，执行卖出操作
                    log.info("[%s]涨停打开，卖出" % stock)     # 记录卖出操作的日志信息
                    g.just_sold.append(stock)        # 将该股票添加到本月涨停过的股票列表中
                    if len(g.just_sold) >= g.limit_days:      # 如果本月涨停过的股票数量超过了限制天数
                        g.just_sold = g.just_sold[-g.stock_num:]      # 保留最近 g.stock_num 数量的股票记录
                else:
                    log.info("[%s]涨停，继续持有" % stock)  # 如果当前股票的最新价格等于涨停价，说明仍然在涨停状态，继续持有
        position_count = len(context.portfolio.positions)      # 获取当前投资组合中的股票数量
        # 当持有股票数量不足时：
        if g.stock_num > position_count and position_count != 0: # 如果设定的最大持仓数大于当前持仓数，并且当前持仓数不为0
            my_Trader(context) # 每月的选股逻辑，调用选股逻辑函数 my_Trader，更新股票选择列表 g.choice
            cdata = get_current_data()   # 获取当前市场数据
            psize = context.portfolio.available_cash/(g.stock_num - position_count)    # 计算每只股票应该买入的金额
            for s in g.choice:       # 遍历股票选择列表 g.choice
                if s not in context.portfolio.positions:     # 如果股票不在当前投资组合中，则买入该股票
                    order = order_value(s, psize)      # 下单买入股票，买入金额为计算出的 psize
                    if len(context.portfolio.positions) == g.stock_num:   # 如果当前投资组合中的股票数量达到了设定的最大持仓数，则停止买
                        break

# 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

# 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

# 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()       # 获取当前所有股票的数据
	return [stock for stock in stock_list   # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
			if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name]

#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过250日的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=250)]

# 过滤涨幅过大的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)    # 获取最近一分钟的历史收盘价数据
	current_data = get_current_data()    # 获取当前市场数据
	# 遍历股票列表，筛选出不在当前持仓中或者最近一分钟的收盘价低于今日涨停价97%（即考虑3%的涨停幅度）的股票
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < current_data[stock].high_limit*0.97]

# 过滤跌幅过大的股票
def filter_limitdown_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)    # 获取最近一分钟的历史收盘价数据
	current_data = get_current_data()    # 获取当前市场数据
	# 遍历股票列表，筛选出不在当前持仓中或者最近一分钟的收盘价高于今日跌停价104%（即考虑4%的跌停幅度）的股票
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] > current_data[stock].low_limit*1.04]

#2-4 过滤股价高于10元的股票	
def filter_highprice_stock(context,stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)    # 获取最近一分钟的历史收盘价数据
	# 遍历股票列表，筛选出不在当前持仓中或者最近一分钟的收盘价低于10元的股票
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < 10]

# 每个交易日结束后执行
def after_market_close(context):
    log.info(str(context.current_dt))    # 打印当前的日期和时间信息
    
#4-2 如果no_trading_today_signal为True，则清仓
def close_account(context):
    if g.no_trading_today_signal == True:          # 检查全局变量 no_trading_today_signal 是否为 True，该变量用于标记今天是否应该进行交易
        position_count = context.portfolio.positions       # 获取当前投资组合中所有持仓的股票
        if len(position_count) != 0:     # 如果当前有持仓（即持仓数量不为0）
            for stock in position_count:    # 遍历所有持仓的股票
                position = context.portfolio.positions[stock]     # 获取每只股票的具体持仓信息
                close_position(position)      # 调用 close_position 函数尝试平仓该股票
                log.info("卖出[%s]" % (stock))       # 记录日志信息，显示卖出的股票代码

#3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:   # 检查目标价值是否为0，即是否需要清仓
        log.debug("Selling out %s" % (security)) # 日志记录清仓信息
    else:
        log.debug("Order %s to value %f" % (security, value)) # 日志记录下单到特定价值的信息
    return order_target_value(security, value)   # 调用聚宽平台的order_target_value函数执行交易

#3-2 交易模块-开仓
def open_position(security, value):
    order = order_target_value_(security, value)  # 尝试对指定股票进行下单到特定价值
    if order != None and order.filled > 0:        # 检查订单是否创建成功且有成交
        return True  # 开仓成功
    return False  # 开仓失败
                
#3-3 交易模块-平仓
def close_position(position):
    security = position.security  # 获取持仓的股票代码
    order = order_target_value_(security, 0)  # 下单清仓 ；可能会因停牌失败
    if order != None:  # 检查订单是否创建成功
        if order.status == OrderStatus.held and order.filled == order.amount:  # 检查订单状态是否为全部成交且订单数量等于下单数量
            return True  # 平仓成功
    return False  # 平仓失败
    
#4-1 判断今天是否为账户资金再平衡的日期
def today_is_between(context, start_date, end_date):
    today = context.current_dt.strftime('%m-%d')    # 获取当前的日期，格式化为月-日的形式
    if (start_date <= today) and (today <= end_date):    # 检查当前日期是否在 start_date 和 end_date 之间
        return True      # 如果当前日期在范围内，返回 True
    else:
        return False      # 如果当前日期不在范围内，返回 False
