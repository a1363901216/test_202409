# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/44880
# 标题：5年12倍-小市值
# 作者：道尘

# 导入聚宽（JoinQuant）数据模块，提供获取股票、基金、期货等金融产品数据的接口
from jqdata import *
# 导入jqfactor库中的get_factor_values函数，用于获取因子数据
from jqfactor import get_factor_values
# 导入jqlib库中的技术分析模块，提供各种技术分析指标的计算
from jqlib.technical_analysis import *
# 导入NumPy库，一个广泛使用的科学计算库，提供了大量的数学函数和操作
import numpy as np
# 导入Pandas库，一个强大的数据处理和分析工具库，提供了DataFrame和Series等数据结构
import pandas as pd
# 导入statsmodels库，一个统计模型库，用于进行统计分析和建模
import statsmodels.api as sm
# 导入datetime库，提供日期和时间处理的功能
import datetime as dt

#初始化函数 
def initialize(context):
    # 设定基准
    set_benchmark('000905.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 交易量限制
    set_option('order_volume_ratio', 1)
    # 将滑点设置为0，不同滑点影响可在归因分析中查看
    set_slippage(PriceRelatedSlippage(0.002),type='stock')
    # 设置交易成本万一免五
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0001, close_commission=0.0001, close_today_commission=0, min_commission=0.1),type='fund')
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    #初始化全局变量
    g.stock_num = 9
    g.limit_up_list = [] #记录持仓中涨停的股票
    g.hold_list = [] #当前持仓的全部股票
    g.history_hold_list = [] #过去一段时间内持仓过的股票
    g.not_buy_again_list = [] #最近买过且涨停过的股票一段时间内不再买入
    g.limit_days = 10 #不再买入的时间段天数
    g.target_list = [] #开盘前预操作股票池
    # 设置策略每天开盘前9:05运行 prepare_stock_list 函数，用于准备股票池
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # 设置策略每周的第一个交易日开盘前9:30运行 weekly_adjustment 函数，用于进行周度调仓
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')
    # 设置策略每天14:00运行 check_limit_up 函数，用于检查持仓中是否有涨停股，是否需要卖出
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG') #检查持仓中的涨停股是否需要卖出
    # 设置策略每天收盘后15:10运行 print_position_info 函数，用于打印持仓信息
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')

def after_code_changed(context):
    unschedule_all()      # 取消所有之前设置的定时运行任务
    # 重新设置策略每天开盘前9:05运行 prepare_stock_list 函数，用于准备股票池
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # 重新设置策略每周的第一个交易日开盘前9:30运行 weekly_adjustment 函数，用于进行周度调仓
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')
    # 重新设置策略每天14:00运行 check_limit_up 函数，用于检查持仓中是否有涨停股，是否需要卖出
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')
    # 重新设置策略每天收盘后15:10运行 print_position_info 函数，用于打印持仓信息
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')

# 1-1 选股模块
# 定义函数 get_factor_filter_list，用于根据因子得分筛选股票
# sort: 排序方式，True 表示升序（得分低的股票在前），False 表示降序（得分高的股票在前）
# p1: 筛选区间的起始比例，如 0.1 表示从得分最低的 10% 开始筛选, p2: 筛选区间的结束比例，如 0.9 表示到得分最高的 90% 结束筛选
def get_factor_filter_list(context,stock_list,jqfactor,sort,p1,p2):
    # 获取前一个交易日的日期
    yesterday = context.previous_date
    # 使用 get_factor_values 函数获取指定股票列表和因子在前一个交易日的值
    # 结果是一个 DataFrame，其中索引是股票代码，列是因子值
    score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1)[jqfactor].iloc[0].tolist()
    # 创建一个 DataFrame 用于存储股票代码和对应的因子得分
    df = pd.DataFrame(columns=['code', 'score'])
    df['code'] = stock_list
    df['score'] = score_list
    # 移除 DataFrame 中含有 NaN 值的行
    df = df.dropna()
    # 根据因子得分对 DataFrame 进行排序, ascending=sort 确定是升序还是降序，得分低的股票在前表示升序，得分高的股票在前表示降序
    df.sort_values(by='score', ascending=sort, inplace=True)
    # 根据比例 p1 和 p2 确定筛选区间，并获取该区间内的股票列表
    # int(p1*len(stock_list)) 获取起始位置的索引，int(p2*len(stock_list)) 获取结束位置的索引
    filter_list = list(df.code)[int(p1*len(stock_list)):int(p2*len(stock_list))]
    return filter_list

# 1-2 选股模块
def get_stock_list(context):
    yesterday = str(context.previous_date)     # 获取前一个交易日的日期字符串
    # 获取所有上市股票，并与热门行业股票进行交集，得到初始股票列表
    initial_list = list(set(get_all_securities().index) & set(get_hot_industry_stock(context)))
    # initial_list = get_all_securities().index.tolist()
    # 过滤掉新上市的股票
    initial_list = filter_new_stock(context, initial_list)
    # 过滤掉科创板股票
    initial_list = filter_kcb_stock(context, initial_list)
    # 过滤掉ST股票
    initial_list = filter_st_stock(initial_list)
    # SG 5年营业收入增长率筛选
    sg_list = get_factor_filter_list(context, initial_list, 'sales_growth', False, 0, 0.1)
    # 查询这些股票的市值和每股收益
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(sg_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday)
    # 过滤掉每股收益为负的股票
    df = df[df['eps']>0]
    sg_list = list(df.code)
    # MS（综合成长因子）筛选
    factor_values = get_factor_values(initial_list, [
        'operating_revenue_growth_rate', #营业收入增长率
        'total_profit_growth_rate', #利润总额增长率
        'net_profit_growth_rate', #净利润增长率
        'earnings_growth', #5年盈利增长率
        ], end_date=yesterday, count=1)
    # 创建DataFrame来存储因子值和计算总得分
    df = pd.DataFrame(index=initial_list, columns=factor_values.keys())
    df['operating_revenue_growth_rate'] = list(factor_values['operating_revenue_growth_rate'].T.iloc[:,0])
    df['total_profit_growth_rate'] = list(factor_values['total_profit_growth_rate'].T.iloc[:,0])
    df['net_profit_growth_rate'] = list(factor_values['net_profit_growth_rate'].T.iloc[:,0])
    df['earnings_growth'] = list(factor_values['earnings_growth'].T.iloc[:,0])
    # 计算综合成长得分
    df['total_score'] = 0.1*df['operating_revenue_growth_rate'] + 0.35*df['total_profit_growth_rate'] + 0.15*df['net_profit_growth_rate'] + 0.4*df['earnings_growth']
    # 根据综合得分降序排序
    df = df.sort_values(by=['total_score'], ascending=False)
    complex_growth_list = list(df.index)[:int(0.1*len(list(df.index)))]
    # 查询这些股票的市值和每股收益
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(complex_growth_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)
    df = df[df['eps']>0]
    ms_list = list(df.code)
    # PEG（市盈率相对盈利增长比）筛选
    peg_list = get_factor_filter_list(context, initial_list, 'PEG', True, 0, 0.2)
    # 换手率波动性筛选
    turnover_list = get_factor_filter_list(context, peg_list, 'turnover_volatility', True, 0, 0.5)
    # 查询这些股票的市值和每股收益
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(turnover_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday)
    peg_list = list(df.code)
    # 将三个策略选出的股票列表合并
    final_list = [sg_list, ms_list, peg_list]
    return final_list

# 1-3 准备股票池
def prepare_stock_list(context):
    yesterday = context.previous_date  # 获取前一个交易日的日期
    #获取已持有列表
    g.hold_list= []
    for position in list(context.portfolio.positions.values()):
        # 提取每只股票的代码
        stock = position.security
        # 将股票代码添加到持有列表中
        g.hold_list.append(stock)
    # 将当前持有的股票列表添加到历史持有列表的记录中
    g.history_hold_list.append(g.hold_list)
    
    # 如果历史持有列表的记录天数超过了设定的限制天数 g.limit_days
    # 则保留最近 g.limit_days 天的记录
    if len(g.history_hold_list) >= g.limit_days:
        g.history_hold_list = g.history_hold_list[-g.limit_days:]
    temp_set = set()   # 创建一个临时集合用于存储历史上曾经持有过的股票
    # 遍历历史持有列表中的每一个持有列表
    for hold_list in g.history_hold_list:
        # 将每个持有列表中的股票添加到临时集合中
        for stock in hold_list:
            temp_set.add(stock)
    # 将临时集合转换为列表，并赋值给全局变量 g.not_buy_again_list
    # 这个列表用于记录那些在历史中曾经持有过，短期内不再考虑买入的股票
    g.not_buy_again_list = list(temp_set)
    # 如果当前持有列表不为空，即持有至少一只股票
    if g.hold_list != []:
        # 获取这些股票在前一个交易日的收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        # 筛选出那些收盘价等于涨停价的股票，即前一交易日涨停的股票
        df = df[df['close'] == df['high_limit']]
        # 将涨停股票的代码列表赋值给全局变量 g.high_limit_list
        g.high_limit_list = list(df.code)
    else:
        g.high_limit_list = []  # 如果当前没有持有任何股票，则涨停列表为空
    # 如果涨停列表不为空，则记录涨停股票的信息
    if len(g.high_limit_list):
        log.info("昨日涨停:[%s]" % (g.high_limit_list))
        
    '''
    # 每日潜池变化提醒
    get_stock_prepare_list = get_stock_list(context)
    sg_list = get_stock_prepare_list[0][:5]
    ms_list = get_stock_prepare_list[1][:5]
    peg_list = get_stock_prepare_list[2][:5]
    union_list = list(set(sg_list).union(set(ms_list)).union(set(peg_list)))
    q = query(valuation.code,valuation.circulating_market_cap).filter(valuation.code.in_(union_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)
    get_prepare_list = list(df.code)
    for stock in get_prepare_list:
        stock_market_cap = get_factor_values(stock,'circulating_market_cap', end_date=yesterday, count=1)['circulating_market_cap'].iloc[0]
        stock_name = get_security_info(stock, date=yesterday).display_name
        log.info("潜力股:%s,代码:%s,流通市值:%f" ,stock_name, stock, stock_market_cap/100_000_000)
'''

# 1-5 执行周度调仓操作
def weekly_adjustment(context):
    # 获取股票池列表，该列表由不同策略筛选出的股票组成
    all_list = get_stock_list(context)
    # 从不同策略筛选出的股票中各取前5个作为初步买入候选
    sg_list = all_list[0][:5]  # 策略1选出的股票
    ms_list = all_list[1][:5]  # 策略2选出的股票
    peg_list = all_list[2][:5]  # 策略3选出的股票
    # 将三个策略选出的股票合并，并去重
    union_list = list(set(sg_list).union(set(ms_list)).union(set(peg_list)))
    # 查询合并后股票列表的流通市值，并按市值升序排列
    q = query(valuation.code,valuation.circulating_market_cap).filter(valuation.code.in_(union_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)
    # 将查询结果的股票代码列表作为目标持仓列表
    g.target_list = list(df.code)
    # 过滤掉停牌的股票
    g.target_list = filter_paused_stock(g.target_list)
    # 过滤掉涨停的股票
    g.target_list = filter_limitup_stock(context, g.target_list)
    # 过滤掉跌停的股票
    g.target_list = filter_limitdown_stock(context, g.target_list)
    #过滤最近买过且涨停过的股票
    recent_limit_up_list = get_recent_limit_up_stock(context, g.target_list, g.limit_days)
    black_list = list(set(g.not_buy_again_list).intersection(set(recent_limit_up_list)))
    g.target_list = [stock for stock in g.target_list if stock not in black_list]
    # 根据设定的最大持仓数截取目标股票列表
    g.target_list = g.target_list[:min(g.stock_num, len(g.target_list))]
    #调仓卖出
    for stock in g.hold_list:
        if (stock not in g.target_list) and (stock not in g.high_limit_list):
            log.info("卖出[%s]" % (stock))
            position = context.portfolio.positions[stock]
            close_position(position)
        else:
            log.info("已持有[%s]" % (stock))
    # 执行调仓卖出操作
    position_count = len(context.portfolio.positions)
    target_num = len(g.target_list)
    if target_num > position_count:
        # 计算每只股票的买入金额
        value = context.portfolio.cash / (target_num - position_count)
        for stock in g.target_list:
            # 如果股票不在当前持仓中，尝试买入
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    # 如果买入后持仓数达到目标持仓数，停止买入
                    if len(context.portfolio.positions) == target_num:
                        break

#1-6 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt  # 获取当前时间
    if g.high_limit_list != []:   # 检查昨日涨停股票列表是否为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.high_limit_list:  # 遍历昨日涨停股票列表
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0,0] < current_data.iloc[0,1]:    # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))
                position = context.portfolio.positions[stock]
                close_position(position)
            else:
                log.info("[%s]涨停，继续持有" % (stock))    # 如果仍然涨停，则记录继续持有的信息

#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()       # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]    # 使用列表推导式，返回不在停牌状态的股票列表

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()       # 获取当前所有股票的数据
	return [stock for stock in stock_list   # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
			if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name]

#2-3 获取最近N个交易日内有涨停的股票
def get_recent_limit_up_stock(context, stock_list, recent_days):
    stat_date = context.previous_date  # 获取前一个交易日的日期
    new_list = []  # 初始化一个空列表，用于存储筛选出的有涨停记录的股票代码
    for stock in stock_list:  # 遍历传入的股票列表
        # 获取每只股票在最近N个交易日的收盘价和涨停价
        df = get_price(stock, end_date=stat_date, frequency='daily', fields=['close','high_limit'], count=recent_days, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]  # 筛选出收盘价等于涨停价的行，即股票发生涨停的交易日
        if len(df) > 0:             # 如果存在股票涨停的记录（即筛选后的 DataFrame 不为空）
            new_list.append(stock)  # 将该股票代码添加到筛选结果列表中
    return new_list                 # 返回筛选出的有涨停记录的股票列表

#2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)   # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	# 使用列表推导式，返回不在涨停状态或已持有的股票列表
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < current_data[stock].high_limit]

#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
    current_data = get_current_data() # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() # 使用列表推导式，返回不在跌停状态或已持有的股票列表
            or last_prices[stock][-1] > current_data[stock].low_limit]

#2-6 过滤科创板
def filter_kcb_stock(context, stock_list):
    return [stock for stock in stock_list  if stock[0:3] != '688']

#2-7 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过375日的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=375)]

# 2-8 筛选热门行业成分股
def get_hot_industry_stock(context, count=10, number=24):
    # 获取前一个交易日的日期
    end_date = context.previous_date  # 获取昨日日期
    # 获取前 count+20 个交易日的日期列表，并取第一个日期
    by_date = get_trade_days(end_date=end_date, count=count+20)[0]
    # 获取在 count+20 个交易日之前就已经上市的所有股票列表
    stock_list = get_all_securities(date=by_date).index.tolist()
    # 获取这些股票在前 count+20 个交易日内的收盘价，并按时间、股票代码进行整理
    df_close = get_price(stock_list,end_date=end_date, count=count+20, fields='close', panel=False).pivot(index='time', values='close', columns='code')
    # 计算收盘价在过去 20 个交易日的均值，然后判断每个收盘价是否大于均值
    df_bias = df_close.iloc[20:] > df_close.rolling(20).mean().iloc[20:]
    # 获取申万一级行业分类
    df_industries = get_industries('sw_l1', date=end_date)
    # 将行业分类 DataFrame 的索引（行业代码）转换为列表
    df_industries['code'] = list(df_industries.index)
    # 初始化一个空的 DataFrame 用于存储每个行业的热门股票比例
    df = pd.DataFrame()
    # 获取所有股票代码的集合
    columns = set(df_bias.columns)
    for idx, row in df_industries.iterrows():  # 遍历每个行业分类
        # 获取该行业的成分股
        ind_stocks = set(get_industry_stocks(idx, date=end_date))
        # 找出在 df_bias 表中存在的成分股
        ind_avail_stocks = list(columns & ind_stocks) # 成分股 在df_bias表中存在的
        if ind_avail_stocks:
            # 计算该行业成分股收盘价大于过去 20 个交易日均值的百分比
            df[row['code']] = (100*(df_bias[ind_avail_stocks].sum(axis=1))/len(ind_avail_stocks)).astype(int)
    df.sort_index(ascending=False, inplace=True)  # 按行业热门股票比例降序排序
    # 选择前 count 行，计算这些行的均值，并降序排序
    sr = df.iloc[0:count,:].mean().sort_values(ascending=False, inplace=False)
    # 选择均值最高的前 number 个行业
    sr = list(sr[0:number].index)
    # 初始化一个空集合用于存储热门行业的成分股
    stocks_set = set()
    # 遍历选出的行业
    for s in sr:
        # 获取该行业的成分股
        ind_stocks = set(get_industry_stocks(s, date=end_date))
        # 将这些成分股添加到集合中
        stocks_set.update(ind_stocks)
    # 返回热门行业的成分股列表
    return list(stocks_set)

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

#3-4 交易模块-调仓
def adjust_position(context, buy_stocks, stock_num):
	for stock in context.portfolio.positions:              # 遍历当前账户持仓中的所有股票
		if stock not in buy_stocks:                        # 如果股票不在应买入列表中
			log.info("[%s]不在应买入列表中" % (stock))     # 日志记录不在买入列表中的股票
			position = context.portfolio.positions[stock]  # 获取股票的持仓信息
			close_position(position)                       # 执行平仓操作
		# 如果股票已经在持仓中
		else:
			log.info("[%s]已经持有无需重复买入" % (stock)) # 日志记录已持有的股票，无需重复买入

	position_count = len(context.portfolio.positions)      # 计算当前持仓的股票数量
	if stock_num > position_count:                         # 如果需要持有的股票数量大于当前持仓数量
		value = context.portfolio.cash / (stock_num - position_count)  # 计算每只股票应分配的资金
		for stock in buy_stocks:                           # 遍历应买入的股票列表
			if context.portfolio.positions[stock].total_amount == 0:   # 如果当前股票不在持仓中
				if open_position(stock, value):            # 执行开仓操作
					if len(context.portfolio.positions) == stock_num:  # 如果持仓的股票数量达到了预期数量，则停止买入
						break

#4-1 打印每日持仓信息
def print_position_info(context):
    #打印当天成交记录
    trades = get_trades()
    for _trade in trades.values():
        print('成交记录：'+str(_trade))
    #打印账户信息
    for position in list(context.portfolio.positions.values()):
        securities=position.security  # 获取股票代码
        cost=position.avg_cost        # 获取股票当前持仓成本
        price=position.price          # 获取股票最新价
        ret=100*(price/cost-1)        # 计算股票收益率
        value=position.value          # 获取股票市值
        amount=position.total_amount  # 获取股票持股数量   
        print('代码:{}'.format(securities))
        print('成本价:{}'.format(format(cost,'.2f')))
        print('现价:{}'.format(price))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
        print('———————————————————————————————————')
    print('———————————————————————————————————————分割线————————————————————————————————————————')