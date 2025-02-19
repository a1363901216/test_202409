# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40237
# 标题：【择时模块实际效果】--论坛随便选了个策略加装
# 作者：一只皮卡丘

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/39961
# 标题：科技与狠活
# 作者：wywy1995

#导入函数库
from jqdata import *  # 从jqdata模块导入所有内容
from jqfactor import get_factor_values  # 从jqfactor模块导入get_factor_values函数
import numpy as np
import pandas as pd
import talib

#初始化函数 
def initialize(context):
    # 设定基准
    set_benchmark('000905.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 设置交易成本万分之三，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='fund')
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    #初始化全局变量
    g.stock_num = 10      # 设置最大持仓股票数量
    g.limit_days = 20     # 设置股票持有或交易的限制天数
    g.limit_up_list = []  # 用于存储涨停股票的列表
    g.hold_list = []      # 用于存储当前持有股票的列表。
    g.history_hold_list = []   # 用于存储历史持有过的股票列表
    g.not_buy_again_list = []  # 用于存储不再购买的股票列表
    # 设置交易时间，每天运行
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    run_daily(daily_adjustment, time='9:40', reference_security='000300.XSHG')
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')


#1-1 选股模块
def get_factor_filter_list(context,stock_list,jqfactor,sort,p1,p2):
    yesterday = context.previous_date    # 获取策略运行前一天的日期
    # 使用get_factor_values函数获取股票列表中每只股票的指定因子值
    score_list = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1)[jqfactor].iloc[0].tolist()
    df = pd.DataFrame(columns=['code','score'])     # 创建一个DataFrame，用于存储股票代码和对应的因子得分
    # 将股票代码和因子得分分别赋值给DataFrame的对应列。
    df['code'] = stock_list
    df['score'] = score_list
    df = df.dropna()   # 去除DataFrame中的缺失值
    df.sort_values(by='score', ascending=sort, inplace=True)  # 根据因子得分对DataFrame进行排序，'sort'参数决定排序方式，True表示升序，False表示降序
    filter_list = list(df.code)[int(p1*len(df)):int(p2*len(df))] # 根据给定的比例p1和p2筛选股票，例如，如果p1=0.1和p2=0.3，则选择得分排名在前20%至30%之间的股票
    return filter_list   # 返回筛选出的股票列表

#1-2 选股模块
def get_stock_list(context):
    yesterday = context.previous_date       # 获取策略运行前一天的日期
    initial_list = get_all_securities().index.tolist()      # 获取所有上市股票的代码列表
    initial_list = filter_kcbj_stock(initial_list)      # 过滤掉科创板和创业板的股票
    initial_list = filter_st_stock(initial_list)        # 过滤掉ST股票
    initial_list_1 = filter_new_stock(context, initial_list, 250)     # 过滤掉新上市的股票，这里以上市天数小于250天为新上市的标准
    # 筛选长期资产回报率（ROA）较小的股票
    test_list = get_factor_filter_list(context, initial_list_1, 'roa_ttm_8y', True, 0, 0.1)
    # 查询这些股票的市值和每股收益（EPS），并按市值升序排序
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(test_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday) 
    df = df[df['eps']>0]   # 进一步筛选出每股收益大于0的股票
    roa_list = list(df.code)[:5]     # 选取前5只股票作为最终的股票池
    # 筛选每股留存收益较小的股票
    test_list = get_factor_filter_list(context, initial_list_1, 'retained_earnings_per_share', True, 0, 0.1)
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(test_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday)
    df = df[df['eps']>0]
    reps_list = list(df.code)[:5]
    # 筛选非线性市值较小的股票，这里以上市天数小于125天为新上市的标准
    initial_list_2 = filter_new_stock(context, initial_list, 125)
    test_list = get_factor_filter_list(context, initial_list_2, 'non_linear_size', True, 0, 0.1)
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(test_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday)
    df = df[df['eps']>0]
    nls_list = list(df.code)[:5]
    # 对三个股票列表进行并集操作，并去除重复项
    union_list = list(set(roa_list).union(set(reps_list)).union(set(nls_list)))
    q = query(valuation.code,valuation.circulating_market_cap).filter(valuation.code.in_(union_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q, date=yesterday)
    final_list = list(df.code)  # 获取最终的股票列表
    return final_list  # 返回最终的股票列表

#1-3 准备股票池
def prepare_stock_list(context):
    g.hold_list= []      # 初始化一个空列表，用于存储当前持有的股票代码
    for position in list(context.portfolio.positions.values()):      # 遍历当前投资组合中的股票持仓
        # 获取股票代码，并添加到持有列表中
        stock = position.security
        g.hold_list.append(stock)
    g.history_hold_list.append(g.hold_list)  # 将当前持有的股票列表添加到历史持有列表中
    # 如果历史持有列表的长度超过了设定的限制天数，则只保留最近的限制天数内的数据
    if len(g.history_hold_list) >= g.limit_days:
        g.history_hold_list = g.history_hold_list[-g.limit_days:]     # 保留最近limit_days天的数据
    # 使用集合来存储历史持有过的所有股票，以去除重复项
    temp_set = set()
    for hold_list in g.history_hold_list:
        for stock in hold_list:
            temp_set.add(stock)  # 将股票代码添加到集合中
    g.not_buy_again_list = list(temp_set)     # 将历史持有过的所有股票的列表存储在全局变量中
    #获取昨日涨停列表
    if g.hold_list != []:   # 如果当前持有列表不为空，则获取昨日涨停的股票列表
        # 使用get_price函数获取持有股票列表在昨日的收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]   # 筛选出收盘价等于涨停价的股票，即昨日涨停的股票
        g.high_limit_list = list(df.code)        # 将涨停股票的代码列表存储在全局变量中
    else:
        g.high_limit_list = []  # 如果当前持有列表为空，则设置昨日涨停股票列表为空

#1-4 整体调整持仓
def daily_adjustment(context):
    #获取应买入列表
    target_list = get_stock_list(context)
    target_list = filter_paused_stock(target_list)
    target_list = filter_limitup_stock(context, target_list)
    target_list = filter_limitdown_stock(context, target_list)
    #过滤最近买过且涨停过的股票
    #recent_limit_up_list = get_recent_limit_up_stock(context, target_list, g.limit_days)
    #black_list = list(set(g.not_buy_again_list).intersection(set(recent_limit_up_list)))
    #target_list = [stock for stock in target_list if stock not in black_list]
    #截取不超过最大持仓数的股票量
    #target_list = target_list[:min(g.stock_num, len(target_list))]
    #调仓卖出
    base = get_bars('000852.XSHG', 30, unit='1d',
    fields=['date', 'open', 'close', 'high', 'low', 'volume', 'money'],
    include_now=False, end_dt=None, df=True)
    
    base['EMA2']=talib.EMA(base['close'],2)
    base['EMA4']=talib.EMA(base['close'],4)
    base['dif']=base['EMA2']-base['EMA4']
    base['dea']=talib.EMA(base['dif'],4)
    base['signal'] = np.where(base['dif']-base['dea']<0,1,0)
    #print(base)
    #获得今日开平仓信号
    today_sig=np.array(base['signal'])[-1]
    try:
        if today_sig > 0:
            for stock in g.hold_list:
                position = context.portfolio.positions[stock]
                close_position(position)
                print(context.previous_date,f'大盘风控止损触发,全仓卖出{stock}')
            return
    except:
        print('开仓！') 

    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):
            log.info("卖出[%s]" % (stock))
            position = context.portfolio.positions[stock]
            close_position(position)
        else:
            log.info("已持有[%s]" % (stock))
    #调仓买入
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    if target_num > position_count:
        value = context.portfolio.cash / (target_num - position_count)
        for stock in target_list:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == target_num:
                        break

#1-5 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt  # 获取当前时间
    if g.high_limit_list != []:    # 检查昨日涨停股票列表是否为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.high_limit_list:   # 遍历昨日涨停股票列表
            # 获取当前股票的最新价格和涨停价
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0,0] < current_data.iloc[0,1]:  # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))   # 如果打开涨停，则记录卖出信息
                position = context.portfolio.positions[stock]   # 获取股票当前的持仓情况
                close_position(position)   # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % (stock))   # 如果仍然涨停，则记录继续持有的信息

#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

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
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()  # 使用列表推导式，返回不在涨停状态或已持有的股票列表
			or last_prices[stock][-1] < current_data[stock].high_limit]

#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
    current_data = get_current_data() # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() # 使用列表推导式，返回不在跌停状态或已持有的股票列表
            or last_prices[stock][-1] > current_data[stock].low_limit]

#2-6 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

#2-7 过滤次新股
def filter_new_stock(context, stock_list, d):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过d的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=d)]

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
    trades = get_trades()  # 获取当天所有成交记录
    for _trade in trades.values():  # 遍历成交记录字典的值（即成交记录对象）
        print('成交记录：'+str(_trade))
    #打印账户信息
    for position in list(context.portfolio.positions.values()):  # 遍历账户中所有持仓的股票信息
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