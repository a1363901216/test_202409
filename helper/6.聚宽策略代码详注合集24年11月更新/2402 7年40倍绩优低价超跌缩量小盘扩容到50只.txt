# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/39960
# 标题：“7年40倍”策略扩容到50只
# 作者：wywy1995
# 回测资金 500000

from jqdata import *
import math
import pandas as pd  # 将 pandas库导入当前环境，并给它指定一个别名 pd

def initialize(context):
    # 设定基准
    set_benchmark('000905.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 设置滑点为理想情况，不同滑点影响可以在归因分析中查看
    set_slippage(PriceRelatedSlippage(0.000))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='fund')
    # 除非需要精简信息，否则不要过滤日志，方便debug
    #log.set_level('system', 'error')
    #初始化全局变量
    g.stock_num = 50      # 设置最大持仓股票数量
    g.limit_up_list = []  # 涨停股票列表
    g.hold_list = []      # 当前持仓列表
    g.weights = [1.0, 1.0, 1.6, 0.8, 2.0]  # 选股因子权重
    # 设置交易时间，每天运行
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')  # 设置定时运行函数，每天开盘前准备股票池
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG') # 每周第一个交易日进行调仓
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')  # 每天14:00检查涨停股票
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')  # 每天收盘后打印持仓信息

#1-1 准备股票池 获取已持有和昨日涨停的股票列表
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []  # 初始化已持有股票列表，用于存储当前持仓的股票代码
    for position in list(context.portfolio.positions.values()):   # 遍历当前账户持仓的每个股票的持仓信息
        # 获取股票代码，并添加到已持有股票列表中
        stock = position.security   
        g.hold_list.append(stock)
    #获取昨日涨停列表
    if g.hold_list != []:  # 判断已持有股票列表是否为空
        # 如果不为空，获取这些股票在前一交易日的收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]  # 从价格数据中筛选出昨日收盘价等于涨停价的股票，即昨日涨停的股票
        g.high_limit_list = list(df.code)         # 从筛选结果中提取股票代码，并赋值给昨日涨停股票列表
    else:
        g.high_limit_list = []                    # 如果已持有股票列表为空，则设置昨日涨停股票列表为一个空列表

#1-2 选股模块，定义函数用于获取股票列表
def get_stock_list(context):
    def get_close(stock, n, unit):  # 定义辅助函数 get_close，用于获取指定股票在过去 n 个单位时间的收盘价
        return attribute_history(stock, n, unit, 'close')['close'][0]  # attribute_history 函数用于获取历史数据，这里获取收盘价

    def get_return(stock, n, unit):  # 定义辅助函数 get_return，用于计算股票现价相对于 n 个单位前价格的涨幅
        price_before = attribute_history(stock, n, unit, 'close')['close'][0]  # 获取 n 个单位前的价格
        price_now = get_close(stock, 1, '1m')  # 获取当前价格，使用 1m 表示当前分钟的价格
        if not isnan(price_now) and not isnan(price_before) and price_before != 0:  # 检查价格是否有效且不为零
            return price_now / price_before  # 如果有效，计算涨幅
        else:
            return 100  # 如果价格无效或为零，返回 100，表示涨幅无效

    # 获得初始列表
    yesterday = context.previous_date  # 获取前一交易日的日期
    initial_list = get_all_securities('stock', yesterday).index.tolist() # 获取所有股票的列表，并转换为初始股票池列表
    initial_list = filter_kcbj_stock(initial_list) # 过滤掉科创板和北交所的股票
    initial_list = filter_new_stock(context, initial_list, 375) # 过滤掉次新股，这里filter_new_stock 函数，用于过滤上市不满 375 天的股票
    initial_list = filter_st_stock(initial_list) # 过滤掉 ST 股票，这里filter_st_stock 函数，用于过滤 ST 类股票
    q = query(
        valuation.code, valuation.market_cap, valuation.circulating_market_cap
    ).filter(
        valuation.code.in_(initial_list),
        valuation.pb_ratio > 0,    # 市净率大于 0
        indicator.inc_return > 0,  # 收入增长率大于 0
        indicator.inc_total_revenue_year_on_year > 0,  # 总收入同比增长率大于 0
        indicator.inc_net_profit_year_on_year > 0      # 净利润同比增长率大于 0
    ).order_by(
        valuation.market_cap.asc()).limit(100)    # 按市值升序排列
    df = get_fundamentals(q, date=yesterday)      # 获取基本面数据
    df.index = df.code   # 设置 DataFrame 索引为股票代码
    initial_list = list(df.index)  # 更新初始股票池为基本面数据中的股票
    
    # 初始化用于存储原始值的列表
    MC, CMC, PN, TV, RE = [], [], [], [], []
    for stock in initial_list:
        # 获取总市值并添加到列表
        mc = df.loc[stock]['market_cap']
        MC.append(mc)
        # 获取流通市值并添加到列表
        cmc = df.loc[stock]['circulating_market_cap']
        CMC.append(cmc)
        # 获取当前价格并添加到列表
        pricenow = get_close(stock, 1, '1m')
        PN.append(pricenow)
        # 获取 5 日累计成交量并添加到列表
        total_volume_n = attribute_history(stock, 1200, '1m', 'volume')['volume'].sum()
        TV.append(total_volume_n)
        # 获取 60 日涨幅并添加到列表
        m_days_return = get_return(stock, 60, '1d') 
        RE.append(m_days_return)
    # 创建 DataFrame 并添加列
    df = pd.DataFrame(index=initial_list,
        columns=['market_cap','circulating_market_cap','price_now','total_volume_n','m_days_return'])
    # 填充 DataFrame 数据
    df['market_cap'] = MC
    df['circulating_market_cap'] = CMC
    df['price_now'] = PN
    df['total_volume_n'] = TV
    df['m_days_return'] = RE
    df = df.dropna()   # 移除含有缺失值的行
    min0, min1, min2, min3, min4 = min(MC), min(CMC), min(PN), min(TV), min(RE)  # 计算各因子的最小值
    # 计算合成因子得分
    temp_list = []
    for i in range(len(list(df.index))):
        # 根据全局变量 g.weights 中定义的权重和因子的最小值计算得分
        score = g.weights[0] * math.log(min0 / df.iloc[i,0]) + g.weights[1] * math.log(min1 / df.iloc[i,1]) + g.weights[2] * math.log(min2 / df.iloc[i,2]) + g.weights[3] * math.log(min3 / df.iloc[i,3]) + g.weights[4] * math.log(min4 / df.iloc[i,4])
        temp_list.append(score)
    df['score'] = temp_list  # 将得分添加到 DataFrame
    
    #排序并返回最终选股列表
    df = df.sort_values(by='score', ascending=False)
    final_list = list(df.index)  # 获取得分最高的股票代码列表
    return final_list  # 返回最终的选股列表

#1-4 定义每周调仓函数 ；整体调整持仓 ；目的是进行量化交易策略中的周期性调仓操作。
def weekly_adjustment(context):
    target_list = get_stock_list(context)  # 获取应买入的股票列表，调用 get_stock_list 函数筛选股票。
    target_list = filter_paused_stock(target_list)  # 过滤停牌股票，确保调仓列表中不包含停牌的股票。
    target_list = filter_limitup_stock(context, target_list)  # 过滤涨停股票，确保调仓列表中不包含涨停的股票。
    target_list = filter_limitdown_stock(context, target_list)  # 过滤跌停股票，确保调仓列表中不包含跌停的股票。
    target_list = target_list[:min(g.stock_num, len(target_list))]  # 截取不超过最大持仓数的股票量，根据全局变量 g.stock_num 确定最大持仓数。
    # 调仓卖出逻辑
    for stock in g.hold_list:  # 遍历当前持仓列表
        if (stock not in target_list) and (stock not in g.high_limit_list):  # 如果股票不在应买入列表中，并且不在昨日涨停列表中，则卖出该股票。
            log.info("卖出[%s]" % (stock))  # 记录卖出信息
            position = context.portfolio.positions[stock]  # 获取股票的持仓信息
            close_position(position)  # 执行平仓操作
        else:
            log.info("已持有[%s]" % (stock)) # 记录已持有股票信息，无需卖出
    # 调仓买入逻辑
    position_count = len(context.portfolio.positions)  # 获取当前持仓数量
    target_num = len(target_list)  # 获取应买入股票列表的数量
    if target_num > position_count:  # 如果应买入股票数量大于当前持仓数量
        value = context.portfolio.cash / (target_num - position_count) # 计算每只股票应分配的资金
        for stock in target_list: # 遍历应买入股票列表
            if context.portfolio.positions[stock].total_amount == 0: # 如果当前股票不在持仓中，则买入该股票。
                if open_position(stock, value):  # 执行开仓操作
                    if len(context.portfolio.positions) == target_num:  # 如果持仓数量等于目标数量，则停止买入
                        break

#1-5 定义检查昨日涨停股票的函数 ；调整昨日涨停股票
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