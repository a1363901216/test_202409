# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/41921
# 标题：大市值价值投资，从2005年至今超额稳定
# 作者：Ahfu

from jqdata import *  # 从jqdata模块导入所有内容

# 初始化函数，设定基准等等
def initialize(context):
	# 设定沪深300作为基准
	set_benchmark('000300.XSHG')
	# 开启动态复权模式(真实价格)
	set_option('use_real_price', True)
	#防止未来函数
	set_option("avoid_future_data", True)
	# 输出内容到日志 log.info()
	log.info('初始函数开始运行且全局只运行一次')
	# 过滤掉order系列API产生的比error级别低的log
	log.set_level('order', 'error')
	
	g.buy_stock_count = 5  # 定义全局变量，用于指定策略希望持有的股票数量是5只
	g.check_out_lists = []  # 定义全局变量，用于存储策略筛选出来的股票代码列表；初始化为空列表，表示开始时没有选定任何股票
	
	# 设置交易成本，          卖出时印花税      买入时印花税               卖出时佣金            最低佣金，不包含印花税
	set_order_cost(OrderCost(close_tax=0.001, open_commission=0.00012, close_commission=0.00012, min_commission=5),
				   type='stock')  # type: 股票
	
	run_monthly(before_market_open, 1, time='5:00', reference_security='000300.XSHG')	# 每月第一个交易日开盘前 5:00 运行 before_market_open
	run_monthly(my_trade, 1, time='9:30', reference_security='000300.XSHG')             # 每月第一个交易日开盘时 9:30 运行 my_trade
	run_daily(after_market_close, time='after_close', reference_security='000300.XSHG') # 收盘后 after_close 运行 after_market_close

# 开盘时运行函数
def my_trade(context):
	# 调用adjust_position函数来调整当前的投资组合，的是在开盘时根据预设的股票列表调整持仓，执行买入或卖出操作
	adjust_position(context, g.check_out_lists)

# 收盘后运行函数
def after_market_close(context):
	positions_dict = context.portfolio.positions      # 获取当前投资组合中所有持仓的字典对象
	for position in list(positions_dict.values()):      # 遍历投资组合中的每个持仓项
	    # 使用log.info打印每个持仓的详细信息，格式化字符串，输出持仓的股票名称、数量、市值、盈利百分比和建仓时间
	    log.info("当前持仓:{0}, 数量:{1}, 市值:{2}, 盈利：{3}%, 建仓时间:{4}".format(get_name(position.security), position.total_amount, round(position.value,0), round((position.value-(position.avg_cost*position.total_amount))/(position.avg_cost*position.total_amount)*100,1), position.init_time))
	log.info('#########################################################################################\n\n')

# 自定义下单
# 根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
# 报单成功返回报单（不代表一定会成交），否则返回None
def order_target_value_(security, value):
	if value == 0:    # 检查目标价值是否为0，如果是，则表示需要卖出该股票
		log.debug("卖出 %s" % (get_name(security)))     # 获取股票的名称，并记录日志信息，表示卖出操作
	else:
		log.debug("买入 %s ，市值： %f" % (get_name(security), value)) # 如果目标价值不为0，表示需要买入该股票至特定市值，获取股票的名称，并记录日志信息，包括买入的股票名称和目标市值
	# 如果股票停牌，创建报单会失败，order_target_value 返回None
	# 如果股票涨跌停，创建报单会成功，此时order_target_value返回Order对象，但报单可能会被取消
	# 对于部分成交后撤销的报单，聚宽平台的状态是已撤销，此时可以通过检查成交量是否大于0来判断是否有成交
	return order_target_value(security, value)

# 开仓，买入指定价值的证券
# 此函数用于根据给定的股票代码（security）和目标价值（value）买入股票
# 如果报单成功并且至少有部分成交（成交量大于0），则函数返回True
# 如果报单失败或者虽然报单成功但最终被取消（此时成交量等于0），则函数返回False
def open_position(security, value):
    # 调用自定义下单函数order_target_value_，传入股票代码和目标价值，此函数会尝试创建一个订单，使指定股票的持仓价值达到目标价值
	order = order_target_value_(security, value)
	if order != None and order.filled > 0:   # 如果订单不为空（即下单成功）并且已成交数量大于0（即至少部分成交）
		return True        # 返回True，表示买入操作成功
	return False     # 如果订单为空（下单失败）或者已成交数量为0（下单成功但未成交，可能被取消），返回False，表示买入操作未成功

# 平仓，卖出指定持仓
# 此函数用于卖出给定的持仓（position），目标是将持仓价值降至0
# 如果平仓成功并且全部成交，函数返回True
# 如果报单失败或者报单成功但最终被取消（此时成交量等于0），或者报单非全部成交，函数返回False
def close_position(position):
	security = position.security    # 从持仓对象中获取股票代码
	order = order_target_value_(security, 0)  # 目标是将指定股票的持仓价值降至0，即全部卖出， 可能会因停牌失败
	if order != None:      # 如果订单不为空（即下单成功）
		# 进一步检查订单状态是否为已成交（held），并且已成交数量等于下单数量（即全部成交）
		if order.status == OrderStatus.held and order.filled == order.amount:
			return True        # 如果全部成交，则返回True，表示平仓操作成功
	return False    # 如果订单为空（下单失败），或者订单状态不是已成交，或者未全部成交（成交量等于0），返回False，表示平仓操作未成功

# 交易函数，用于调整持仓
def adjust_position(context, buy_stocks):
	for stock in context.portfolio.positions:      # 遍历当前投资组合中的所有持仓股票
		current_data = get_current_data()          # 获取当前的股票数据
		nosell_1 = context.portfolio.positions[stock].price >= current_data[stock].high_limit   # 检查是否因为涨停而不能卖出
		sell_2 = stock not in buy_stocks    # 检查该股票是否不在买入列表中
		if sell_2 and not nosell_1:     # 如果股票不在买入列表中且不是因为涨停而不能卖出，则卖出该股票
			log.info("调出平仓：[%s]" % (stock)) 
			position = context.portfolio.positions[stock]     # 获取该股票的持仓信息
			close_position(position)      # 调用close_position函数尝试卖出该股票
		else:
			log.info("已持仓，本次不买入：[%s]" % (stock))     # 如果股票仍然在买入列表中或因涨停不能卖出，则不进行操作
	# 根据股票数量分仓，此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
	position_count = len(context.portfolio.positions)
	if g.buy_stock_count > position_count:     # 如果当前持仓数量少于预设的股票数量
		value = context.portfolio.cash / (g.buy_stock_count - position_count)     # 计算每只股票的购买价值
		for stock in buy_stocks:        # 遍历买入列表中的股票
			if stock not in context.portfolio.positions:     # 如果该股票不在当前持仓中
				if open_position(stock, value):      # 调用open_position函数尝试买入该股票
					if len(context.portfolio.positions) == g.buy_stock_count:     # 如果买入成功且持仓数量达到预设的股票数量，则停止买入
						break

# 通过代码返回股票名称
def get_name(stk):
    return get_security_info(stk).display_name+':'+stk[:6]  # get_security_info 可以获取股票等的信息，包括代码，中文名称等

# 开盘前运行函数
def before_market_open(context):
    g.check_out_lists = []    # 清空前一个交易日的股票池列表
    current_data = get_current_data()    # 获取当前的交易数据
    check_date = context.previous_date - datetime.timedelta(days=200)    # 设置查询日期为前一个交易日减去200天，用于获取足够历史数据的证券列表
    all_stocks = list(get_all_securities(date=check_date).index)    # 获取指定日期的所有证券列表
    # 过滤掉不符合条件的股票：创业板、ST股、停牌、当日涨停或跌停的股票
    all_stocks = [stock for stock in all_stocks if not (
            (current_data[stock].day_open == current_data[stock].high_limit) or  # 涨停开盘
            (current_data[stock].day_open == current_data[stock].low_limit) or  # 跌停开盘
            current_data[stock].paused or  # 停牌
            current_data[stock].is_st or  # ST
            ('ST' in current_data[stock].name) or
            ('*' in current_data[stock].name) or
            ('退' in current_data[stock].name) or
            (stock.startswith('30')) or  # 创业
            (stock.startswith('68')) or  # 科创
            (stock.startswith('8')) or  # 北交
            (stock.startswith('4'))   # 北交
    )]
    # 构建查询对象，用于筛选符合条件的股票
    q = query(
        valuation.code, valuation.market_cap, valuation.pe_ratio, income.total_operating_revenue
        ).filter(
        valuation.pb_ratio < 1,    # 市净率小于1
        cash_flow.subtotal_operate_cash_inflow > 1e6,  # 经营活动产生的现金流量净额大于1亿
        indicator.adjusted_profit > 1e6,    # 调整后的净利润大于1亿
        indicator.roa > 0.15,    # 资产回报率大于0.15
        indicator.inc_net_profit_year_on_year > 0,    # 净利润同比增长大于0
    	valuation.code.in_(all_stocks)  # 股票代码在过滤后的列表中
    	).order_by(
    	indicator.roa.desc()  # 按资产回报率降序排列
    ).limit(
    	g.buy_stock_count * 3  # 限制查询结果数量为预设买入股票数量的3倍
    )
    check_out_lists = list(get_fundamentals(q).code)      # 执行查询并获取符合条件的股票列表
    check_out_lists = check_out_lists[:g.buy_stock_count]  # 取需要的股票数量，即预设买入股票数量
    g.check_out_lists = check_out_lists    # 更新全局变量check_out_lists为新的股票池
    log.info("今日股票池：%s" % g.check_out_lists)      # 记录今日股票池到日志

