# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40139
# 标题：正黄旗大妈选股法，修改版，年化92%
# 作者：oupian

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40038
# 标题：正黄旗大妈选股法
# 作者：GoodThinker

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40004
# 标题：菜场大妈选股法
# 作者：开心果

import pandas as pd   # 导入pandas库并设置别名pd
from jqdata import *  # 从jqdata模块导入所有内容
from jqlib.technical_analysis import *  # 从jqlib.technical_analysis模块中导入所有函数、类和变量

def initialize(context):
    # setting
    log.set_level('order', 'error')    # 过滤order中低于error级别的日志
    set_option('use_real_price', True)    # 用真实价格交易
    set_option('avoid_future_data', True)
    set_benchmark('000905.XSHG')
    # 设置滑点为理想情况，纯为了跑分好看，实际使用注释掉为好
    set_slippage(PriceRelatedSlippage(0.000))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='fund')
    # strategy
    g.stock_num = 10  # 设置要持有的股票数量为10
    # 设置交易时间，按日，按周运行
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    run_monthly(my_Trader, 1 ,time='9:30')
    run_daily(check_limit_up, time='14:00')
    run_daily(check_at_dayend, time='14:30')
    g.buylist = []  # 初始化买入股票列表

#1-1 根据最近一年分红除以当前总市值计算股息率并筛选    
def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date    # 获取策略运行的上一个日期
    time0 = time1 - datetime.timedelta(days=365)    # 计算一年前的日期，用于获取分红数据的时间范围
    #获取分红数据，由于finance.run_query最多返回4000行，以防未来数据超限，最好把stock_list拆分后查询再组合
    interval = 1000   # 某只股票可能一年内多次分红，导致其所占行数大于1，所以interval不要取满4000
    list_len = len(stock_list)     # 获取股票列表长度
    # 截取不超过interval的列表并查询，始化一个查询对象 q，用于获取分红数据，finance.STK_XR_XD 表包含股票的分红信息
    #                股票代码                         股权登记日                        每股分红金额（人民币）
    q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date, finance.STK_XR_XD.bonus_amount_rmb
    ).filter(
        finance.STK_XR_XD.a_registration_date >= time0,  # 股权登记日在一年前
        finance.STK_XR_XD.a_registration_date <= time1,  # 股权登记日在当前日期之前
        finance.STK_XR_XD.code.in_(stock_list[:min(list_len, interval)]))  # 股票代码在给定的股票列表中
    df = finance.run_query(q)    # 执行查询并获取分红数据
    #对interval的部分分别查询并拼接
    if list_len > interval:    # 如果股票列表长度超过查询限制，则分批查询
        df_num = list_len // interval    # 计算需要查询的次数
        for i in range(df_num):
            # 构建每个批次的查询对象
            q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date, finance.STK_XR_XD.bonus_amount_rmb
            ).filter(
                finance.STK_XR_XD.a_registration_date >= time0,  # 股权登记日在一年前
                finance.STK_XR_XD.a_registration_date <= time1,  # 股权登记日在当前日期之前
                finance.STK_XR_XD.code.in_(stock_list[interval*(i+1):min(list_len,interval*(i+2))]))
            # 执行查询并追加到之前的分红数据 DataFrame
            temp_df = finance.run_query(q)
            df = df.append(temp_df)
    # 对分红数据进行处理，填充空值为0，并按股票代码分组求和
    dividend = df.fillna(0)
    #PY3 dividend = dividend.set_index('code')
    dividend = dividend.groupby('code').sum()
    # 获取有分红信息的股票列表
    temp_list = list(dividend.index) #query查询不到无分红信息的股票，所以temp_list长度会小于stock_list
    # 获取市值数据  股票代码          总市值              股票代码在有分红信息的股票列表中
    q = query(valuation.code,valuation.market_cap).filter(valuation.code.in_(temp_list))
    cap = get_fundamentals(q, date=time1)    # 执行查询并获取市值数据
    cap = cap.set_index('code')      # 将市值数据设置为以股票代码为索引的 DataFrame
    # 合并分红数据和市值数据，并计算股息率
    DR = pd.concat([dividend, cap] ,axis=1, sort=False)
    DR['dividend_ratio'] = (DR['bonus_amount_rmb']/10000) / DR['market_cap']
    # 根据股息率进行排序并筛选
    DR = DR.sort_values(by=['dividend_ratio'], ascending=sort)
    final_list = list(DR.index)[int(p1*len(DR)):int(p2*len(DR))]     # 根据比例 p1 和 p2 计算筛选的股票列表
    return final_list    # 返回筛选后的股票列表

# 定义 my_Trader 函数，用于执行交易策略
def my_Trader(context):
    dt_last = context.previous_date    # 获取前一个交易日的日期
    # 获取所有股票的列表
    stocks = get_all_securities('stock', dt_last).index.tolist()
    # 过滤掉科创板和创业板股票
    stocks = filter_kcbj_stock(stocks)
    # 筛选高股息率的股票，选择全市场中股息率最高的25%
    stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.25)
    # 获取基本面数据，包括PEG比率和PB-ROE比率
    q = query(valuation.code,
                valuation.pe_ratio / indicator.inc_net_profit_year_on_year,  # PEG
                indicator.roe / valuation.pb_ratio,   # PB-ROE
                indicator.roe ,
                ).filter(
                    valuation.pe_ratio / indicator.inc_net_profit_year_on_year>-1,
                    valuation.pe_ratio / indicator.inc_net_profit_year_on_year<3,
                    #indicator.roe / valuation.pb_ratio > 0,
                    valuation.code.in_(stocks))
    # 获取符合条件的股票的基本面数据
    df_fundamentals = get_fundamentals(q, date=None)
    # 提取股票代码列表
    stocks = list(df_fundamentals.code)
    # fuandamental data
    #df = get_fundamentals(query(valuation.code).filter(valuation.code.in_(stocks)).order_by(valuation.market_cap.asc()))
    #df = get_fundamentals(query(valuation.code,valuation.market_cap).filter(valuation.code.in_(stocks)).order_by(valuation.market_cap.asc()))
    # 获取市值数据，筛选市值小于等于100亿的股票
    q = query(valuation.code,
              valuation.market_cap
                ).filter(
                    valuation.code.in_(stocks),
                    valuation.market_cap<=100).order_by(valuation.market_cap.asc())    
    # 获取符合条件的股票的市值数据
    df = get_fundamentals(q, date = None)
    print(df.shape)
    #print(df)
    # 提取股票代码列表
    choice = list(df.code)
    # 过滤掉ST股票
    choice = filter_st_stock(choice)
    # 过滤掉停牌的股票
    choice = filter_paused_stock(choice)
    # 过滤掉涨停的股票
    choice = filter_limitup_stock(context, choice)
    # 过滤掉跌停的股票
    choice = filter_limitdown_stock(context, choice)
    # 过滤掉高价股票
    choice = filter_highprice_stock(context, choice)
    #choice = choice[:g.stock_num]
    g.buylist = choice  # 更新全局变量 g.buylist 为筛选后的股票列表
    # Sell
    #sell_list = []
    #for s in context.portfolio.positions:
    #    if (s not in choice) :
    #        sell_list.append(s)
    #sellstock(context, sell_list)
    # buy
    buystock(context, g.buylist)   # 执行买入操作

# 定义 buystock 函数，用于执行买入操作
def buystock(context,choice):
    position_count = len(context.portfolio.positions)    # 获取当前持仓的股票数量
    if g.stock_num <= position_count:    # 如果预设的股票数量小于或等于当前持仓数量，则不执行买入操作
        return
    buylist = choice     # 初始化买入列表为传入的选择股票列表
    #buylist = []
    #K,D = KD(choice, check_date=context.previous_date, N = 9, M1 = 3, M2 = 3)
    #VOLT,MAVOL10,MAVOL20 = VOL(choice, check_date=context.current_dt, M1=10, M2=20, include_now = True)
    #VOLT,MAVOL5,MAVOL20 = VOL(choice, check_date=context.current_dt, M1=5, M2=20, include_now = True)
    #for s in choice:
        #K值小于25 #20日均量不能超过10日均量的1.3倍；10日均量不能超过50日均量的1.3倍；说明前期未明显放量
        #if (K[s] > 25) or (MAVOL20[s] > MAVOL10[s]*1.3) or (MAVOL10[s] > MAVOL5[s]*1.8):
        #    continue
    #    buylist.append(s)
    cdata = get_current_data()      # 获取当前市场数据
    #namelist = []
    #for s in buylist:
    #    namelist.append(cdata[s].name)
    #log.info('bulist:', namelist)    
    # 计算每只股票的预期买入金额
    psize = context.portfolio.available_cash/(g.stock_num - position_count)
    for s in buylist:    # 遍历买入列表中的股票
        if s not in context.portfolio.positions:        # 检查股票是否已经在持仓中
            log.info('buy', s, cdata[s].name)
            order_value(s, psize)       # 下达买入订单，买入金额为计算出的预期买入金额
            # 如果持仓数量达到预设的股票数量，则停止买入操作
            if len(context.portfolio.positions) == g.stock_num:
                break   

# 用于执行卖出操作
def sellstock(context, sell_list):
    current_data=get_current_data()    # 获取当前的股票市场数据，包括价格、涨跌停信息等
    if len(sell_list)>0:    # 检查sell_list列表是否不为空，即是否有股票需要卖出
        for security in sell_list:
            cprice = current_data[security].last_price     # 获取当前股票的最新成交价格
            boughtcost = context.portfolio.positions[security].acc_avg_cost   # 获取该股票的持仓成本
            profit = (cprice - boughtcost)/boughtcost *100    # 计算利润率，即(当前价格 - 成本价格) / 成本价格 * 100%
            log.info("Sell %s " % (current_data[security].name), "profit: %.1f%%" % profit, "init time %s" % context.portfolio.positions[security].init_time)
            limit_price = max(cprice*0.95,current_data[security].low_limit)    # 设置一个限制价格，为当前价格的95%和股票的跌停价中的较高者
            # 下一个限价单，目标是将该股票的持仓数量减少到0，即全部卖出，使用上面计算的限制价格
            # LimitOrderStyle是下单的方式，这里指定了限制价格
            ordert = order_target_value(security,0, LimitOrderStyle(limit_price))
            # 如果下单失败（例如，如果股票停牌或不存在），则记录失败信息到日志
            if (None == ordert):
                log.info("Sell failed %s" % (current_data[security].name))
    #else:
    #    log.info("no one to sell")
    return

# 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.high_limit_list = []    # 初始化全局变量 g.high_limit_list 为空列表，用于存储昨日涨停的股票列表
    hold_list = list(context.portfolio.positions)  # 将持仓股票代码转换为列表
    if hold_list:
        # 获取该股票昨日的收盘价和涨停价，
        df = get_price(hold_list, end_date=context.previous_date, frequency='daily',
                       fields=['close', 'high_limit'],
                       count=1, panel=False)
        # g.high_limit_list 将被赋值为所有收盘价等于涨停价的股票代码
        g.high_limit_list = df[df['close'] == df['high_limit']]['code'].tolist()

#  调整昨日涨停股票
def check_limit_up(context):
    # 获取当前的股票市场数据，包括价格、涨跌停信息等
    cdata = get_current_data()
    sell_list = []    # 初始化一个空列表，用于存储需要卖出的股票代码
    # 检查全局变量g.high_limit_list是否存在，这个列表包含了昨日涨停的股票代码
    if g.high_limit_list:
        # 遍历昨日涨停的股票列表
        for stock in g.high_limit_list:
            # 检查当前股票的最新价格是否低于涨停价
            if cdata[stock].last_price < cdata[stock].high_limit:
                # 记录日志信息，表示该股票涨停已经打开，准备卖出
                log.info("[%s]涨停打开，卖出" % cdata[stock].name)
                # 注释掉的order_target函数可以用于卖出股票，这里没有执行
                # order_target(stock, 0)
                # 将打开涨停的股票加入到卖出列表中
                sell_list.append(stock)
            else:
                # 如果股票仍然处于涨停状态，则记录日志信息，表示继续持有
                log.info("[%s]涨停，继续持有" % cdata[stock].name)
    # 如果卖出列表不为空，即有股票需要卖出
    if (len(sell_list) > 0):
        # 调用sellstock函数，传入context和sell_list，执行卖出操作
        sellstock(context, sell_list)
 
# 尾盘买卖股；放量未涨停的卖出；低位的买进
def check_at_dayend(context):
    btlist = context.portfolio.positions    # 获取当前投资组合中所有持仓的股票
    cdata=get_current_data()    # 获取当前的股票市场数据，包括价格、涨跌停信息等
    # 调用VOL函数计算所有持仓股票的成交量情况
    # VOL函数用于计算成交量的倍数（VOLT），以及5日和10日成交量的均值（MAVOL5, MAVOL10）
    # check_date参数为策略当前的日期，M1和M2分别为计算均值的周期
    # include_now=True表示包括当前的交易数据
    VOLT,MAVOL5,MAVOL10 = VOL(btlist, check_date=context.current_dt, M1=5, M2=10, include_now = True)
    sell_list = []   # 初始化一个空列表，用于存储需要卖出的股票代码
    for stock in btlist:
        # 如果股票当前价格已经达到涨停价，则跳过不处理
        if (cdata[stock].last_price == cdata[stock].high_limit):
            continue
        # 检查股票是否放量，这里放量定义为成交量超过过去10日成交量均值的3倍
        if (VOLT[stock]>MAVOL10[stock]*3): #放量未涨停
            log.info("[%s]放量未涨停，卖出" % cdata[stock].name)
            sell_list.append(stock)     # 将放量未涨停的股票加入到卖出列表中
    sellstock(context, sell_list)    # 如果卖出列表不为空，即有股票需要卖出，调用sellstock函数执行卖出操作
    # 调用buystock函数，传入context和g.buylist，执行买入操作
    # g.buylist是一个全局变量，应该在策略的其他部分被定义，包含了需要买入的股票列表
    buystock(context, g.buylist)

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

# 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	
	# 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
	# 使用列表推导式，返回不在涨停状态或已持有的股票列表
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < current_data[stock].high_limit]

# 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
	current_data = get_current_data() # 获取当前所有股票的数据
	# 使用列表推导式，返回不在跌停状态或已持有的股票列表
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] > current_data[stock].low_limit]

#2-4 过滤股价高于9元的股票	
def filter_highprice_stock(context,stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()   # 使用列表推导式
			or last_prices[stock][-1] < 9]
