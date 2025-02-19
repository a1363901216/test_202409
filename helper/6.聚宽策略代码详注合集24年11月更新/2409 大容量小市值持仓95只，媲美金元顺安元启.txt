# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/39814
# 标题：持仓95只大容量小市值，媲美金元顺安元启
# 作者：开心果
# 回测资金需要 1000000

import pandas as pd

def initialize(context):
    # setting
    log.set_level('order', 'error')  # 过滤掉order系列API产生的比error级别低的log
    set_option('use_real_price', True) # 开启动态复权模式(真实价格)
    set_option('avoid_future_data', True) # 开启避免未来数据模式
    set_benchmark('000905.XSHG')  # 设置基准股票
    # strategy
    g.stock_num = 95   
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')  # 每日9:05运行，筛选出上一交易日涨停的持仓股票列表
    run_monthly(my_Trader, 1 ,time='9:30')  # 每月第一个交易日9:30运行，用于全市场选股买入卖出调仓操作
    run_daily(check_limit_up, time='14:00') # 每日14:00运行，卖出持仓股票中上一交易日涨停而现在没有涨停的股票
                
def my_Trader(context):
    # all stocks
    dt_last = context.previous_date # 获取上一个交易日的日期
    stocks = get_all_securities('stock', dt_last).index.tolist()  # 获取市场中上一交易日所有股票的代码；.tolist()：它将股票代码的索引转换成一个列表。
    # filter, ST
    stocks = filter_kcbj_stock(stocks) # 过滤科创北交股票
    stocks = filter_st_stock(stocks)   # 过滤ST及其他具有退市标签的股票
    df = get_fundamentals(query(     # get_fundamentals查询股票的财务数据，并筛选出符合条件的股票
            valuation.code           # 市值
        ).filter(
            valuation.code.in_(stocks), # 筛选valuation.code股票代码在stocks中
            valuation.pb_ratio > 0,     # 市净率PB
            indicator.inc_return >0,    # 净资产收益率(扣除非经常损益)(%)
            indicator.inc_total_revenue_year_on_year>0,  # 营业总收入同比增长率(%)
            indicator.inc_net_profit_year_on_year>0,     # 净利润同比增长率(%)
            indicator.ocf_to_operating_profit>5,         # 经营活动产生的现金流量净额/经营活动净收益(%)
        ).order_by(
            valuation.market_cap.asc()   # 选出的股票按市值升序排序
		).limit(
		    g.stock_num           # 返回不超过g.stock_num个数据
	    ))
    log.info("所有股票参数的列表" ,'\n%s'%df )
    choice = list(df.code)   # 获取df中的股票代码
    # 卖出交易
    for s in context.portfolio.positions:
        if (s  not in choice) and (s not in g.high_limit_list):   # 如果此股票不在本次选股列表，也不在涨停股票中
            order_target(s, 0)  # 卖出这只持仓股票,使这只股票的最终持有量为0
            #print('卖出',s)
    # 买入交易
    psize = 1.0/g.stock_num * context.portfolio.total_value  # 计算出买入每只股票所需要金额
    for s in choice:
        if context.portfolio.available_cash < psize:  # 如果帐号内的现金小于购买每只股票所要的资金
            break
        if s not in context.portfolio.positions:   # 如果此股票不在账户持仓列表中
            order_value(s, psize)  # 买入价值为psize的这只股票。
            #print('买入',s)

#1-3 准备涨停的股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.high_limit_list = []  #新建一个g.high_limit_list空列表
    # 聚宽中con...tions,是一个字典（dict），其中的键（key）是股票代码，值（value）是 Position 对象，Position 对象包含了该股票的持仓详情，比如持有数量、平均成本、当前价值等。
    hold_list = list(context.portfolio.positions)  # 获取持仓股票的代码列表
    if hold_list:
        df = get_price(hold_list, end_date=context.previous_date, frequency='daily',   # 获取hold_list中股票上一交易日的行情数据
                       fields=['close', 'high_limit', 'paused'],
                       count=1, panel=False)
        # 从df中筛选出那些收盘价是涨停价并且没有停牌的股票，并将这些股票的代码存储到全局变量 g.high_limit_list 中，作为一个列表。
        g.high_limit_list = df.query('close==high_limit and paused==0')['code'].tolist() 
        
# 1-5 调整昨日涨停股票
def check_limit_up(context):
     # 获取持仓的昨日涨停列表
    current_data = get_current_data() # 获取当前单位时间（当天/当前分钟）的涨跌停价, 是否停牌，当天的开盘价等。
    if g.high_limit_list:
        for stock in g.high_limit_list:  # 遍历涨停股票列表
            if current_data[stock].last_price < current_data[stock].high_limit:  # 如果当前价小于涨停价
                log.info("[%s]涨停打开，卖出" % stock)
                order_target(stock, 0)  # 卖出这只持仓股票,使这只股票的最终持有量为0
            else:
                log.info("[%s]涨停，继续持有" % stock)
 
 
#2-6 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':
            stock_list.remove(stock)
    return stock_list

# 2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()  # 获取当前单位时间（当天/当前分钟）的涨跌停价, 是否停牌，当天的开盘价等
    return [stock for stock in stock_list if not (
            current_data[stock].is_st or
            'ST' in current_data[stock].name or
            '*' in current_data[stock].name or
            '退' in current_data[stock].name)]
            
# end