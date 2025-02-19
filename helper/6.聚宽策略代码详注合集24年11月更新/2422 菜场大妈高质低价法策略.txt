# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40850
# 标题：菜场大妈！皇城大妈！年化108%！大妈吉祥！
# 作者：Clarence.罗

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40139
# 标题：正黄旗大妈选股法，修改版，年化108%
# 作者：clarence 罗 感谢下列各位大咖
# 这是Python 2 版本，回测请调到Python 2，一创可以运行
# 但请注意PE为负值，和利润大幅下降的股票

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40811
# 标题：策略分析辅助--实时最大回撤记录
# 作者：Bingyou

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40139
# 标题：正黄旗大妈选股法，修改版，年化92%
# 作者：oupian 1211修改

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40004
# 标题：菜场大妈选股法
# 作者：美吉姆优秀毕业代表 贴在社区下面的

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

# 注意：回测请选择python 2  

import pandas as pd   # 导入pandas库并设置别名pd
from jqdata import *  # 从jqdata模块导入所有内容
from jqlib.technical_analysis import *  # 从jqlib.technical_analysis模块中导入所有函数、类和变量

def initialize(context):
    log.set_level('order', 'error')    # 过滤order中低于error级别的日志
    set_option('use_real_price', True)    # 用真实价格交易
    # set_option('avoid_future_data', True)
    set_benchmark('000905.XSHG')    # 设定基准
    set_slippage(PriceRelatedSlippage(0.000))     # 设置滑点    
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='fund')

    # 初始化全局变量
    g.stock_num = 10  # 设置要持有的股票数量为10
    g.buylist = []  # 初始化买入股票列表
    g.new_high_value = context.portfolio.starting_cash  # 设置资金量新高值为初始资金
    g.maxdown = 0  # 初始化最大回撤为0
    g.new_high_value = 0  # 重置资金量新高值

    run_daily(get_high_limit_stocks, time='9:05', reference_security='000300.XSHG') # 设置每天9:05运行 get_high_limit_stocks 函数，用于获取涨停股票
    run_monthly(select_stocks_and_buy,1,time='9:30')  # 每月派息好股，按小市值排序建立了g.buylist；派息不怎么变，市值在变; 每个月第一个交易日9:30运行
    run_daily(sell_stocks_opened_from_up_limit, time='14:00') # 设置每天14:00运行 sell_stocks_opened_from_up_limit 函数，用于卖出前一日涨停但当日打开的股票
    run_daily(sell_hi_vol_stocks_at_dayend_and_buy_again, time='14:30')  #  设置每天14:30运行 sell_hi_vol_stocks_at_dayend_and_buy_again 函数，用于卖出涨停但成交量不放大的股票，并再次买入
    #问题是买入了刚卖出的股.
    run_monthly(analyze_stocks_held, 1, time='15:01')  # 设置每月第一个交易日的15:01运行 analyze_stocks_held 函数，用于分析持仓股票的表现和财务状况

# 1-1 根据最近一年分红除以当前总市值计算股息率并筛选    
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
    #PY3 DR = pd.concat([dividend, cap] ,axis=1, sort=False)
    # 合并分红数据和市值数据，并计算股息率
    DR = pd.concat([dividend, cap], axis=1)
    DR['dividend_ratio'] = (DR['bonus_amount_rmb']/10000) / DR['market_cap']
    # 根据股息率进行排序并筛选
    #PY3 DR = DR.sort_values(by=['dividend_ratio'], ascending=sort)
    DR = DR.sort(columns='dividend_ratio', ascending=False)
    final_list = list(DR.index)[int(p1*len(DR)):int(p2*len(DR))]    # 根据比例 p1 和 p2 计算筛选的股票列表
    return final_list    # 返回筛选后的股票列表

# 选择股票并执行买入操作
def select_stocks_and_buy(context):
    select_stocks(context) # 调用 select_stocks 函数，该函数负责根据一定的策略筛选股票，筛选逻辑在该函数内部实现，可能会涉及到市场数据、财务数据等的分析
    
    choice=g.buylist  # 从全局变量 g.buylist 中获取筛选后的股票列表
    # Sell
    #sell_list = []
    #for s in context.portfolio.positions:
    #    if (s not in choice) :
    #        sell_list.append(s)
    #sell_stocks(context, sell_list)

    # buy
    buy_stocks(context, g.buylist)  # 调用 buy_stocks 函数，传入 context 和筛选后的股票列表 g.buylist

# 定义 select_stocks 函数，用于根据特定条件筛选股票
def select_stocks(context):
    dt_last = context.previous_date    # 获取上一个交易日期
    stocks = get_all_securities('stock', dt_last).index.tolist()    # 获取所有上市股票的列表

    stocks = filter_kcbj_stock(stocks)    # 排除科创板及创业板股票
    #stocks = filter_st_stock(stocks)
    #stocks = filter_paused_stock(stocks)
    #stocks = filter_limitup_stock(context,stocks)
    #stocks = filter_limitdown_stock(context,stocks)
    #stocks = filter_highprice_stock(context,stocks)
    
    # 根据股息率筛选股票，选择股息率位于全市场最高的25%的股票
    stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.25)
    # 获取基本面数据，包括PEG比率和PB-ROE比率
    q = query(valuation.code,  # 指定查询的股票代码字段
                valuation.pe_ratio / indicator.inc_net_profit_year_on_year,  # 计算PEG比率
                indicator.roe / valuation.pb_ratio,  # 计算PB-ROE比率
                indicator.roe ,  # 净资产收益率(ROE)
                ).filter(
                    valuation.pe_ratio / indicator.inc_net_profit_year_on_year>-1,
                    valuation.pe_ratio / indicator.inc_net_profit_year_on_year<3,
                    #indicator.roe / valuation.pb_ratio > 0,
                    valuation.code.in_(stocks))
    df_fundamentals = get_fundamentals(q, date = None)  # 执行查询并获取基本面数据
    stocks = list(df_fundamentals.code)  # 提取股票代码列表
    # fuandamental data
    #df = get_fundamentals(query(valuation.code).filter(valuation.code.in_(stocks)).order_by(valuation.market_cap.asc()))
    #df = get_fundamentals(query(valuation.code,valuation.market_cap).filter(valuation.code.in_(stocks)).order_by(valuation.market_cap.asc()))
    # 获取市值数据，筛选市值不超过100亿的股票
    q = query(valuation.code,  # 指定查询的股票代码字段
              valuation.market_cap  # 指定查询的股票市值字段
                ).filter(
                    valuation.code.in_(stocks),  # 过滤条件：股票代码在之前筛选出的股票列表
                    valuation.market_cap<=100).order_by(valuation.market_cap.asc())      # 过滤条件：股票市值不超过100亿，排序条件：按照市值升序排序
    df = get_fundamentals(q, date = None)  # 执行查询并获取市值数据
    print(df.shape)  # 打印DataFrame的形状，即股票数量和列数
    choice = list(df.code)    # 提取筛选后的股票代码列表
    choice = filter_st_stock(choice)    # 排除ST股票
    choice = filter_paused_stock(choice)    # 排除停牌股票
    choice = filter_limitup_stock(context,choice)    # 排除涨停股票
    choice = filter_limitdown_stock(context,choice)    # 排除跌停股票
    choice = filter_highprice_stock(context,choice)    # 排除高价股票
    choice = choice[:g.stock_num*2]     # 选择最终的股票列表，数量为设定的目标持股数量的两倍
    g.buylist = choice    # 更新全局变量 g.buylist 为筛选后的股票列表
################End select_stocks_and_buy

# 定义 buy_stocks 函数，用于购买股票
def buy_stocks(context, choice):
    position_count = len(context.portfolio.positions)    # 获取当前投资组合中的股票持仓数量
    # 如果当前持仓数量已经达到预设的目标持仓数，则直接返回，不进行购买操作
    if g.stock_num <= position_count:
        return
    buylist = choice    # 初始化购买列表为传入的选择股票列表
    #buylist = []
    #K,D = KD(choice, check_date=context.previous_date, N = 9, M1 = 3, M2 = 3)
    #VOLT,MAVOL10,MAVOL20 = VOL(choice, check_date=context.current_dt, M1=10, M2=20, include_now = True)
    #VOLT,MAVOL5,MAVOL20 = VOL(choice, check_date=context.current_dt, M1=5, M2=20, include_now = True)
    #for s in choice:
        #K值小于25 #20日均量不能超过10日均量的1.3倍；10日均量不能超过50日均量的1.3倍；说明前期未明显放量
        #if (K[s] > 25) or (MAVOL20[s] > MAVOL10[s]*1.3) or (MAVOL10[s] > MAVOL5[s]*1.8):
        #    continue
    #    buylist.append(s)
    cdata = get_current_data()    # 获取当前的市场数据
    #namelist = []
    #for s in buylist:
    #    namelist.append(cdata[s].name)
    #log.info('bulist:', namelist)    
    # 计算每只股票的购买金额，即总可用资金除以剩余需要购买的股票数量
    psize = context.portfolio.available_cash/(g.stock_num - position_count)
    for s in buylist:    # 遍历购买列表，对每只股票执行购买操作
        if s not in context.portfolio.positions:     # 如果股票不在当前持仓中，则执行购买
            log.info('buy', s, cdata[s].name)    # 记录购买操作
            order_value(s, psize)      # 调用 order_value 函数购买股票，数量为计算出的 psize
            if len(context.portfolio.positions) == g.stock_num:      # 如果当前持仓数量达到目标持仓数，则停止购买
                break
################End buy_stocks

# 定义 sell_stocks 函数，用于卖出指定的股票列表
def sell_stocks(context, sell_list):
    current_data=get_current_data()    # 获取当前市场数据
    if len(sell_list)>0:    # 检查是否有股票需要卖出
        for security in sell_list:      # 遍历需要卖出的股票列表
            cprice = current_data[security].last_price     # 获取当前股票的市场价格
            boughtcost = context.portfolio.positions[security].avg_cost    # 获取持仓成本
            if context.portfolio.positions[security].avg_cost==0:   # 如果持仓成本为0，记录错误并设置利润为0
                log.error("Sell %s " % (current_data[security].name), "avg_cost is 0")
                profit = 0
            else:
                profit = (cprice - boughtcost)/boughtcost *100      # 计算卖出利润百分比
            # 记录卖出操作的信息，包括股票名称、预估利润和初始购买时间
            log.info("Sell %s " % (current_data[security].name), "profit: %.1f%%" % profit, "init time %s" % context.portfolio.positions[security].init_time)
            limit_price = max(cprice*0.95,current_data[security].low_limit)      # 设置限价单的价格为当前价格的95%或当天跌停价中的较高者
            ordert = order_target_value(security,0, LimitOrderStyle(limit_price))    # 下达限价卖出指令，目标是将该股票的持仓数量降至0
            if (None == ordert):     # 如果下单失败（即order_target_value返回None），记录失败信息
                log.info("Sell failed %s" % (current_data[security].name))
    #else:
    #    log.info("no one to sell")
    return    # 函数没有返回值，因为操作是直接在策略上下文中执行的
################End sell_stocks

# 准备涨停股票池，用于获取上一交易日以涨停价收盘的股票列表
def get_high_limit_stocks(context):
    g.high_limit_list = []    # 初始化一个空列表，用于存储涨停股票的代码
    hold_list = list(context.portfolio.positions)    # 获取当前持有的股票列表
    if hold_list:    # 如果当前持有股票
        #panel = get_price(hold_list, end_date=context.previous_date, frequency='daily',
        #               fields=['close', 'high_limit'],
        #               count=1) # , panel=False)
        #PY3 g.high_limit_list = df[df['close'] == df['high_limit']]['code'].tolist()
        for stock in hold_list:    # 遍历当前持有的股票列表
            # 获取每只股票的上一交易日的涨停价和收盘价
            df=get_price(stock, count = 1, end_date=context.previous_date, frequency='daily', fields=['high_limit', 'close'])
            if df['high_limit'][-1]==df['close'][-1]:      # 检查该股票是否以涨停价收盘，即收盘价是否等于涨停价
                g.high_limit_list.append(stock)   # 如果是，则将该股票代码添加到涨停股票列表中

#  调整昨日涨停股票，用于卖出昨日涨停但今日未继续涨停的股票
def sell_stocks_opened_from_up_limit(context):
    cdata = get_current_data()    # 获取当前市场数据
    sell_list = []      # 初始化一个空列表，用于存储需要卖出的股票代码
    if len(g.high_limit_list)>0:    # 检查是否已有昨日涨停的股票列表
        for stock in g.high_limit_list:     # 遍历昨日涨停的股票列表
            if cdata[stock].last_price < cdata[stock].high_limit:     # 检查今日的实时价格是否低于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % cdata[stock].name)      # 如果是，记录卖出信息
                #order_target(stock, 0)
                sell_list.append(stock)     # 将该股票代码添加到卖出列表
                #if stock in g.buylist:
                #    g.buylist.remove(stock)
            else:
                log.info("[%s]涨停，继续持有" % cdata[stock].name)# 如果今日继续涨停，则记录持有信息
    if sell_list:   # 如果卖出列表不为空，执行卖出操作
    #    select_stocks(context) 
    #    for stock in sell_list: 
    #        if stock in g.buylist: 
    #            g.buylist.remove(stock)

        sell_stocks(context, sell_list)    # 调用 sell_stocks 函数，传入策略上下文和需要卖出的股票列表
    #    buy_stocks(context, g.buylist)
 
# 尾盘买卖股；放量未涨停的卖出；低位的买进；用于执行尾盘买卖策略
def sell_hi_vol_stocks_at_dayend_and_buy_again(context):
    btlist = context.portfolio.positions    # 获取当前投资组合中的股票持仓
    cdata=get_current_data()
    #PY3 VOLT,MAVOL5,MAVOL10 = VOL(btlist, check_date=context.current_dt, M1=5, M2=10, include_now = True)
    sell_list = []    # 初始化卖出列表
    for stock in btlist:    # 遍历当前持仓中的每只股票
        if (cdata[stock].last_price == cdata[stock].high_limit):   # 如果股票当前价格已经达到涨停价，则跳过不处理
            continue
        stock_now_vol = now_vol(context, stock)     # 获取当前股票的成交量
        stock_ma10_vol = ma_vol(context, stock, 10)    # 获取过去10个交易日的平均成交量
        if (stock_now_vol>stock_ma10_vol*3):     # 如果当前成交量超过过去10日均量的3倍，即放量未涨停
            log.info("[%s]放量未涨停，卖出" % cdata[stock].name)    # 记录卖出信息
            sell_list.append(stock)      # 将该股票添加到卖出列表
    if sell_list:     # 如果有股票需要卖出，执行卖出操作
    #    select_stocks(context) 
    #    for stock in sell_list: 
    #        if stock in g.buylist: 
    #            g.buylist.remove(stock)
        sell_stocks(context, sell_list)    # 调用 sell_stocks 函数，传入策略上下文和需要卖出的股票列表
    buy_stocks(context, g.buylist)   # 调用 buy_stocks 函数，传入策略上下文和股票池列表 g.buylist，执行买入操作

# 定义函数 ma_vol，用于计算指定股票在给定天数内的平均成交量
def ma_vol(context, stock, number_of_days):   
    # 使用 get_price 函数获取指定股票在过去 number_of_days 天的成交量数据
    # end_date 参数设置为 context.previous_date，即上一个交易日
    # frequency 设置为 'daily'，表示获取日频率的数据
    # count 设置为 number_of_days，表示需要获取的天数
    # fields 设置为 ['volume']，表示需要获取的字段是成交量
    df_vol = get_price(stock, end_date=context.previous_date, frequency='daily', count=number_of_days, fields=['volume'])
    ft_ma_vol = df_vol['volume'].mean() # df_vol['volume'] 是一个包含指定股票在过去 number_of_days 天成交量的 pandas Series
    #print (stock, number_of_days, 'volume mean', ft_ma_vol) 
    return ft_ma_vol         # 函数返回计算得到的平均成交量

# 定义 now_vol 函数，用于计算指定股票在当前交易日的总成交量
def now_vol(context, stock):  
    # 计算今天的日期零点时间点，从当前时间中减去当前的时、分、秒和微秒，得到当天的零点时间
    dt_zero_clock_today = context.current_dt - datetime.timedelta(hours=context.current_dt.hour, minutes=context.current_dt.minute, seconds=context.current_dt.second, microseconds=context.current_dt.microsecond)
    #print ('dt_zero_clock_today', dt_zero_clock_today, type(dt_zero_clock_today))
    # 计算今天的开盘时间点，从零点时间点开始加上9小时15分钟，即股市开盘时间
    dt_trading_start_today = dt_zero_clock_today + datetime.timedelta(hours=9, minutes=15, seconds=00)
    #print ('dt_trading_start_today', dt_trading_start_today, type(dt_trading_start_today))
    # 获取股票从开盘到当前时刻的分钟频率的价格数据，特别是成交量字段
    df_vol = get_price(stock, start_date=dt_trading_start_today, end_date=context.current_dt, frequency='minute', fields=['volume'])
    ft_now_vol = df_vol['volume'].sum()# 计算从开盘到当前时刻的总成交量，对获取到的成交量数据进行求和操作
    #print (stock, 'ft_now_vol', ft_now_vol)
    return ft_now_vol       # 返回计算得到的当前总成交量

# 定义 analyze_stocks_held 函数，用于分析当前持仓股票的财务指标和市场表现
def analyze_stocks_held(context):
    current_data = get_current_data()     # 获取当前市场数据
    hold_stocks = context.portfolio.positions.keys()     # 获取当前投资组合中的持仓股票列表
    for s in hold_stocks:       # 遍历当前持仓中的每只股票
        # 构建查询对象：股票代码            市值               市盈率              净利润同比增长率                      # 执行查询并获取股票的财务数据 过滤条件，只查询当前股票的数据
        q = query(valuation.code,valuation.market_cap,valuation.pe_ratio, indicator.inc_net_profit_year_on_year).filter(valuation.code == s)
        df = get_fundamentals(q)     # 执行查询并获取股票的财务数据
        # log.info(s,current_data[s].name,'流值',df['circulating_market_cap'][0],'亿')
        #log.info(s,current_data[s].name,'市值',df['market_cap'][0],'亿')
        #log.info(s,current_data[s].name,'股价',current_data[s].last_price,'元')
        log.info(s,current_data[s].name,'市盈率',df['pe_ratio'][0])   # 记录股票的市盈率信息
        # 记录股票的净利润同比增长率信息
        log.info(s,current_data[s].name,'净利润同比增长率',df['inc_net_profit_year_on_year'][0])
        #inc_net_profit_year_on_year
        #净利润同比增长率(%)	
        #（当期的净利润-上年当期的净利润）/上年当期的净利润绝对值=净利润同比增长率。
        #log.info('##############################################################')
        #inc_net_profit_year_on_year
        #净利润同比增长率(%)	
        #（当期的净利润-上年当期的净利润）/上年当期的净利润绝对值=净利润同比增长率。
    log.info('一天结束')
    log.info('##############################################################')

# 定义函数 after_trading_end，它在每个交易日结束后被调用
def after_trading_end(context):
    g.total_value = context.portfolio.total_value    # 获取当前策略的总资产价值，并将其赋值给全局变量 g.total_value
    # 若总资金大于昨天纪录的资金新高值，则将资金新高值变为今日总资金，令最大回撤值归0；否则计算今日总资金的回撤值
    if g.total_value > g.new_high_value:   
        g.new_high_value = g.total_value       # 更新资金新高值 g.new_high_value 为当前的总资产价值
        g.maxdown = 0     # 将最大回撤值 g.maxdown 重置为 0，因为已经达到了新高
    else:
        # 计算回撤值：(之前的最高资产价值 - 当前资产价值) / 之前的最高资产价值 * 100
        max_down = (g.new_high_value - g.total_value)/g.new_high_value*100
        # 更新最大回撤值 g.maxdown：如果新的回撤值 max_down 大于已有的最大回撤值 g.maxdown，则用新的回撤值更新最大回撤值；否则保持最大回撤值不变
        g.maxdown = max_down if max_down > g.maxdown else g.maxdown
    record(maxdown=g.maxdown)   # 使用 record 函数记录当前的最大回撤值，可以在聚宽平台的图表中查看这个指标
    
# 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

# 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]  # 使用列表推导式，返回不在停牌状态的股票列表

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
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < current_data[stock].high_limit]

# 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
	current_data = get_current_data() # 获取当前所有股票的数据
	# 使用列表推导式，返回不在跌停状态或已持有的股票列表
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] > current_data[stock].low_limit]

# 过滤股价高于9元的股票	
def filter_highprice_stock(context,stock_list):
	# 使用 history 函数获取 stock_list 中所有股票的最新（最近一分钟）收盘价
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < 9]
