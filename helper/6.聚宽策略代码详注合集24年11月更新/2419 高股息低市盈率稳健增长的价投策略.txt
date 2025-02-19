# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/45552
# 标题：高股息低市盈率高增长的价投策略
# 作者：芹菜1303
# 原回测时间选择分钟，如果选择每天，回测速度就会更快

'''
0、股票筛选所有股票，去科创板和北交所，去ST，去上市未满300天；
1、股票筛选和排序：最近3年平均分红股息率最大10%的股票，按分红/股价 从大到小排序；（股价上涨，会导致排名下跌）
2、股票筛选：PEG在0.08-2之间，市盈率：0~20之间，净资产收益率>3%,营业收入同比增长率>5%,净资产同比增长>11%;（股价上涨到PE超过25就会跌出股票池）
3、选取的股票：去停牌，去昨天涨停的，取前10个股票做为买入股票池（股价）
4、执行频率每月执行一次；
5、每天执行的是：如果昨天是涨停的，今天没有涨停，则卖出；
'''
import pandas as pd   # 导入pandas库并设置别名pd
from jqdata import *  # 从jqdata模块导入所有内容

def initialize(context):
    set_benchmark('000300.XSHG')    # 设定基准
    log.set_level('order', 'error')    # 过滤order中低于error级别的日志
    set_option('use_real_price', True)    # 用真实价格交易
    set_option('avoid_future_data', True)    # 打开防未来函数
    set_slippage(FixedSlippage(0.02))     # 设置滑点    
    g.stock_num = 10  # 设置要持有的股票数量
    g.month=context.current_dt.month-1  # 获取上一个月份

# 开盘前运行，在每个交易日的开盘前运行，用于执行一些开盘前需要准备的工作
def before_trading_start(context):
    prepare_stock_list(context)   # 调用 prepare_stock_list 函数
    print(context.run_params.type)   # 打印策略运行参数中的类型，了解策略是在进行回测还是模拟交易

# 开盘时运行函数
def handle_data(context,data):
    hour=context.current_dt.hour          # 读取当前时间的小时部分
    minute=context.current_dt.minute      # 读取当前时间的分钟部分
    #选股程序每月执行一次，检查是否到达新的月份，并且当前时间是9:30（开盘时）
    if context.current_dt.month !=g.month and hour==9 and minute==30:
        my_Trader(context)       # 调用 my_Trader 函数执行选股和交易逻辑
        g.month=context.current_dt.month     # 更新全局变量 g.month 为当前月份，这样选股程序下个月才会再次执行
    #-----------------------执行频率的设置和股票筛选程序----------------------
    if hour==14 and minute==0:    # 检查当前时间是否是14:00
        check_limit_up(context)   # 调用 check_limit_up 函数，卖出昨天涨停今天开板的股票
    # 显示可动用现金与总资产的比例，这里记录的是现金占总资产的比例
    record(cash=context.portfolio.available_cash/context.portfolio.total_value*100)

# my_Trader 函数，用于执行策略的交易逻辑
def my_Trader(context):
    dt_last = context.previous_date    # 获取策略运行前一天的日期
    stocks = get_all_securities('stock', dt_last).index.tolist() # 获取所有股票的列表，并转换为列表形式
    stocks = filter_kcbj_stock(stocks)  # 过滤掉科创板和北交所的股票
    stocks = filter_st_stock(stocks)    # 过滤掉ST股票
    stocks = filter_new_stock(context, stocks) # 去除上市未满300天的新股
    stocks = choice_try_A(context,stocks) # 根据基本面进行选股
    stocks = filter_paused_stock(stocks)  # 过滤掉停牌的股票
    stocks = filter_limit_stock(context,stocks)[:g.stock_num] # 去除涨停的股票，并确保股票数量不超过预设的最大持股数 g.stock_num
    cdata = get_current_data()  # 获取当前所有股票的数据
    slist(context,stocks)   # 调用 slist 函数
    # 卖出逻辑
    for s in context.portfolio.positions:   # 遍历当前投资组合中的所有持仓股票
        # 检查如果某只股票不在预设的股票池中，并且当前价格低于涨停价
        if (s  not in stocks) and (cdata[s].last_price <  cdata[s].high_limit):
            log.info('Sell', s, cdata[s].name)       # 记录卖出操作的日志信息，包括股票代码、股票名称
            order_target(s, 0)      # 下单将该股票的持仓数量目标设置为0，即全部卖出
    # 买入部分
    position_count = len(context.portfolio.positions)  # 计算当前持仓数量
    if g.stock_num > position_count:   # 检查预设的股票数量是否大于当前持仓数量
        psize = context.portfolio.available_cash/(g.stock_num - position_count)  # 计算每只股票的购买资金，即剩余资金平均分配到每只股票上
        for s in stocks:    # 遍历预设的股票池
            if s not in context.portfolio.positions:        # 如果某只股票不在当前持仓中
                log.info('buy', s, cdata[s].name)    # 记录买入操作的日志信息，包括股票代码、股票名称
                order_value(s, psize)    # 下单购买该股票，购买金额为之前计算的psize
                if len(context.portfolio.positions) == g.stock_num:      # 如果持仓数量达到了预设的股票数量，则停止购买
                    break

#显示筛查出股票的：名称，代码，市值
def slist(context,stock_list):    
    current_data = get_current_data()   # 获取当前所有股票的数据
    for stock in stock_list:    # 遍历股票列表中的每只股票
        # 获取单只股票的财务数据，这里只获取了估值表（valuation）的数据，且筛选条件是股票代码等于当前遍历到的股票代码
        df = get_fundamentals(query(valuation).filter(valuation.code == stock))
        # 打印股票的详细信息，包括：股票代码：使用格式化字符串中的占位符 {0} 来表示；# 股票名称：调用 get_security_info 函数获取股票信息对象，然后通过 display_name 属性获取股票的名称；总市值：从 df（DataFrame）中获取 market_cap 列的值，索引为 [0]，表示获取第一行的数据
        # 流通市值：从 df 中获取 circulating_market_cap 列的值；PE（市盈率）：从 df 中获取 pe_ratio 列的值；股价：从 current_data 中获取 last_price 属性，表示当前时间点的股票最新价格；.2f 是格式化说明符，表示浮点数保留两位小数
        print('股票代码：{0},  名称：{1},  总市值:{2:.2f},  流通市值:{3:.2f},  PE:{4:.2f},股价：{5:.2f}'.format(stock,get_security_info(stock).display_name,df['market_cap'][0],df['circulating_market_cap'][0],df['pe_ratio'][0],current_data[stock].last_price))

#1-1 准备股票池
# 如果持有的股票昨日涨停，则将其加入涨停列表，以便在今日监控是否打开涨停，一旦打开涨停就卖出，这个每天执行
def prepare_stock_list(context):
    g.hold_list= []    # 初始化全局变量 g.hold_list 为空列表，用于存储当前持有的股票列表
    g.high_limit_list=[]    # 初始化全局变量 g.high_limit_list 为空列表，用于存储昨日涨停的股票列表
    for position in list(context.portfolio.positions.values()):    # 遍历当前投资组合中的所有持仓
        stock = position.security          # 获取持仓的股票代码
        g.hold_list.append(stock)         # 将股票代码添加到 g.hold_list 列表中
    #获取昨日涨停列表
    if g.hold_list != []:    # 如果 g.hold_list 不为空，即有持仓股票
        for stock in g.hold_list:        # 遍历 g.hold_list 中的每只股票
            # 获取该股票昨日的收盘价和涨停价，
            df = get_price(stock, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1)
            if df['close'][0] >= df['high_limit'][0]*0.98:  # 检查昨日的收盘价是否接近涨停价（这里使用 0.98 作为阈值）
                g.high_limit_list.append(stock)     # 将昨日涨停的股票添加到 g.high_limit_list 列表中
    
#1-5 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt  # 获取当前时间
    if g.high_limit_list != []:   # 检查昨日涨停股票列表是否为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.high_limit_list:   # 遍历昨日涨停股票列表
            current_data = get_current_data()   # 获取当前所有股票的数据
            if current_data[stock].last_price <   current_data[stock].high_limit:  # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))     # 如果打开涨停，则记录卖出信息
                order_target(stock, 0)     # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % (stock))    # 如果仍然涨停，则记录继续持有的信息
 
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
def filter_limit_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	# 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or current_data[stock].low_limit < last_prices[stock][-1] < current_data[stock].high_limit]

#1-1 根据最近三年分红除以当前总市值计算股息率并筛选
def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date    # 获取策略运行的上一个日期
    time0 = time1 - datetime.timedelta(days=365*3)      # 计算三年前日期，用于获取分红数据
    #获取分红数据，由于finance.run_query最多返回4000行，以防未来数据超限，最好把stock_list拆分后查询再组合
    interval = 1000 #某只股票可能一年内多次分红，导致其所占行数大于1，所以interval不要取满4000
    list_len = len(stock_list)
    # 初始化查询分红数据的 Query 对象
    #               股票代码                        股权登记日                     每股分红金额（人民币）
    q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date, finance.STK_XR_XD.bonus_amount_rmb
    ).filter(
        finance.STK_XR_XD.a_registration_date >= time0, # 股权登记日大于等于三年前
        finance.STK_XR_XD.a_registration_date <= time1, # 股权登记日小于等于上一个日期
        finance.STK_XR_XD.code.in_(stock_list[:min(list_len, interval)]))  # 股票代码在列表中
    df = finance.run_query(q)    # 执行查询，获取分红数据
    #对interval的部分分别查询并拼接
    if list_len > interval:      # 如果股票列表长度大于设定的查询间隔，则分批查询
        df_num = list_len // interval
        for i in range(df_num):
            q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date,  finance.STK_XR_XD.bonus_amount_rmb
            ).filter(
                finance.STK_XR_XD.a_registration_date >= time0, # 股权登记日大于等于三年前
                finance.STK_XR_XD.a_registration_date <= time1, # 股权登记日小于等于上一个日期
                finance.STK_XR_XD.code.in_(stock_list[interval*(i+1):min(list_len,interval*(i+2))]))
            temp_df = finance.run_query(q)
            df = df.append(temp_df)
    # 处理分红数据，填充空值为0，并按股票代码分组求和
    dividend = df.fillna(0) # df.fillna() 是一个 Pandas 数据处理库中的函数，它可以用来填充数据框中的空值
    dividend = dividend.groupby('code').sum()
    temp_list = list(dividend.index) #query查询不到无分红信息的股票，所以temp_list长度会小于stock_list
    #获取市值相关数据 股票代码      总市值
    q = query(valuation.code,valuation.market_cap).filter(valuation.code.in_(temp_list))
    cap = get_fundamentals(q, date=time1)  # 执行查询，获取市值数据
    cap = cap.set_index('code')  # 将市值数据设置为以股票代码为索引的 DataFrame
    # 计算股息率（分红金额除以总市值）
    cap['dividend_ratio']=(dividend['bonus_amount_rmb']/10000)/cap['market_cap']
    # 根据股息率进行排序并筛选
    cap = cap.sort_values(by=['dividend_ratio'], ascending=sort)
    final_list = list(cap.index)[int(p1*len(cap)):int(p2*len(cap))] # 计算筛选比例，p1 和 p2 为筛选的上下界比例
    return final_list      # 返回筛选后的股票列表
	
# 过滤次新股
def filter_new_stock(context, stock_list):
    # 使用列表推导式来构建一个新的列表，包含满足特定条件的股票
     # 对于 stock_list 中的每只股票，如果满足以下条件，则将其包含在新列表中：条件是：(策略运行的上一个日期 - 300天) 必须大于该股票的上市日期
    return [stock for stock in stock_list if (context.previous_date - datetime.timedelta(days=300)) > get_security_info(stock).start_date]

def choice_try_A(context,stocks):
    # 调用 get_dividend_ratio_filter_list 函数来筛选股票，基于股息率进行排序，# False 表示按股息率降序排列，0 和 0.1 表示选择股息率在前10%的股票
    stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.1)    #股息率排序
    #  使用聚宽的 query 函数构建一个查询，获取特定财务数据
    df = get_fundamentals(query(
            valuation.code,  # 股票代码
            valuation.circulating_market_cap,  # 流通市值
        ).filter(
            valuation.code.in_(stocks),  # 股票代码在筛选后的股票列表中
            valuation.pe_ratio.between(0,25),   # 市盈率在0到25之间
            indicator.inc_return >3,     # 净资产收益率(扣除非经常损益)(%)大于3
            indicator.inc_total_revenue_year_on_year>5,    # 营业总收入同比增长率(%)大于5
            indicator.inc_net_profit_year_on_year>11,    # 净利润同比增长率大于11%
            valuation.pe_ratio / indicator.inc_net_profit_year_on_year>0.08,    # 市盈率与净利润同比增长率的比值大于0.08
            valuation.pe_ratio / indicator.inc_net_profit_year_on_year<1.9,     # 市盈率与净利润同比增长率的比值小于1.9
            ))
    stocks = list(df.code)   # 提取符合条件的股票代码列表
    print("分红比率筛选后的股票有：{}".format(len(stocks)))  # 打印筛选后的股票数量
    return stocks    # 返回筛选后的股票列表
