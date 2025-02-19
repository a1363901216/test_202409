# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/41763
# 标题：挖掘中国特色的估值体系投资机会，年化150%+
# 作者：子匀

# 回测资金 500000

#导入函数库
from jqdata import *                          # 导入聚宽函数库
from jqfactor import get_factor_values        # 导入因子库
import numpy as np
import pandas as pd

def initialize(context):
    # 设定基准
    set_benchmark('000300.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 设置交易成本万分之三，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, 
                            close_today_commission=0, min_commission=5),type='stock')
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')      
    log.set_level('system', 'error')
    #初始化全局变量
    g.stock_num = 4        # 最大持仓数
    g.limit_up_list = []   # 记录持仓中涨停的股票
    g.hold_list = []       # 当前持仓的全部股票
    g.limit_days = 20      # 不再买入的时间段天数
    g.target_list = []     # 开盘前预操作股票池
    # 调用调度函数，设置定时任务
    do_schedule(context)

# 代码变更后的回调函数
def after_code_changed(context):
    # 取消所有定时运行
    unschedule_all()
    # 重新调用调度函数，设置新的定时任务
    do_schedule(context)

# 调度函数，用于设置策略的定时任务
def do_schedule(context):    
    # 设置交易日8:00运行get_stock_list函数，准备预操作股票池
    run_daily(get_stock_list, time='8:00', reference_security='000300.XSHG')                    
    # 设置交易日8:05运行prepare_trade函数，准备预操作股票池
    run_daily(prepare_trade, time='8:05', reference_security='000300.XSHG')                   
    # 设置交易日14:00运行check_limit_up函数，检查持仓中的涨停股是否需要卖出
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')                   
    # 设置每周一9:30运行weekly_adjustment函数，进行周度调仓
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')     
    # 设置每周一15:10运行print_position_info函数，打印复盘信息
    run_weekly(print_position_info, weekday=1, time='15:10', reference_security='000300.XSHG')  
    
#1-1 选股模块
def get_stock_list(context):
    yesterday = context.previous_date  # 获取上一交易日的日期
    # 直属国企，央企
    stocklists = ['601919.XSHG', '300073.XSHE', '600536.XSHG', '000951.XSHE', '601628.XSHG', '600036.XSHG', \
            '601818.XSHG', '001289.XSHE', '601111.XSHG', '600787.XSHG', '688396.XSHG', '601611.XSHG', '600795.XSHG',\
            '601390.XSHG', '600489.XSHG', '600007.XSHG', '600938.XSHG', '688187.XSHG', '600026.XSHG', '601898.XSHG', \
            '000066.XSHE', '600862.XSHG', '601658.XSHG', '300374.XSHE', '600900.XSHG', '601881.XSHG', '000999.XSHE', \
            '000009.XSHE', '601106.XSHG', '000928.XSHE', '000797.XSHE', '003816.XSHE', '688779.XSHG', '000807.XSHE', \
            '600845.XSHG', '601965.XSHG', '600158.XSHG', '601319.XSHG', '601989.XSHG', '600916.XSHG', '601668.XSHG',\
            '600760.XSHG', '601398.XSHG', '600905.XSHG', '600176.XSHG', '000996.XSHE', '601766.XSHG', '601328.XSHG', \
            '600028.XSHG', '601808.XSHG', '600150.XSHG', '601800.XSHG', '600875.XSHG', '600486.XSHG', '600030.XSHG', \
            '600685.XSHG', '000777.XSHE', '600970.XSHG', '000617.XSHE', '601336.XSHG', '600019.XSHG', '001979.XSHE', \
            '000733.XSHE', '002080.XSHE', '002013.XSHE', '601318.XSHG', '601117.XSHG', '000927.XSHE', '600705.XSHG', \
            '600737.XSHG', '600977.XSHG', '601857.XSHG', '600372.XSHG', '600061.XSHG', '600025.XSHG', '601995.XSHG', \
            '601618.XSHG', '600730.XSHG', '000877.XSHE', '600406.XSHG', '601888.XSHG', '601006.XSHG', '000786.XSHE',\
            '000166.XSHE', '601985.XSHG', '601601.XSHG', '601816.XSHG', '601179.XSHG', '600050.XSHG', '000758.XSHE', \
            '601088.XSHG', '601868.XSHG', '601598.XSHG', '601698.XSHG', '000625.XSHE', '000629.XSHE', '601186.XSHG', \
            '000768.XSHE', '002401.XSHE', '601858.XSHG', '000069.XSHE', '600999.XSHG', '002179.XSHE', '601872.XSHG', \
            '000799.XSHE', '601728.XSHG', '601600.XSHG', '601788.XSHG', '600764.XSHG', '600886.XSHG', '000708.XSHE',\
            '600056.XSHG', '600011.XSHG', '600893.XSHG', '600941.XSHG', '002268.XSHE', '601236.XSHG', '002415.XSHE', \
            '600048.XSHG', '600027.XSHG', '601939.XSHG', '600118.XSHG', '002116.XSHE', '601988.XSHG', '002051.XSHE', \
            '000800.XSHE', '601998.XSHG', '600765.XSHG', '300140.XSHE', '603126.XSHG', '601288.XSHG', '600115.XSHG', \
            '601669.XSHG', '600029.XSHG', '002916.XSHE', '301269.XSHE', '600482.XSHG']

    stocklists = filter_st_stock(stocklists)  # 过滤st股票
    # 2. 剔除预期增长的后15%
    # 获取这些股票的'growth'因子值
    factor_data = get_factor_values(securities=stocklists, factors=['growth'], end_date=yesterday,count=1)['growth'].iloc[0]
    # 按照'growth'因子值降序排序，并获取排序后的索引列表
    growth_list = factor_data.sort_values(ascending=False).index.tolist()
    # 剔除预期增长的后15%的股票
    growth_list = growth_list[:int(len(growth_list) * 0.80)]
    # 3.按PE、PB复合排序
    # 获取筛选后股票列表的估值数据，包括市盈率(PE)和市净率(PB)
    df=get_valuation(growth_list,  end_date=yesterday, fields=['pe_ratio','pb_ratio'], count=1).set_index('code')
    # 4.行业比重
    df['sw_code']= ''    # 初始化'sw_code'列为空字符串
    dict1=get_industry(security=growth_list, date=context.previous_date)  # 获取这些股票的行业代码
    for stock in growth_list :  # 将行业代码填充到'sw_code'列
        df.loc[stock,'sw_code'] = dict1[stock].get('sw_l1')['industry_code']
    # 5 只留下前五年表现极差的四傻 801180	房地产I	801780	银行I	801790	非银金融I	801720	建筑装饰I	    
    df = df[df['sw_code'].isin(['801180','801780','801790','801720'])]    # 筛选特定行业的公司
    # 计算行业内市净率的排名
    df['dense'] = df.groupby('sw_code')['pb_ratio'].rank(method='min', ascending=True, pct=True)
    # 计算综合得分，市净率排名占80%，市盈率排名占20%
    df['score'] = df['dense'] * 0.8 + df['pe_ratio'].rank(method='min', ascending=True, pct=True) * 0.2

    # 按综合得分升序排序，并获取排序后的索引列表
    pb_list = (df.sort_values('score', ascending=True)).index.tolist()
    # 将得分最高的前N+2只股票（N为最大持仓数）作为目标股票列表
    g.target_list = pb_list[:g.stock_num + 2]
    return g.target_list    # 返回目标股票列表

#1-3 准备交易，推送信息
def prepare_trade(context):
    # 1. 获取当前持仓列表，并更新全局变量g.hold_list
    g.hold_list= list(context.portfolio.positions.keys())
    # 2. 获取昨日涨停列表，并初始化全局变量g.high_limit_list
    g.high_limit_list = []
    if g.hold_list != []:    # 如果当前有持仓
        # 获取这些持仓股票昨日的收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        # 筛选出昨日收盘价等于涨停价的股票，即昨日涨停的股票
        df = df[df['close'] == df['high_limit']]
        # 更新全局变量g.high_limit_list为昨日涨停的股票列表
        g.high_limit_list = list(df.code)

# 定义weekly_adjustment函数，用于整体调整持仓
def weekly_adjustment(context):
    # 过滤掉暂停交易的股票
    g.target_list = filter_paused_stock(g.target_list)
    # 过滤掉仍在涨停的股票
    g.target_list = filter_limitup_stock(context, g.target_list)
    # 过滤掉仍在跌停的股票
    g.target_list = filter_limitdown_stock(context, g.target_list)
    #过滤最近买过且涨停过的股票
    black_list = get_recent_limit_up_stock(context, g.target_list, g.limit_days)
    # 从目标列表中移除黑名单中的股票
    g.target_list = [stock for stock in g.target_list if stock not in black_list]
    # 调仓卖出，卖出不再目标列表中的股票，且不是昨日涨停的股票
    for stock in g.hold_list:
        if (stock not in g.target_list) and (stock not in g.high_limit_list):
            close_position(stock)
    # 调仓买入：如果当前持仓数量小于最大持仓数，则买入新的股票
    position_count = len(context.portfolio.positions)
    # target_num = len(g.target_list)
    if position_count < g.stock_num:
        value = context.portfolio.cash / (g.stock_num - position_count)  # 计算每只股票的买入金额
        for stock in g.target_list:
            if stock not in context.portfolio.positions.keys():  # 如果股票不在当前持仓中
                if open_position(stock, value):   # 尝试买入股票
                    if len(context.portfolio.positions) >= g.stock_num:  # 如果持仓数量达到最大持仓数，则停止买入
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
                close_position(stock)      # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % (stock))   # 如果仍然涨停，则记录继续持有的信息

#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()       # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].is_st]  # 使用列表推导式

#2-3 获取最近N个交易日内有涨停的股票
def get_recent_limit_up_stock(context, stock_list, recent_days):
    stat_date = context.previous_date  # 获取策略运行前一天的日期，这通常是回测或模拟交易中的最后一个交易日
    new_list = []  # 初始化一个空列表，用于存储有过涨停记录的股票代码
    # 使用get_price函数获取每只股票在最近N个交易日内的收盘价和涨停价
    new_list = get_price(stock_list, end_date=stat_date, frequency='daily', fields=['close','high_limit'], count=recent_days, 
        panel=False, fill_paused=False).query('close==high_limit').code.tolist()
    return new_list       # 函数返回包含有过涨停记录的股票代码的列表

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

#2-6 过滤科创板
def filter_kcb_stock(context, stock_list):
    return [stock for stock in stock_list  if stock[0:3] != '688']  # 使用列表推导式

#2-7 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过250日的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=250)]

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
# 	security = position.security      # 获取持仓的股票代码
	order = order_target_value_(position, 0)   # 下单清仓 ；可能会因停牌失败
	if order != None:     # 检查订单是否创建成功
		if order.status == OrderStatus.held and order.filled == order.amount:    # 检查订单状态是否为全部成交且订单数量等于下单数量
			return True    # 平仓成功
	return False           # 平仓失败

#5-1 复盘模块-打印
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
        msg = '代码:{}'.format(securities)
        msg2= '  成本价:{}'.format(format(cost,'.2f'))
        msg3= '  现价:{}'.format(price)
        msg4= '  收益率:{}%'.format(format(ret,'.2f'))
        msg5= '  持仓(股):{}'.format(amount)
        msg6= '  市值:{}'.format(format(value,'.2f'))
        print(msg + msg2+ msg3+msg4+msg5+msg6)
    print('———————————————————————————————————————分割线————————————————————————————————————————')
