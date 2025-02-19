# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42029
# 标题：大资金优质股策略，总收益1217%！无小市值因子！
# 作者：hello friends
# 回测资金  10000000

# 从 jqfactor 模块导入 neutralize 函数
from jqfactor import neutralize
# neutralize 函数用于对因子数据进行中性化处理，以减少某些因子带来的影响，例如市值因子。
# 从 jqfactor 模块导入 winsorize 函数
from jqfactor import winsorize
# winsorize 函数用于对数据进行缩尾处理，将数据中极端的值限制在一个合理的范围内，以减少极端值对分析的影响。
# 从 jqfactor 模块导入 standardlize 函数
from jqfactor import standardlize
# standardlize 函数用于对数据进行标准化处理，使得数据符合标准正态分布，即均值为0，标准差为1。
# 导入 pandas 库
import pandas as pd
# pandas 是一个强大的数据处理和分析工具库，用于处理和分析结构化数据。
# 导入 statsmodels 库
import statsmodels.api as sm
# statsmodels 是一个统计建模库，用于进行统计分析和建模，例如线性回归、时间序列分析等。
# 从 jqdata 模块导入所有内容
from jqdata import *
# jqdata 是聚宽平台提供的数据接口模块，用于获取股票、基金、期货等金融产品的历史数据。
# 从 jqfactor 模块导入 get_factor_values 函数
from jqfactor import get_factor_values
# get_factor_values 函数用于获取指定股票池和因子列表的因子值数据，通常用于量化选股和回测。
# 导入 datetime 库
import datetime
# datetime 库提供了日期和时间处理的功能，可以方便地进行日期时间的计算和格式化。

# 初始化函数
def initialize(context):
    # 设定基准
    set_benchmark('000905.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')

    # 初始化全局变量
    g.stock_num = 8    # 持股数
    g.limit_days = 20  # 用来检查最近20天内列表中有涨停的股票
    g.hold_list = []
    # 设置交易时间，每天运行
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')
    run_monthly(monthly_adjustment, monthday=1, time='9:30', reference_security='000300.XSHG')
    # 每周获取股票列表，去除最近20天内曾经涨停过的和曾经买过的股，去掉下跌趋势明显的
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')
    #把昨日涨停今天没涨停的股票卖出


# 1-2 选股模块：根据因子找出股票
def get_stock_list(context):
    yesterday=context.previous_date     # 获取前一个交易日的日期
    # 去除次新股：获取前375天（大约一年前）就已经上市的股票列表
    by_date = context.previous_date - datetime.timedelta(days=375)
    initial_list = get_all_securities(date=by_date).index.tolist()
    # 过滤掉科创板股票和ST股票
    initial_list = filter_kcb_stock(initial_list)
    sample = filter_st_stock(initial_list)
    # 构建查询对象，用于查询股票的财务数据和市场因子
    q=query(valuation.code,
        (cash_flow.net_operate_cash_flow-cash_flow.subtotal_invest_cash_outflow)/100000000,      # 自由现金流（亿元）
        (cash_flow.net_operate_cash_flow-cash_flow.subtotal_invest_cash_outflow)/(valuation.market_cap*100000000),    # FCF/市值
        indicator.roe,     # 净资产收益率(ROE)
        indicator.net_profit_margin,
        indicator.inc_net_profit_year_on_year,
        valuation.circulating_market_cap
       ).filter(valuation.code.in_(sample),      # 只选择过滤后的股票样本
               (cash_flow.net_operate_cash_flow-cash_flow.subtotal_invest_cash_outflow)/(valuation.market_cap*100000000)>0.02,   # FCF/市值大于2%
               indicator.adjusted_profit>0,   # 季度扣非利润大于0
               indicator.roe>3,            # 季度ROE大于3%
               indicator.net_profit_margin>10,   # 净利率大于10%
               indicator.inc_net_profit_to_shareholders_year_on_year>5     # 归母净利同比增长大于5%
       ).order_by(
        (cash_flow.net_operate_cash_flow-cash_flow.subtotal_invest_cash_outflow)/(valuation.market_cap*100000000).desc()    # 按FCF/市值降序排列
       )
    # 执行查询，获取股票的财务数据和市场因子
    df=get_fundamentals(q,date=None)
    # 重命名列，方便理解
    df.columns=['code', '自由现金流（亿元）','FCF/市值(排序列)','roe','净利率','归母净利润YOY','流通市值']
    df.index=df.code.values
    del df['code']     # 删除code列，不再需要
    df
    # 去极值、中性化、标准化处理因子
    factors_list=['FCF/市值(排序列)','roe']
    df['score']=0     # 初始化得分列为0
    for factor in factors_list:
        df[factor]=winsorize(df[factor], qrange=[0.05,0.93], inclusive=True, inf2nan=True, axis=0)    # 去极值处理
        df[factor]=neutralize(df[factor], how=['jq_l1', 'market_cap'], date=None, axis=0)     # 中性化处理                                                                                 #是否有未来函数？    
        df[factor]=standardlize(df[factor], inf2nan=True, axis=0)     # 标准化处理
        df['score']=df['score']+df[factor]
    # 按得分排序，取分数最高的前10个股票
    df3=df.sort_values("score",ascending=False).iloc[:10]
    WNS1_list=df3.index.tolist()       # 将选出的股票代码存入列表
    print('选股结果：',len(WNS1_list))  # 打印选股结果数量
    return WNS1_list  # 返回选出的股票列表

# 1-3 准备股票池
def prepare_stock_list(context):
    # 获取已持有列表
    g.hold_list = list(context.portfolio.positions)
    # 初始化持仓列表，g.hold_list 用于存储当前持仓的股票代码
    g.high_limit_list = []
    if g.hold_list:
        #  获取每只股票的昨日收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily',
                       fields=['close', 'high_limit', 'paused'],
                       count=1, panel=False)
        g.high_limit_list = df.query('close==high_limit and paused==0')['code'].tolist()  # paused为0表示不停牌

# 1-4 整体调整持仓  
def monthly_adjustment(context):
    # 获取应买入列表
    target_list = get_stock_list(context)   #  获取股票列表
    target_list = filter_paused_stock(target_list)  # 过滤停牌的股票
    target_list = filter_limit_stock(context, target_list)  # 过滤涨停的股票
    # 调仓：不在列表，昨日未涨停的持仓票卖出。
    for stock in g.hold_list:    # 遍历当前持有的股票列表
        if (stock not in target_list) and (stock not in g.high_limit_list): # 如果股票不在目标买入列表中，并且不在昨日涨停或跌停的股票列表中
            log.info("卖出[%s]" % (stock))                 # 记录卖出信息
            position = context.portfolio.positions[stock]  # 获取股票当前的持仓情况
            close_position(position)                       # 卖出股票持仓，调用close_position 函数，用于平仓
        else:
            log.info("已持有[%s]" % stock)
    #调仓买入逻辑
    position_count = len(context.portfolio.positions)      # 当前持仓的股票数量
    target_num = g.stock_num                               # 目标买入的股票数量
    if target_num > position_count:                        # 如果目标买入的股票数量大于当前持仓数量
        value =  context.portfolio.available_cash / (target_num - position_count)  # 计算每只股票应分配的资金
        for stock in target_list:                          # 遍历目标买入的股票列表
            if stock not in context.portfolio.positions:    # 如果当前股票不在持仓中
                if open_position(stock, value):           # 买入股票，调用open_position 函数，用于开仓
                    if len(context.portfolio.positions) >= g.stock_num:  # 如果持仓数量大于等于目标数量，则退出买入循环
                        break   # 中断循环

# 1-5 调整昨日涨停股票
def check_limit_up(context):
    current_data = get_current_data()       # 获取当前所有股票的数据
    if g.high_limit_list:      # if list: list非0非空，则为true
        for stock in g.high_limit_list:  # 遍历昨日涨停股票列表
            if current_data[stock].last_price < current_data[stock].high_limit:   # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))             # 如果打开涨停，则记录卖出信息
                position = context.portfolio.positions[stock]        # 获取股票当前的持仓情况
                close_position(position)                             # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % stock)               # 如果仍然涨停，则记录继续持有的信息


# 2-1 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

# 2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()       # 获取当前所有股票的数据
    return [stock for stock in stock_list if not (    # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
            current_data[stock].is_st or
            'ST' in current_data[stock].name or
            '*' in current_data[stock].name or
            '退' in current_data[stock].name)]

# 2-4 过滤涨停的股票
def filter_limit_stock(context, stock_list):
    # type: (Context, list) -> list
    current_data = get_current_data()         # 获取当前所有股票的数据
    holdings = list(context.portfolio.positions)
    return [stock for stock in stock_list if (stock in holdings) or
            current_data[stock].low_limit < current_data[stock].last_price < current_data[stock].high_limit]

# 2-6 过滤科创板
def filter_kcb_stock(stock_list):
    return [stock for stock in stock_list if not stock.startswith('68')]

# 3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:   # 检查目标价值是否为0，即是否需要清仓
        log.debug("Selling out %s" % (security)) # 日志记录清仓信息
    else:
        log.debug("Order %s to value %f" % (security, value)) # 日志记录下单到特定价值的信息
    return order_target_value(security, value)  # 调用聚宽平台的order_target_value函数执行交易

# 3-2 交易模块-开仓
def open_position(security, value):
    _order = order_target_value_(security, value)  # 尝试对指定股票进行下单到特定价值
    if _order is not None and _order.filled > 0:          # 检查订单是否创建成功且有成交
        return True  # 开仓成功
    return False  # 开仓失败

# 3-3 交易模块-平仓
def close_position(position):
    security = position.security  # 获取持仓的股票代码
    _order = order_target_value_(security, 0)    # 下单清仓 ；可能会因停牌失败
    if _order is not None:    # 检查订单是否创建成功
        if _order.status == OrderStatus.held and _order.filled == _order.amount:    # 检查订单状态是否为全部成交且订单数量等于下单数量
            return True  # 平仓成功
    return False  # 平仓失败
