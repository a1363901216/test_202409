# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40346
# 标题：尝试用机器学习批量生产小盘策略
# 作者：wywy1995
# 回测资金 200000

#导入函数库
from jqdata import *  # 导入技术分析库
from jqfactor import *    # 导入因子分析库
import numpy as np  # 导numpy库并设置别名np
import pandas as pd  # 导入pandas库并设置别名pd

#初始化函数 
def initialize(context):
    # 设定基准
    set_benchmark('399303.XSHE')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 设置交易成本万分之三，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='stock')
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    #初始化全局变量
    g.stock_num = 10     # 初始股票数量
    g.hold_list = []     # 当前持仓的全部股票    
    g.yesterday_HL_list = [] # 记录持仓中昨日涨停的股票
    g.factor_list = [        # 获取聚宽数据中因子数据
        'price_no_fq', # 技术指标因子 不复权价格因子
        'total_profit_to_cost_ratio', # 质量类因子 成本费用利润率
        'inventory_turnover_rate' # 质量类因子 存货周转率
        ]
    # 设置交易运行时间
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')     # 每天09:05 运行 prepare_stock_list
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')  # 每周第一个交易日的09:30 运行 weekly_adjustment
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')        # 每天14:00 运行 check_limit_up  检查持仓中的涨停股是否需要卖出
    run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')   # 每天15:10 运行 print_position_info

# 定义一个函数，用于准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []  # 初始化一个空列表，用于存储当前持有的股票代码。
    for position in list(context.portfolio.positions.values()):  # 遍历当前投资组合中的股票持仓。
        stock = position.security  # 获取持仓股票代码。
        g.hold_list.append(stock)  # 将股票代码添加到持有列表中
    #获取昨日涨停列表
    if g.hold_list != []:   # 如果持有列表不为空，则获取昨日涨停的股票列表
        # 使用get_price函数获取持有股票列表在昨日的收盘价和涨停价。
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]  # 筛选出收盘价等于涨停价的股票，即昨日涨停的股票
        g.yesterday_HL_list = list(df.code)       # 将涨停股票的代码列表存储在全局变量中。
    else:
        g.yesterday_HL_list = []  # 如果持有列表为空，则设置昨日涨停股票列表为空

# 定义一个函数，用于选股，即筛选出符合特定条件的股票
def get_stock_list(context):
    yesterday = context.previous_date  # 获取昨日日期。
    initial_list = get_all_securities().index.tolist()  # 获取所有股票的列表。
    initial_list = filter_new_stock(context, initial_list)  # 筛选出新上市的股票。
    initial_list = filter_kcbj_stock(initial_list)  # 筛选出科创板的股票。
    initial_list = filter_st_stock(initial_list)  # 筛选出ST股票。
    factor_values = get_factor_values(initial_list, [       # 获取因子值，这里使用了三个因子，具体因子名称存储在g.factor_list中。
        g.factor_list[0],
        g.factor_list[1],
        g.factor_list[2],
    ], end_date=yesterday, count=1)
    df = pd.DataFrame(index=initial_list, columns=factor_values.keys())    # 创建一个DataFrame，用于存储因子值
    # 将因子值填充到DataFrame中。
    df[g.factor_list[0]] = list(factor_values[g.factor_list[0]].T.iloc[:, 0])
    df[g.factor_list[1]] = list(factor_values[g.factor_list[1]].T.iloc[:, 0])
    df[g.factor_list[2]] = list(factor_values[g.factor_list[2]].T.iloc[:, 0])
    df = df.dropna()    # 去除含有缺失值的股票
    # 定义因子的系数列表，用于计算股票的综合得分。
    coef_list = [
        -6.123355346008858e-05,
        -0.002579342458393642,
        -2.194257357346814e-06
    ]
    # 计算每只股票的综合得分。
    df['total_score'] = coef_list[0] * df[g.factor_list[0]] + coef_list[1] * df[g.factor_list[1]] + coef_list[2] * df[g.factor_list[2]]
    df = df.sort_values(by=['total_score'], ascending=False)    # 根据综合得分对股票进行降序排序
    complex_factor_list = list(df.index)[:int(0.1 * len(list(df.index)))]    # 选择得分最高的前10%的股票
    # 使用SQL查询获取特定股票的财务数据。
    q = query(valuation.code, valuation.circulating_market_cap, indicator.eps).filter(
        valuation.code.in_(complex_factor_list)
    ).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)    # 获取财务数据，并存储到DataFrame中。
    df = df[df['eps'] > 0]    # 筛选出每股收益（EPS）大于0的股票。
    final_list = list(df.code)    # 获取最终的股票列表。
    return final_list

#1-3 整体调整持仓
def weekly_adjustment(context):
    #获取应买入列表 
    target_list = get_stock_list(context)  # 获取get_stock_list中的股票列表
    target_list = filter_paused_stock(target_list)  # 过滤停牌股票
    target_list = filter_limitup_stock(context, target_list)  # 过滤涨停的股票
    target_list = filter_limitdown_stock(context, target_list)  # 过滤跌停的股票
    #截取不超过最大持仓数的股票量
    target_list = target_list[:min(g.stock_num, len(target_list))]
    # 调仓卖出逻辑
    for stock in g.hold_list:  # 遍历当前持有的股票列表
        if (stock not in target_list) and (stock not in g.yesterday_HL_list):  # 如果股票不在目标买入列表中，并且不在昨日涨停或跌停的股票列表中
            log.info("卖出[%s]" % (stock))                 # 记录卖出信息
            position = context.portfolio.positions[stock]  # 获取股票当前的持仓情况
            close_position(position)                       # 卖出股票持仓，调用close_position 函数，用于平仓
        else:
            log.info("已持有[%s]" % (stock))               # 记录持有信息
    #调仓买入
    position_count = len(context.portfolio.positions)      # 当前持仓的股票数量
    target_num = len(target_list)                          # 目标买入的股票数量
    if target_num > position_count:                        # 如果目标买入的股票数量大于当前持仓数量
        value = context.portfolio.cash / (target_num - position_count)  # 计算每只股票应分配的资金
        for stock in target_list:                          # 遍历目标买入的股票列表
            if context.portfolio.positions[stock].total_amount == 0:    # 如果当前股票不在持仓中
                if open_position(stock, value):            # 买入股票，调用open_position 函数，用于开仓
                    if len(context.portfolio.positions) == target_num:  # 如果持仓数量等于目标数量，则退出买入循环
                        break   # 中断循环

#1-4 定义检查昨日涨停股票的函数 ；调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt   # 获取当前时间
    if g.yesterday_HL_list != []:   # 检查昨日涨停股票列表是否为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.yesterday_HL_list:  # 遍历昨日涨停股票列表
            # 获取当前股票的最新价格和涨停价
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0,0] <    current_data.iloc[0,1]:   # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))             # 如果打开涨停，则记录卖出信息
                position = context.portfolio.positions[stock]        # 获取股票当前的持仓情况
                close_position(position)                             # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % (stock))             # 如果仍然涨停，则记录继续持有的信息

#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if not current_data[stock].paused]  # 使用列表推导式，返回不在停牌状态的股票列表

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()       # 获取当前所有股票的数据
    return [stock for stock in stock_list   # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
            if not current_data[stock].is_st
            and 'ST' not in current_data[stock].name
            and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name]

#2-3 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:   # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':   # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)   # 从列表中移除该股票
    return stock_list

#2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
    current_data = get_current_data() # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() # 使用列表推导式，返回不在涨停状态或已持有的股票列表
            or last_prices[stock][-1] <    current_data[stock].high_limit]

#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
    current_data = get_current_data() # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() # 使用列表推导式，返回不在跌停状态或已持有的股票列表
            or last_prices[stock][-1] > current_data[stock].low_limit]

#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date # 获取昨日日期
    # 使用列表推导式，返回上市时间超过375天的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date <    datetime.timedelta(days=375)]

#3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:   # 检查目标价值是否为0，即是否需要清仓
        log.debug("Selling out %s" % (security)) # 日志记录清仓信息
    else:
        log.debug("Order %s to value %f" % (security, value)) # 日志记录下单到特定价值的信息
    return order_target_value(security, value)  # 调用聚宽平台的order_target_value函数执行交易

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

#4-3 打印每日持仓信息
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