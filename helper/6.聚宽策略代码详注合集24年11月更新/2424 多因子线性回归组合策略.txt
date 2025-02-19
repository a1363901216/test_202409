# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42371
# 标题：凑波热闹，也来试试多因子线性回归
# 作者：Plisking

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42310
# 标题：wywy大佬的差不多得了策略三只股最优版
# 作者：chalengr4

#https://www.joinquant.com/view/community/detail/30684f8d65a74eef0d704239f0eec8be?type=1&page=2

#导入函数库
from jqdata import *     # 导入聚宽量化平台的函数库
from jqfactor import *   # 导入聚宽因子库
import numpy as np       # 导入numpy库，用于数据处理
import pandas as pd      # 导入pandas库，用于数据处理

#初始化函数 
def initialize(context):
    # 设定中证500指数为策略的基准
    set_benchmark('000905.XSHG')
    # 使用真实价格进行交易，即不使用前复权价格
    set_option('use_real_price', True)
    # 打开避免未来数据的选项，防止策略中出现未来函数
    set_option("avoid_future_data", True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0))
    # 设置交易成本万分之三，包括印花税和佣金
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='stock')
    # 设置日志级别，只记录error级别的日志，避免太多无用信息
    log.set_level('order', 'error')
    #初始化全局变量
    g.no_trading_today_signal = False  # 标志变量，用于指示今日是否发出交易信号
    g.stock_num = 1  # 策略持仓的股票数量
    g.hold_list = []  # 当前持仓的全部股票列表
    g.yesterday_HL_list = []  # 记录持仓中昨日涨停的股票
    # 定义因子列表，每个因子列表包含因子名称和对应的系数
    g.factor_list = [
        # 第一组因子：情绪类因子 ARBR，质量类因子 SGAI、净利润与营业总收入之比，每股未分配利润
        (#ARBR-SGAI-NPtTORttm-RPps
            [
                'ARBR', # 情绪类因子 ARBR
                'SGAI', # 质量类因子 销售管理费用指数
                'net_profit_to_total_operate_revenue_ttm', # 质量类因子 净利润与营业总收入之比
                'retained_profit_per_share'                # 每股指标因子 每股未分配利润
            ],
            [
                -0.00015399364219672028,  # 对应 ARBR 因子的系数
                0.0068040696770965275,    # 对应 SGAI 因子的系数
                -0.013582394749579795,    # 对应 净利润与营业总收入之比 因子的系数
                -0.05043296392026463      # 对应 每股未分配利润 因子的系数
            ]
            # 后续还有两组因子和系数，每组包含不同的因子和对应的系数
        ),
        (#P1Y-TPtCR-VOL120
            [
                'Price1Y', # 动量类因子 当前股价除以过去一年股价均值再减1
                'total_profit_to_cost_ratio', # 质量类因子 成本费用利润率
                'VOL120' # 情绪类因子 120日平均换手率
            ],
            [
              -1.6481969388084845,
              -0.17062057099935446,
              -0.061842557079243125
            ]
        ),
        (#DtA-OCtORR-DAVOL20-PNF-SG
            [
                'debt_to_assets', # 风格因子 资产负债率
                'operating_cost_to_operating_revenue_ratio', # 质量类因子 销售成本率
                'DAVOL20',     # 情绪类因子 20日平均换手率与120日平均换手率之比
                'price_no_fq', # 技术指标因子 不复权价格因子
                'sales_growth' # 风格因子 5年营业收入增长率
            ],
            [
              0.058175841938529524,
              -0.1910332189773409,
              -0.2736912625714264,
              -0.027468330345688075,
              0.11887746662741136
            ]
        )
    ]

    # g.factor_list = [
    #     (#ARBR-SGAI-NPtTORttm-RPps
    #         [
    #             'ARBR', #情绪类因子 ARBR
    #             'SGAI', #质量类因子 销售管理费用指数
    #             'net_profit_to_total_operate_revenue_ttm', #质量类因子 净利润与营业总收入之比
    #             'retained_profit_per_share' #每股指标因子 每股未分配利润
    #         ],
    #         [
    #             -2.3425,
    #             -694.7936,
    #             -170.0463,
    #             -1362.5762
    #         ]
    #     ),
    #     (#P1Y-TPtCR-VOL120
    #         [
    #             'Price1Y', #动量类因子 当前股价除以过去一年股价均值再减1
    #             'total_profit_to_cost_ratio', #质量类因子 成本费用利润率
    #             'VOL120' #情绪类因子 120日平均换手率
    #         ],
    #         [
    #             -0.0647128120839873,
    #             -0.006385116279168804,
    #             -0.0029867925845833217
    #         ]
    #     ),
    #     (#DtA-OCtORR-DAVOL20-PNF-SG
    #         [
    #             'debt_to_assets', #风格因子 资产负债率
    #             'operating_cost_to_operating_revenue_ratio', #质量类因子 销售成本率
    #             'DAVOL20', #情绪类因子 20日平均换手率与120日平均换手率之比
    #             'price_no_fq', #技术指标因子 不复权价格因子
    #             'sales_growth' #风格因子 5年营业收入增长率
    #         ],
    #         [
    #             0.04477354820057883,
    #             0.021636407482421707,
    #             -0.01864268317469762,
    #             -0.0004678118383947827,
    #             0.02884867440332058
    #         ]
    #     )
    # ]
    # 设置交易运行时间
    run_daily(prepare_stock_list, '9:05')      # 每天运行
    run_weekly(weekly_adjustment, 1, '9:30')   # 每周第一个交易日运行
    run_daily(check_limit_up, '14:00')         # 检查持仓中的涨停股是否需要卖出
    run_daily(close_account, '14:30')          # 每天运行
    run_daily(print_position_info, '15:10')    # 每天运行

#1-1 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []   # 初始化持仓列表，g.hold_list 用于存储当前持仓的股票代码
    for position in list(context.portfolio.positions.values()):   # 遍历当前持仓的每个持仓价值
        stock = position.security    # 获取股票代码
        g.hold_list.append(stock)    # 将股票代码添加到持仓列表
    #获取昨日涨停列表
    if g.hold_list != []:            # 如果当前持仓列表不为空，即存在持仓
        # 获取每只股票的昨日收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]  # 筛选出昨日收盘价等于涨停价的股票，即昨日涨停的股票
        g.yesterday_HL_list = list(df.code)       # 更新昨日涨停列表，存储为股票代码列表
    else:
        g.yesterday_HL_list = []                  # 如果没有持仓，昨日涨停列表为空
    # 判断今天是否为账户资金再平衡的日期，使用 today_is_between 函数判断当前日期是否在 '04-05' 到 '04-30' 之间
    g.no_trading_today_signal = today_is_between(context, '04-05', '04-30')
    
#1-2 选股模块
def get_stock_list(context):
    # 获取昨天的日期，用于获取历史数据，防止未来函数
    yesterday = context.previous_date
    # 获取今天的日期
    today = context.current_dt
    # 获取所有股票的初始列表
    initial_list = get_all_securities('stock', today).index.tolist()
    # 过滤新上市的股票
    initial_list = filter_new_stock(context, initial_list)
    # 过滤科创板及创业板股票
    initial_list = filter_kcbj_stock(initial_list)
    # 过滤ST股票
    initial_list = filter_st_stock(initial_list)
    # 初始化最终的股票列表
    final_list = []
    # 遍历因子列表和对应的系数列表
    for factor_list, coef_list in g.factor_list:
        # 获取每个因子对应的值
        factor_values = get_factor_values(initial_list, factor_list, end_date=yesterday, count=1)
        # 创建一个DataFrame来存储因子值
        df = pd.DataFrame(index=initial_list, columns=factor_values.keys())
        # 将因子值填充到DataFrame中
        for i in range(len(factor_list)):
            df[factor_list[i]] = list(factor_values[factor_list[i]].T.iloc[:,0])
        # 删除DataFrame中的空值
        df = df.dropna()
        # 计算每只股票的总评分
        df['total_score'] = 0
        for i in range(len(factor_list)):
            df['total_score'] += coef_list[i]*df[factor_list[i]]
        # 根据总评分对股票进行降序排序
        df = df.sort_values(by=['total_score'], ascending=False)  # 分数越高即预测未来收益越高，排序默认降序
        df_pos=df[df['total_score']>0]          # 选取评分大于0的股票
        complex_factor_list = list(df_pos.index)[:int(0.1*len(list(df.index)))]      # 选取前10%的股票
        # 查询这些股票的市值和每股收益信息
        q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(complex_factor_list)).order_by(valuation.circulating_market_cap.asc())
        df = get_fundamentals(q)
        df = df[df['eps']>0]    # 过滤每股收益为负的股票
        # 获取过滤后的股票列表
        lst = list(df.code)
        # 过滤停牌的股票
        lst = filter_paused_stock(lst)
        # 过滤涨停的股票
        lst = filter_limitup_stock(context, lst)
        # 过滤跌停的股票
        lst = filter_limitdown_stock(context, lst)
        # 选取前g.stock_num个股票，或者列表本身的长度
        lst = lst[:min(g.stock_num, len(lst))]
        # 将股票添加到最终列表中，避免重复
        for stock in lst:
            if stock not in final_list:
                final_list.append(stock)
    # 返回最终的股票列表
    return final_list

#1-3 定义每周调仓函数 ；整体调整持仓 ；目的是进行量化交易策略中的周期性调仓操作。
def weekly_adjustment(context):
    if g.no_trading_today_signal == False:      # 检查今日是否为非交易日，根据 g.no_trading_today_signal 信号判断
        target_list = get_stock_list(context)   # 获取应买入的股票列表，使用自定义函数 get_stock_list
        # 调仓卖出逻辑
        for stock in g.hold_list:  # 遍历当前持有的股票列表
            if (stock not in target_list) and (stock not in g.yesterday_HL_list):  # 如果股票不在目标买入列表中，并且不在昨日涨停或跌停的股票列表中
                log.info("卖出[%s]" % (stock))                 # 记录卖出信息
                position = context.portfolio.positions[stock]  # 获取股票当前的持仓情况
                close_position(position)                       # 卖出股票持仓，调用close_position 函数，用于平仓
            else:          
                log.info("已持有[%s]" % (stock))               # 记录卖出信息
        #调仓买入逻辑
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
    current_data = get_current_data()      # 获取当前所有股票的数据
    return [stock for stock in stock_list if not current_data[stock].paused]    # 使用列表推导式，返回不在停牌状态的股票列表

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()            # 获取当前所有股票的数据
    return [stock for stock in stock_list       # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
            if not current_data[stock].is_st
            and 'ST' not in current_data[stock].name
            and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name]

#2-3 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

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

#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过375日的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date <    datetime.timedelta(days=375)]

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

#4-1 判断今天是否为账户资金再平衡的日期
def today_is_between(context, start_date, end_date):
    today = context.current_dt.strftime('%m-%d')       # 获取当前日期并格式化为月-日格式
    if (start_date <= today) and (today <= end_date):  # 判断当前日期是否在指定的起始和结束日期之间
        return True   # 今天是资金再平衡的日期
    else:
        return False  # 今天不是资金再平衡的日期

#4-2 清仓后次日资金可转
def close_account(context):
    if g.no_trading_today_signal == True:  # 检查今日是否为非交易日
        if len(g.hold_list) != 0:          # 如果持有股票列表不为空，则清仓
            for stock in g.hold_list:
                position = context.portfolio.positions[stock]  # 获取股票的持仓信息
                close_position(position)        # 执行平仓操作
                log.info("卖出[%s]" % (stock))  # 日志记录卖出信息

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