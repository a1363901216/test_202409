# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40992
# 标题：低价股优化，18年至今10625.40%，已加防未来函数
# 作者：南国草

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40980
# 标题：狂飙！低价股难道是圣杯？
# 作者：咔咔系

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/25650
# 标题：ST股日内做套每周调仓
# 作者：人生如梦

# 注意：回测时间选择 分钟

# 导入聚宽量化平台的函数库
from jqdata import *
# 导入numpy库，用于数据处理
import numpy as np 
# 导入pandas库，用于数据处理
import pandas as pd
# 从pandas库导入Series类
from pandas import Series
# 从datetime库导入datetime和time类
from datetime import datetime, time
# 初始化函数，在策略开始前运行一次
def initialize(context):
    # 设定沪深300指数为策略的基准
    set_benchmark('000300.XSHG')
    # 使用真实价格进行交易，即不使用前复权价格
    set_option('use_real_price', True)
    # 输出内容到日志，log.info()用于记录信息，便于调试和追踪
    log.info('初始函数开始运行且全局只运行一次')
    # 设置日志级别，过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    # 初始化全局变量，用于存储选定的股票
    g.sel_stock = None
    # 设置策略的开始买卖时间（上午10:20）
    g.strategy_starttime = time(10, 20)
    # 设置策略的尾盘撮合买入时间（下午14:55）
    g.strategy_endtime = time(14, 55)
    # 设置股票交易的手续费
    # 买入时佣金万分之二点五，卖出时佣金万分之二点五加千分之一印花税，每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.00025, close_commission=0.00025, min_commission=5), type='stock')
    # 每周一上午9:31运行weekly函数
    run_weekly(weekly, weekday=1, time='09:31', reference_security='000300.XSHG')
    # 开盘前运行before_market_open函数
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    # 每个交易日的开盘时运行market_open函数
    run_daily(market_open, time='every_bar', reference_security='000300.XSHG')
    # 每个交易日收盘后运行after_market_close函数
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

# 定义 weekly 函数，用于每周执行一次的策略调整
def weekly(context):
    # 重置全局变量 g.sel_stock，表示当前选择的股票
    g.sel_stock = None
    # 获取当前市场的所有股票数据
    current_data = get_current_data()
    # 获取所有股票的列表
    stock_list = get_all_securities().index.tolist()
    # 获取这些股票过去5天的收盘价
    last_5_prices = history(count=5, field='close', security_list=stock_list)
    # 计算这些股票过去5天收盘价的平均值
    last_5_prices = pd.DataFrame({'mean_val': last_5_prices.mean()})
    # 根据平均值进行升序排序
    last_5_prices_sort = last_5_prices.sort_values(axis=0, ascending=True, by=['mean_val'])
    # 筛选出平均值大于等于1.5的股票
    q = 'mean_val >= 1.5'
    last_5_prices_query = last_5_prices_sort.query(q)
    # 获取符合条件的股票列表
    last_5_prices_stock = list(last_5_prices_query.index)
    # 获取今天的日期字符串
    today_str = context.current_dt.strftime("%Y-%m-%d")
    #log.info(last_5_prices_stock)
    # 遍历筛选出的股票列表
    for security in last_5_prices_stock:
        # 检查股票是否非停牌、非ST股、名称中不包含'*'和'退'
        if (current_data[security].paused == False) \
            and not current_data[security].is_st \
            and '*' not in current_data[security].name \
            and '退' not in current_data[security].name:
            # 选择第一个符合条件的股票
            g.sel_stock = security
            break
    # 如果找到了符合条件的股票
    if g.sel_stock is not None:
        # 记录选中的股票信息
        log.info(g.sel_stock)
        log.info(get_security_info(g.sel_stock).display_name)
        # 准备卖出不在选中股票列表中的所有股票
        sell_list = set(context.portfolio.positions.keys()) - set([g.sel_stock])
        print('sell:', sell_list)
        # 执行卖出操作
        for stock in sell_list:
            order_target_value(stock, 0)
    else:
        # 如果没有找到符合条件的股票，卖出所有持仓
        for stock in context.portfolio.positions.keys():
            order_target_value(stock, 0)

# 定义 before_market_open 函数，该函数在每个交易日开盘前运行
def before_market_open(context):
    # 记录函数的运行时间
    log.info('函数运行时间(before_market_open)：' + context.current_dt.strftime("%Y-%m-%d %H:%M:%S"))
    # 初始化全局变量，用于控制买卖行为
    g.not_buy_flg = 1  # 用于标记是否允许买入，1 表示不允许
    g.not_sell_flg = 1  # 用于标记是否允许卖出，1 表示不允许
    # 初始化订单ID和价格变量
    g.last_buy_orderid = None  # 上一次买入操作的订单ID
    g.last_sell_orderid = None  # 上一次卖出操作的订单ID
    g.last_buy_price = 0  # 上一次买入的价格
    g.last_sell_price = 0  # 上一次卖出的价格
    # 初始化50分钟周期的价格信息
    g.price_50m = {'open': 0.000, 'close': 0.000, 'high': 0.000, 'low': 0.000}
    # 初始化收盘前的价格信息
    g.price_before_close = 0.000

# 定义 get_buy_price 函数，用于根据当前时间获取买入价格
def get_buy_price(context):
    # 获取选定股票在过去50分钟内的历史价格信息
    sel_stock_price = attribute_history(g.sel_stock, 1, unit='50m', fields=('open', 'close', 'high', 'low'), skip_paused=True, df=True, fq='pre')
    # 判断当前时间是否在策略的买入时间范围内
    if context.current_dt.time() >= g.strategy_starttime and context.current_dt.time() < g.strategy_endtime:
        # 如果在买入时间范围内，返回该股票的最低价
        return sel_stock_price['low'][0]
    else:
        # 如果不在买入时间范围内，返回收盘价
        return sel_stock_price['close'][0]
        
# 定义 get_sell_price 函数，用于根据当前时间获取卖出价格
def get_sell_price(context):
    # 获取选定股票在过去50分钟内的历史价格信息
    sel_stock_price = attribute_history(g.sel_stock, 1, unit='50m', fields=('open', 'close', 'high', 'low'), skip_paused=True, df=True, fq='pre')
    # 判断当前时间是否在策略的卖出时间范围内
    if context.current_dt.time() >= g.strategy_starttime and context.current_dt.time() < g.strategy_endtime:
        # 如果在卖出时间范围内，返回该股票的最高价
        return sel_stock_price['high'][0]
    else:
        # 如果不在卖出时间范围内，返回收盘价
        return sel_stock_price['close'][0]
    
# 定义 market_open 函数，该函数在每个交易日开盘时调用
def market_open(context):
    # 获取当前市场数据
    current_data = get_current_data()
    # 如果没有选定股票，则直接返回
    if g.sel_stock is None:
        return
    
    # 测试限价单bug ,时间 2022-05-05 10:38可以成交 ，10：37，10：36等都不行 
    # if context.current_dt.time() ==time(10, 37):
    #     _order = order('600568.XSHG',60900,LimitOrderStyle(1.63))
    #     print(_order)
    # return
    
    # 判断是否到达策略设定的交易开始时间
    if context.current_dt.time() >= g.strategy_starttime:
        # 如果标志位表示当前不允许买入
        if g.not_buy_flg == 1:
            # 调用 get_buy_price 函数获取当前买入价格
            now_buy_price = get_buy_price(context)
            # 获取所有未完成的订单
            orders = get_open_orders()
            # 遍历所有订单
            for _order in orders.values():
                # 如果存在之前下的买入订单，则取消该订单
                if _order.order_id == g.last_buy_orderid:
                    cancel_order(_order)
            # 价格和之前的下单价格不一致 且 之前下过单
            # if now_buy_price!=g.last_buy_price and g.last_buy_orderid is not None:
            #     orders=get_open_orders()
            #     g.not_buy_flg=0
            #     for _order in orders.values():
            #         if _order.order_id==g.last_buy_orderid:
            #             cancel_order(_order)
            #             g.not_buy_flg=1
            
            # 更新上次买入价格
            g.last_buy_price = now_buy_price
            # 计算可以买入的最大股数，确保买入数量为100的整数倍
            buy_count = int(context.portfolio.available_cash / now_buy_price / 100) * 100
            # 如果计算出的买入数量不少于100股，则下单买入
            if buy_count >= 100:
                # 下限价单买入
                new_order = order(g.sel_stock, buy_count, LimitOrderStyle(now_buy_price))
                # 如果订单创建成功
                if new_order is not None:
                    # 如果订单状态为 'held'（挂起），则表示订单已提交但尚未成交
                    if str(new_order.status) == 'held':
                        g.not_sell_flg = 0  # 允许卖出
                    else:
                        # 记录最新的买入订单ID
                        g.last_buy_orderid = new_order.order_id
        # 判断是否到达策略设定的交易结束时间
        if context.current_dt.time() < g.strategy_endtime and g.not_sell_flg == 1:
            # 调用 get_sell_price 函数获取当前卖出价格
            now_sell_price = get_sell_price(context)
            # 获取所有未完成的订单
            orders = get_open_orders()
            # 遍历所有订单
            for _order in orders.values():
                # 如果存在之前下的卖出订单，则取消该订单
                if _order.order_id == g.last_sell_orderid:
                    cancel_order(_order)        
            # if now_sell_price!=g.last_sell_price and g.last_sell_orderid is not None:
            #     orders=get_open_orders()
            #     g.not_sell_flg=0
            #     for _order in orders.values():
            #         if _order.order_id==g.last_sell_orderid:
            #             cancel_order(_order)
            #             g.not_sell_flg=1

            # 检查是否持有选定股票且持有数量大于0
            if g.sel_stock in context.portfolio.positions \
            and context.portfolio.positions[g.sel_stock].closeable_amount > 0:
                # 更新上次卖出价格
                g.last_sell_price = now_sell_price                
                # 下限价单卖出所有持有的选定股票
                new_order = order(g.sel_stock, -context.portfolio.positions[g.sel_stock].closeable_amount, LimitOrderStyle(now_sell_price))
                # 如果订单创建成功
                if new_order is not None:
                    # 如果订单状态为 'held'（挂起），则表示订单已提交但尚未成交
                    if str(new_order.status) == 'held':
                        g.not_sell_flg = 0  # 允许买入
                    else:                
                        # 记录最新的卖出订单ID
                        g.last_sell_orderid = new_order.order_id
            else:
                # 如果没有持有选定股票，则不允许卖出
                g.not_sell_flg = 0

# 定义 after_market_close 函数，该函数在每个交易日收盘后运行
def after_market_close(context):
    # 记录函数的运行时间，输出到日志中
    # context.current_dt.time() 获取当前时间，格式为小时:分钟:秒
    log.info(str('函数运行时间(after_market_close):' + str(context.current_dt.time())))
    # 获取当天所有成交记录
    # get_trades() 函数返回一个字典，包含当天所有交易的 Trade 对象
    trades = get_trades()
    # 遍历所有成交记录
    # trades.values() 获取字典中的所有值，即所有 Trade 对象
    for _trade in trades.values():
        # 记录每一条成交记录的详细信息
        # _trade 是单个 Trade 对象，包含成交的具体信息
        log.info('成交记录：' + str(_trade))
    log.info('一天结束')
    log.info('##############################################################')
