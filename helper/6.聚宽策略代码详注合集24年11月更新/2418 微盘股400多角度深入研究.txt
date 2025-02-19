# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/44563
# 标题：微盘股研究
# 作者：Gyro^.^
# 回测资金选择 1234567

import pandas as pd   # 导入pandas库并设置别名pd

def initialize(context):
    log.set_level('order', 'error')     # 设置日志级别，只记录order模块中error级别及以上的日志
    set_option('use_real_price', True)      # 使用真实价格进行交易，即考虑股票的复权信息
    set_option('avoid_future_data', True)       # 打开防未来函数
    g.days = 0                  # 初始化一个全局变量g.days，可能用于跟踪策略运行的天数

# 更换代码后运行的单元，每次策略代码更新并启动时都会执行这个函数
def after_code_changed(context):   
    unschedule_all()   # 取消所有已经设置的定时运行任务
    run_daily(iUpdate, time='before_open')  # 重新设置定时运行任务，每天开盘前运行iUpdate函数
    run_daily(iTrader, time='9:35')    # 每天9:35运行iTrader函数
    run_daily(iReport, time='after_close')   # 收盘后运行iReport函数，可能用于生成报告或进行日终处理

# iUpdate 函数，用于更新策略的股票选择和仓位大小
def iUpdate(context):
    nchoice = 120     # 定义选择股票的数量上限
    nposition = 100   # 定义持有股票的数量上限
    # 全部A股
    dt_last = context.previous_date      # 获取策略运行前一天的日期
    all_stock = get_all_securities('stock', dt_last)      # 获取全部A股的股票列表
    cdata = get_current_data()    # 获取当前所有股票的数据
    stocks = [s for s in all_stock.index if not cdata[s].is_st]      # 过滤掉ST股票，只保留非ST股票
    # 获取小盘股的股票列表，查询股票代码和市值
    df = get_fundamentals(query(
            valuation.code,
            valuation.market_cap,
        ).filter(
            valuation.code.in_(stocks),    # 只查询非ST股票的市值
            valuation.pb_ratio > 0,        # 过滤掉市净率小于等于0的股票
        ).order_by(valuation.market_cap.asc()    # 按市值升序排序
        ).limit(nchoice)                     # 限制查询结果数量为nchoice
        ).dropna().set_index('code')       # 去除缺失值，并将股票代码设置为索引
    # 结果
    g.choice = df.index.tolist()           # 将查询结果的股票列表保存到全局变量g.choice中
    # 计算每只股票的仓位大小，这里使用了倒数法来分配仓位，即总资金除以持有股票的数量
    g.position_size = 1.0/nposition * context.portfolio.total_value     

# iTrader 函数，用于执行策略的买卖逻辑
def iTrader(context):
    # 从全局变量中获取选择的股票列表和每只股票的仓位大小
    choice = g.choice 
    position_size = g.position_size 
    
    # 计算买入和卖出的阈值，用于调整仓位
    lm_value = 0.8 * position_size  # 较低市值阈值，用于增加仓位
    hm_value = 1.2 * position_size  # 较高市值阈值，用于减少仓位 
    
    cdata = get_current_data()   # 获取当前所有股票的数据
    # 卖出逻辑
    for s in context.portfolio.positions:   # 遍历当前持仓的股票
        # 检查股票是否停牌、是否触及涨跌停板，如果是则跳过
        if cdata[s].paused or \
            cdata[s].last_price >= cdata[s].high_limit or \
            cdata[s].last_price <= cdata[s].low_limit:
            continue # 过滤三停
        if s not in choice:  # 如果股票不在选择的股票列表中，卖出该股票
            log.info('sell', s, cdata[s].name)
            # 使用市价单并以最后一次成交价的99%下单卖出
            order_target(s, 0, MarketOrderStyle(0.99*cdata[s].last_price))
    # 买入逻辑
    for s in choice:        # 遍历选择的股票列表
        if context.portfolio.available_cash < position_size:    # 如果可用现金不足以买入一只股票，则跳出循环
            break
        # 检查股票是否停牌、是否触及涨跌停板，如果是则跳过
        if cdata[s].paused or \
            cdata[s].last_price >= cdata[s].high_limit or \
            cdata[s].last_price <= cdata[s].low_limit:
            continue # 过滤三停
        # 如果股票不在当前持仓中，买入该股票
        if s not in context.portfolio.positions:
            log.info('buy', s, cdata[s].name)
            # 使用市价单并以最后一次成交价的101%下单买入
            order_target_value(s, position_size, MarketOrderStyle(1.01*cdata[s].last_price))
       # 如果股票已在持仓中，根据其价值调整仓位
        elif context.portfolio.positions[s].value < lm_value:
            log.info('balance+', s, cdata[s].name)
            # 如果股票价值低于较低阈值，增加仓位
            order_target_value(s, position_size, MarketOrderStyle(1.01*cdata[s].last_price))
        elif context.portfolio.positions[s].value > hm_value:
            log.info('balance-', s, cdata[s].name)
            # 如果股票价值高于较高阈值，减少仓位
            order_target_value(s, position_size, MarketOrderStyle(0.99*cdata[s].last_price))

# iReport 函数，用于在策略运行结束后生成报告
def iReport(context):
    g.days = g.days + 1  # 增加全局变量 g.days 的值，用于跟踪策略运行的天数
    log.info('  positions', len(context.portfolio.positions))    # 记录当前持仓的股票数量
    log.info('  return %.2f', 100*context.portfolio.returns)     # 记录策略的累计收益率
    log.info('  cash %.2f',   context.portfolio.available_cash/10000)    # 记录当前可用的现金余额
    log.info('  value %.2f',  context.portfolio.total_value/10000)       # 记录当前投资组合的总价值
    log.info('running days', g.days)    # 记录策略运行的天数
# end