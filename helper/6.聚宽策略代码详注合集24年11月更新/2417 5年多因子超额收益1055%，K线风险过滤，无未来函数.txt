# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/41175
# 标题：5年超额收益1055%，K线处理大阴线，无未来
# 作者：hello friends

import statsmodels.api as sm  # 导入statsmodels库，用于统计分析
from jqdata import *  # 从jqdata模块导入所有内容，提供数据获取接口
from jqfactor import get_factor_values  # 导入获取因子值的函数
import datetime  # 导入datetime库，用于处理日期和时间

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
    # 设置交易成本，          卖出时印花税      买入时印花税               卖出时佣金          最低佣金,不含印花税   股票类型
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    # 初始化全局变量
    g.stock_num = 5      # 设置策略的目标持股数量为5
    g.limit_days = 20    # 设置检查涨停股票的天数为20天
    g.hold_list = []     # 初始化当前持仓列表
    g.history_hold_list = []    # 初始化历史持仓列表
    g.not_buy_again_list = []   # 初始化短期内不重复买入的股票列表
    g.switch=0                  # 初始化一个开关变量，用于控制某些逻辑
    # 设置交易时间，每天运行
    # 每天开盘前 9:05 运行prepare_stock_list函数，准备股票列表
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # 每周的第一个交易日 9:30 运行weekly_adjustment函数，进行周调整
    run_weekly(weekly_adjustment, weekday=1, time='9:30', reference_security='000300.XSHG')
    # 每天14:00运行check_limit_up函数，检查昨日涨停今天未涨停的股票并卖出
    run_daily(check_limit_up, time='14:00', reference_security='000300.XSHG')
    # 每天开盘时9:30运行check_csy函数，检查昨天长上影的股票并卖出
    run_daily(check_csy, time='09:30', reference_security='000300.XSHG')

# 1-1 选股模块
def get_single_factor_list(context, stock_list, jqfactor, sort, p1, p2):
    # 这个函数用于根据指定的因子（jqfactor）对股票列表（stock_list）进行排序，并返回排序后位于p1到p2百分比范围内的股票列表。
    yesterday = context.previous_date    # 获取策略运行前一天的日期，用于获取因子数据
    # 使用get_factor_values函数获取指定股票列表和因子的数据，end_date指定获取数据的日期，count=1表示获取最新的数据
    # 从获取的因子数据中提取特定因子的值，并去除NaN值，然后根据sort参数指定的顺序进行排序
    s_score = get_factor_values(stock_list, jqfactor, end_date=yesterday, count=1
                                )[jqfactor].iloc[0].dropna().sort_values(ascending=sort)
    # 计算开始和结束的索引位置，然后根据这些索引从排序后的股票列表中选择股票，列表切片操作用于选择特定范围内的股票
    return s_score.index[int(p1 * len(stock_list)):int(p2 * len(stock_list))].tolist()

# 定义一个函数，用于根据流通市值对股票列表进行排序，并返回市值最大的前5只股票
def sorted_by_circulating_market_cap(stock_list, n_limit_top=5):
    # 创建一个查询对象，用于查询股票的代码和流通市值
    q = query(
        valuation.code,  # 指定查询字段为股票代码
    ).filter(
        valuation.code.in_(stock_list),      # 过滤条件，只选择列表中的股票
        indicator.eps > 0                    # 过滤条件，只选择每股收益大于0的股票
    ).order_by(
        valuation.circulating_market_cap.asc()  # 排序条件，按流通市值升序排序
    ).limit(
        n_limit_top    # 限制条件，限制返回的股票数量为前N名
    )
    # 获取流通市值最大的前N只股票，get_fundamentals(q) 执行查询，返回查询结果，.tolist() 将股票代码转换为列表形式
    return get_fundamentals(q)['code'].tolist() 

# 1-2 选股模块：根据营业收入增长率、盈利增长率、PEG等因子找出并返回10个股票
def get_stock_list(context):
    by_date = context.previous_date - datetime.timedelta(days=375)      # 计算距离当前日期大约一年的日期，用于筛选非次新股
    initial_list = get_all_securities(date=by_date).index.tolist()      # 获取大约一年前所有上市的股票列表
    # 过滤掉科创板股票和ST股
    initial_list = filter_kcb_stock(initial_list)
    initial_list = filter_st_stock(initial_list)
    # 1. SG 过去5年营业收入增长率, 从大到小的前10%；再按流通市值升序，取前5名
    sg_list = get_single_factor_list(context, initial_list, 'sales_growth', False, 0, 0.1)
    sg_list = sorted_by_circulating_market_cap(sg_list)
    # 2. MS 复合增长率, 从大到小的前10%；
    factor_list = [
        'operating_revenue_growth_rate',  # 营业收入TTM增长率
        'total_profit_growth_rate',  # 利润总额TTM增长率
        'net_profit_growth_rate',  # 净利润TTM增长率
        'earnings_growth'       # 5年盈利增长率
    ]
    # 获取因子数据，使用get_factor_values函数获取股票列表（initial_list）中所有股票的特定因子（factor_list）的数据
    factor_values = get_factor_values(initial_list, factor_list, end_date=context.previous_date, count=1)
    df = pd.DataFrame(index=initial_list)      # 创建DataFrame存储因子数据，索引为股票代码（initial_list）
    # 遍历因子列表（factor_list），将获取到的因子数据填充到DataFrame（df）中，factor_values[factor].iloc[0]表示获取每个因子的最新值
    for factor in factor_list:
        df[factor] = factor_values[factor].iloc[0]
    # 计算总评分，这里根据不同因子的重要性给予不同的权重，营业收入增长率占10%，利润总额增长率占15%，净利润增长率占15%，5年盈利增长率占60%
    df['total_score'] = 0.1* df['operating_revenue_growth_rate'] + 0.15 * df['total_profit_growth_rate'] + 0.15 * df[
        'net_profit_growth_rate'] + 0.6 * df['earnings_growth']
    # 按总评分从高到低排序，取前10%，ascending=False表示降序排序，即评分最高的在前面
    ms_list = df.sort_values(by=['total_score'], ascending=False).index[:int(0.1 * len(df))].tolist()
    # 对选出的股票列表按流通市值进行升序排序，并取前5名
    ms_list = sorted_by_circulating_market_cap(ms_list)
    # 对股票列表按PEG因子进行升序排序，取前20%
    peg_list = get_single_factor_list(context, initial_list, 'PEG', True, 0, 0.2)
    # 对PEG排序后的股票列表再按TURNOVER_VOLATILITY因子进行升序排序，取前50%
    peg_list = get_single_factor_list(context, peg_list, 'turnover_volatility', True, 0, 0.5)
    # 对PEG和TURNOVER_VOLATILITY排序后的股票列表按流通市值进行升序排序
    peg_list = sorted_by_circulating_market_cap(peg_list)
    # 1、2、3的并集；取上述三个列表的并集，即所有选出的股票的集合，这可以确保我们从不同的因子中获取不同的股票，增加多样性
    union_list = list(set(sg_list).union(set(ms_list)).union(set(peg_list)))
    # 对并集后的股票列表按流通市值进行升序排序，并取前12名作为最终的选股结果
    union_list = sorted_by_circulating_market_cap(union_list, 12)
    print('选股结果：', union_list)     # 打印选股结果
    return union_list   # 返回最终的选股列表

# 1-3 准备股票池  
def prepare_stock_list(context):
    g.hold_list = list(context.portfolio.positions)  # 获取当前投资组合中已持有的股票列表，并更新g.hold_list全局变量
    g.history_hold_list.append(g.hold_list)    # 获取最近一段时间持有过的股票列表，将当前持有的股票列表添加到历史持有列表中
    if len(g.history_hold_list) >= g.limit_days:  # 如果历史持有列表的长度超过了限定的天数g.limit_days
        g.history_hold_list = g.history_hold_list[-g.limit_days:]   # 则保留最近g.limit_days天的数据
    # 使用集合来去重，存储过去g.limit_days天内持有过的所有股票
    temp_set = set()  # 建一个新集合
    for hold_list in g.history_hold_list:  
        temp_set = temp_set.union(set(hold_list))  # 通过set.union()去重
    g.not_buy_again_list = list(temp_set)    # 将集合转换回列表，这个列表用于记录接下来一段时间内不再次购买的股票
    # 获取持仓的昨日涨停列表
    g.high_limit_list = []
    if g.hold_list:    # 如果当前有持仓股票
        # 获取这些股票昨日的收盘价、涨停价和是否停牌的数据
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily',
                       fields=['close', 'high_limit', 'paused'],
                       count=1, panel=False)
        # 筛选出昨日收盘价等于涨停价且未停牌的股票，即昨日涨停的股票
        g.high_limit_list = df.query('close==high_limit and paused==0')['code'].tolist()#paused为0表示不停牌

# 1-4 整体调整持仓  
def weekly_adjustment(context):
    target_list = get_stock_list(context)   # 获取不多于12个股票列表
    target_list = filter_paused_stock(target_list)  # 过滤停牌股票
    target_list = filter_limit_stock(context, target_list)  # 过滤涨停股票
    # target_list中，去除最近20天内曾经涨停过的和曾经买过的股
    recent_limit_up_list = get_recent_limit_up_stock(context, target_list, g.limit_days)
    # 取两个列表的交集，即最近20天内曾经涨停过的股票
    black_list = list(set(g.not_buy_again_list).intersection(set(recent_limit_up_list)))
    # 从目标列表中移除这些股票
    target_list = [stock for stock in target_list if stock not in black_list]
    # 如果目标列表的股票数量超过10，只保留前10个
    if len(target_list) > 10:
        target_list = target_list[:10]
    # 获取最近40天的收盘价，计算MA20，并去掉下跌趋势明显的
    h_ma = history(20 + 20, '1d', 'close', target_list).rolling(window=20).mean().iloc[20:] # 获取最近40天的收盘价，计算MA20
    #上面取最后20行
    X = np.arange(len(h_ma))  # 生成0、1、...19的数组
    tmp_target_list = []    # 临时存储不处于下跌趋势的股票
    for stock in target_list:
        MA_N_Arr = h_ma[stock].values      # 得到每个股票最近20天的MA20数值
        MA_N_Arr = MA_N_Arr - MA_N_Arr[0]  # 截距归零，理解成标准化
        slope = round(sm.OLS(MA_N_Arr, X).fit().params[0] * 100, 1)  # Statsmodels 中 OLS 回归功能sm.OLS(因变量,自变量)，在 OLS之后调用拟合函数 fit()，
        #才进行回归运算，并且得到RegressionResultsWrapper结果，它包含了这组数据进行回归拟合的结果摘要。调用 params 可以查看计算出的回归系数 b0,b1,…,bn。
        #params[0]是为了去除列表，取具体值。sm.OLS(Y,X).fit().summary()可以看总体回归情况
        remove_it = False 
        # 如果斜率小于-2%，则认为处于下跌趋势
        if slope < -2:
            if stock not in g.hold_list:
                print('{}下降趋势明显，切勿开仓'.format(stock))
                remove_it = True
        if not remove_it:
            tmp_target_list.append(stock)
    target_list = tmp_target_list    # 更新目标列表
   #把股票列表转为简称
    gupiao=[]
    for s in target_list:   # 遍历目标股票列表（target_list），这个列表包含了策略决定买入的股票
        ss=get_security_info(s).display_name  # 获取每个股票的详细信息，包括股票名称
        gupiao.append(ss)   # 将股票名称添加到列表中
    print("提示买的股票列表%s"%gupiao)
    # 调仓：不在列表，昨日未涨停的持仓票卖出。
    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):      # 检查股票是否不在目标列表中，且昨日未涨停
            log.info("卖出[%s]" % stock)   # 记录卖出操作的日志
            position = context.portfolio.positions[stock]        # 获取股票的持仓信息
            close_position(position)        # 调用close_position函数卖出股票，平仓
        else:
            log.info("已持有[%s]" % stock)        # 如果股票在目标列表中或昨日涨停，记录已持有的日志

    position_count = len(context.portfolio.positions)   # 获取当前持仓的数量
    target_num = g.stock_num  # 获取目标持仓数量
    if target_num > position_count:   # 如果目标持仓数量大于当前持仓数量，进行买入操作
        value =  context.portfolio.available_cash / (target_num - position_count)    # 计算每个新股票的买入金额
        for stock in target_list:    # 遍历目标股票列表，买入不在当前持仓中的股票
            if stock not in context.portfolio.positions:     # 如果股票不在当前持仓中
                if open_position(stock, value):      # 调用open_position函数买入股票
                    if len(context.portfolio.positions) >= g.stock_num:       # 如果买入成功，且当前持仓数量达到目标持仓数量，停止买入
                        break

# 1-5 调整昨日涨停股票，如果今日不再涨停，则卖出
def check_limit_up(context):
    current_data = get_current_data()   # 获取当前所有股票的数据
    if g.high_limit_list:    # 检查昨日涨停股票列表是否为空
        for stock in g.high_limit_list:     # 遍历昨日涨停股票列表
            if current_data[stock].last_price < current_data[stock].high_limit:      # 判断当前价格是否低于涨停价格，即涨停打开没有
                log.info("[%s]涨停打开，卖出" % stock)       # 记录卖出操作的日志信息
                position = context.portfolio.positions[stock]       # 获取股票的持仓信息
                close_position(position)    # 调用close_position函数卖出股票，平仓
            else:
                log.info("[%s]涨停，继续持有" % stock)     # 如果股票今日仍然涨停，则记录继续持有的日志信息
                
#1-6 调整昨日大阴线的股票，把昨天长上影的股票卖出
def check_csy(context):
    if g.switch==0:   # 检查全局开关g.switch，如果为0，则设置为1，否则保持不变，用于控制函数的执行，避免重复执行相同的逻辑
        g.switch=g.switch+1
    else:
        yesterday = context.previous_date      # 获取昨日的日期
        # 获取昨日的最高价，开盘价和收盘价
        dict_high=history(1, unit='1d', field='high', security_list=g.hold_list, df=False, skip_paused=False, fq='pre')
        dict_open=history(1,unit='1d', field='open', security_list=g.hold_list, df=False, skip_paused=False, fq='pre')
        dict_close=history(2, unit='1d', field='close', security_list=g.hold_list, df=False, skip_paused=False, fq='pre')
        for stock in g.hold_list:       # 遍历当前持仓列表
               # 计算昨日开盘涨幅
               kpzf=(dict_open[stock][0]-dict_close[stock][0])/dict_close[stock][0]
               # 计算昨日收盘涨幅
               spzf=(dict_close[stock][1]-dict_close[stock][0])/dict_close[stock][0]
               print("%s股票昨日的收盘涨幅是%s"%(stock,spzf))    # 打印股票昨日的收盘涨幅
               # 如果昨日的开盘到收盘的跌幅超过7%，则认为出现了大阴线，开盘卖出
               if (kpzf-spzf)>0.068:
                  log.info("[%s]昨日大阴线，卖出" % stock)     # 记录卖出操作的日志信息
                  position = context.portfolio.positions[stock]    # 获取股票的持仓信息
                  close_position(position)         # 调用close_position函数卖出股票，平仓
               else:   # 如果没有出现大阴线，则不执行操作
                   pass

# 2-1 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

# 2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()       # 获取当前所有股票的数据
    return [stock for stock in stock_list if not (      # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
            current_data[stock].is_st or
            'ST' in current_data[stock].name or
            '*' in current_data[stock].name or
            '退' in current_data[stock].name)]

# 2-3 获取最近rencent_days个交易日内有涨停的股票列表
def get_recent_limit_up_stock(context, stock_list, recent_days):
    yesterday = context.previous_date    # 获取策略运行前一天的日期
    # 获取指定股票列表在最近recent_days天内的每日收盘价、涨停价和是否停牌的数据
    h = get_price(stock_list, end_date=yesterday, frequency='daily', fields=['close', 'high_limit', 'paused'],
                  count=recent_days, panel=False)
    # 查询涨停且未停牌的股票，然后按股票代码分组，计算每只股票涨停的次数
    s_limit = h.query('close==high_limit and paused==0').groupby('code')['high_limit'].count()
    return s_limit.index.tolist()        # 返回涨停次数大于0的股票列表

# 2-4 过滤涨停的股票
def filter_limit_stock(context, stock_list):
    current_data = get_current_data()        # 获取当前所有股票的数据，包括今日的开盘价、最高价、最低价和最新价
    holdings = list(context.portfolio.positions)      # 获取当前持仓的股票列表
    # 过滤掉不在持仓列表中，或者今日未涨停（即最新价不在涨停价和跌停价之间）的股票
    return [stock for stock in stock_list if (stock in holdings) or
            current_data[stock].low_limit < current_data[stock].last_price < current_data[stock].high_limit]

# 2-6 过滤科创板
def filter_kcb_stock(stock_list):
    return [stock for stock in stock_list if not stock.startswith('68')]   # 使用列表推导式，过滤68开头的科创版股票

# 3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:   # 检查目标价值是否为0，即是否需要清仓
        log.debug("Selling out %s" % security)   # 日志记录清仓信息
    else:
        log.debug("Order %s to value %f" % (security, value)) # 日志记录下单到特定价值的信息
    return order_target_value(security, value)   # 调用聚宽平台的order_target_value函数执行交易

# 3-2 交易模块-开仓
def open_position(security, value):
    _order = order_target_value_(security, value)  # 尝试对指定股票进行下单到特定价值
    if _order is not None and _order.filled > 0:        # 检查订单是否创建成功且有成交
        return True  # 开仓成功
    return False  # 开仓失败

# 3-3 交易模块-平仓
def close_position(position):
    security = position.security  # 获取持仓的股票代码
    _order = order_target_value_(security, 0)    # 下单清仓 ；可能会因停牌失败
    if _order is not None:  # 检查订单是否创建成功
        if _order.status == OrderStatus.held and _order.filled == _order.amount:  # 检查订单状态是否为全部成交且订单数量等于下单数量
            return True  # 平仓成功
    return False  # 平仓失败
