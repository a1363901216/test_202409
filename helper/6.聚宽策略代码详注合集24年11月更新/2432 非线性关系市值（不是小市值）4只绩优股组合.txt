# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42211
# 标题：非线性市值（非小市值）组合4只
# 作者：璐璐202006

import math
from jqdata import *
from pandas.core.frame import DataFrame
def initialize(context):
    # 设置系统参数
    set_option('use_real_price', True)    # 用真实价格交易
    set_slippage(PriceRelatedSlippage(0.00))  # 设定滑点为百分比滑点
    # 设置交易成本
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5),type='stock')
    # 初始化全局变量
    g.chosen_stock_list = []  # 选出的股票列表
    g.sold_stock = {}         # 已卖出股票列表
    g.buy_stock_count = 4    # 购买股票数量
#    g.increase1d = 0.06       # 1日涨幅限制
#    g.tradeday = 12          # 上市天数限制
#    g.buyagain = 31            # 再次购买间隔天数
    g.score = 7               # 股票评分
    g.buyrank = g.buy_stock_count * 2   # 输出可买入列表的个数
    g.sellrank = g.buy_stock_count * 2  # 筛选时保留的股票个数
    g.stock_selection_percent = 0.7     # 设置选取市值最大股票的百分比，1为全部。
    g.volume_days = 5                   # 成交量天数
    g.increase_days = 60                # 涨幅天数
#    g.score_weights = [10,9,1,10,10]# [当前价格，成交量，涨幅天数，流通市值，总市值]
    g.score_weights = [2,1,1,4,4]# [当前价格，成交量，涨幅天数，流通市值，总市值]
    # 设置定时任务
    run_monthly(before_trading,1,'09:29')  # 每月第一个交易日运行
    run_daily(mysell,'09:30')
    run_daily(mybuy,'09:31')
    # 设置日志级别
    log.set_level('order', 'error')   # 过滤order中低于error级别的日志
    log.set_level('system', 'error')
    log.set_level('history', 'error')

def select_top_percent_stocks(df, percent):
    """
    选择前N%的股票
    :param df: DataFrame，包含股票数据的数据帧
    :param percent: float，要选择的股票百分比，范围为0到1
    :return: DataFrame，包含所选股票的数据帧
    """
    top_percent = int(len(df) * percent)  # 计算前N%的股票数量
    return df.head(top_percent)  # 返回前N%的股票

# 每月运行，筛选股票
def before_trading(context):
#    temp = g.sold_stock  # 临时存储已卖出股票
#    g.sold_stock = {}  # 清空已卖出股票列表
#    for stock in temp.keys():# 判断已经卖出的股票是否达到了再次购买的条件
#        if temp[stock] >= g.buyagain - 1:
#            pass
#        else:
#            g.sold_stock[stock] = temp[stock] + 1
    g.chosen_stock_list = get_stock_list(context)  # 获取筛选后的股票列表

# 获取和筛选股票列表
def get_stock_list(context):
    # 使用 query 函数从 valuation 表中获取股票代码，并添加过滤条件
    q = query(
        valuation.code,  # 股票代码
        valuation.pe_ratio,  # 市盈率
        valuation.pb_ratio,  # 市净率
        indicator.inc_return,  # 股息率
        indicator.inc_total_revenue_year_on_year,  # 营业收入同比增长率
        indicator.inc_net_profit_year_on_year,  # 净利润同比增长率
        valuation.market_cap   # 市值
    ).filter(
        valuation.pe_ratio > 0,  # 过滤市盈率大于0的股票
        valuation.pb_ratio > 0,  # 过滤市净率大于0的股票
        indicator.inc_return > 0,  # 过滤股息率大于0的股票
        indicator.inc_total_revenue_year_on_year > 0,  # 过滤营业收入同比增长率大于0的股票
        indicator.inc_net_profit_year_on_year > 0  # 过滤净利润同比增长率大于0的股票
    )
    # 执行查询并获取数据
    df = get_fundamentals(q)
    # 将查询结果转换为 DataFrame
    df = pd.DataFrame(df)
    # 删除包含 NaN 值的行
    df = df.dropna()
    # 按市值从大到小排序
    df = df.sort_values(by='market_cap', ascending=False)  
    print('本月股票总数: %s' % len(df))  # 输出符合条件的股票总数
    df = select_top_percent_stocks(df, g.stock_selection_percent)  # 调用全局函数，选取前N%的股票
    print('本月选中股票总数: {}% ({})'.format(g.stock_selection_percent * 100, len(df)))  # 输出选取的股票总数
    stock_list = list(df['code'])      # 将股票代码转换为列表
    # 过滤股票
    stock_list = filter_st_stock(stock_list)  # 过滤ST股票
    stock_list = filter_paused_stock(stock_list)  # 过滤停牌股票
    # stock_list = filter_new_stock(context, stock_list)  # 过滤新股（注释掉）
    stock_list = filter_limitup_stock(context, stock_list)  # 过滤涨停股票
    stock_list = filter_limitdown_stock(context, stock_list)  # 过滤跌停股票
    # stock_list = filter_increase1d(stock_list)  # 过滤连续涨停股票（注释掉）
    # stock_list = filter_buyagain(stock_list)  # 过滤近期可买入股票（注释掉）
    stock_list = filter_kcbj_stock(stock_list)  # 过滤科创板股票
    stock_list = ffscore_stock(context, g.score, stock_list, context.current_dt.date())  # 根据财务评分过滤股票
    print('本月股票池 %s 个' % len(stock_list))
#    log.info("——————————————————————————————————")
#    for i, stock in enumerate(stock_list):
#        rank=i+1
#        name = get_security_info(stock).display_name
#        print("本月股票池第 {}：{} {}".format(rank, stock, name))
#    log.info("——————————————————————————————————")
    return stock_list

#   定义调仓策略：调整持仓至设定的仓位比例
def my_adjust_position(context, hold_stocks):
    # 获取账户总资产价值
    free_value = context.portfolio.total_value
    # 计算每只股票的最大持仓比例
    maxpercent = 1.3 / g.buy_stock_count
    # 计算每只股票的买入金额
    buycash = free_value / g.buy_stock_count
    # 遍历当前持仓的每只股票
    for stock in context.portfolio.positions.keys():
        # 获取当前的股票市场数据
        current_data = get_current_data()
        # 获取该股票最近一日的收盘价
        price1d = get_close_price(stock, 1)
        # 判断股票是否处于不可卖出状态（例如涨停）
        nosell_1 = context.portfolio.positions[stock].price >= current_data[stock].high_limit
        # 判断股票是否不在持有列表中
        sell_2 = stock not in hold_stocks
        # 如果股票不在持有列表中且不是处于不可卖出状态，则清仓该股票
        if sell_2 and not nosell_1:
            close_position(stock)
        # 否则，计算当前股票持仓占总资产的比例
        else:
            current_percent = context.portfolio.positions[stock].value / context.portfolio.total_value
            # 如果当前持仓比例超过最大持仓比例，则调整至目标买入金额
            if current_percent > maxpercent:
                order_target_value(stock, buycash)

#   卖出函数
def mysell(context):
    # 调用 get_stock_rank_m_m 函数对全局变量 g.chosen_stock_list 中的股票进行排名
    # 这个函数可能基于某种策略（例如动量、价值或其他财务指标）对股票进行排序
    # g.chosen_stock_list 存储了当前选择持有的股票列表
    g.chosen_stock_list = get_stock_rank_m_m(g.chosen_stock_list)
    
    # 调用 my_adjust_position 函数调整持仓
    # 这个函数可能会根据预设的仓位比例和当前的持仓情况进行调整
    # 例如，如果某只股票的持仓比例超出了设定的范围，该函数可能会卖出部分股票
    # 如果某只股票不在 g.chosen_stock_list 中，也可能会被卖出
    # 该函数确保最终的持仓符合策略的要求
    my_adjust_position(context, g.chosen_stock_list)

#   买入函数
def mybuy(context):
     # 获取已筛选股票列表
    hold_stocks = (g.chosen_stock_list)    
    # 检查持有股票的数量是否小于预期购买数量
    if len(hold_stocks) < g.buy_stock_count:
        g.buy_stock_count = len(hold_stocks)
        log.info("Adjusted buy_stock_count to {} as there are fewer stocks in hold_stocks.".format(g.buy_stock_count))
    # 获取可用资金和最小持仓比例
    free_value, minpercent = context.portfolio.total_value, 0.7 / g.buy_stock_count
    # 计算每只股票应购买金额
    buycash = free_value / g.buy_stock_count
    # 计算当前可用资金
    free_cash = free_value - context.portfolio.positions_value
    # 计算最小购买金额
    min_buy = context.portfolio.total_value / (g.buy_stock_count * 10)
    # 遍历持仓股票，尝试调整其持仓比例
    for i in range(g.buy_stock_count):
        # 如果已经持有了目标数量的股票，则退出循环
        if len(context.portfolio.positions) >= g.buy_stock_count:
            break
        # 获取当前循环股票
        stock = hold_stocks[i]
        # 如果当前可用资金小于最小购买金额，则退出循环
        if free_cash <= min_buy:
            break
        # 获取当前股票的持仓信息
        position = context.portfolio.positions.get(stock)
        # 如果已经持有该股票且其持仓比例已达到最小持仓比例，则继续循环
        current_percent = position.value / context.portfolio.total_value if position else 0
        if current_percent >= minpercent:
            continue
        # 计算应购买该股票的金额
        tobuy = min(free_cash, buycash - position.value) if position else min(buycash, free_cash)
        # 下单购买该股票
        order_value(stock, tobuy)
        # 更新可用资金
        free_cash -= tobuy

# 根据自定义评分排名筛选股票
def get_stock_rank_m_m(stock_list):
    # 将股票列表转换为DataFrame格式
    rank_stock_list = DataFrame(stock_list)  
    # 重命名列名，假设原始数据中第一列是股票代码
    rank_stock_list.rename(columns={0: 'code'}, inplace=True) 
    # 获取流通市值和总市值,使用列表推导式，对每只股票获取其流通市值和总市值
    rank_stock_list['circulating_market_cap'] = [get_fundamentals(query(valuation).filter(valuation.code == stock)).iloc[0]['circulating_market_cap'] for stock in rank_stock_list['code']]
    rank_stock_list['market_cap'] = [get_fundamentals(query(valuation).filter(valuation.code == stock)).iloc[0]['market_cap'] for stock in rank_stock_list['code']]
    # 获取成交量数据
    volume_days_sum = [attribute_history(stock, g.volume_days, '1d', 'volume', df=False)['volume'].sum() for stock in rank_stock_list['code']]
    # 获取增长率数据
    increase_period = [get_growth_rate(g.increase_days, stock) for stock in rank_stock_list['code']]
    # 获取当前价格数据
    current_price = [get_close_price(stock, 1, '1m') for stock in rank_stock_list['code']]
    # 计算各项指标的最小值
    min_price = min(current_price)
    min_increase_period = min(increase_period)
    min_volume = min(volume_days_sum)
    min_circulating_market_cap = min(rank_stock_list['circulating_market_cap'])
    min_market_cap = min(rank_stock_list['market_cap'])
    # 计算每只股票的评分
    totalcount = [[i,
                   math.log(min_price / current_price[i]) * g.score_weights[0] +
                   math.log(min_volume / volume_days_sum[i]) * g.score_weights[1] +
                   math.log(min_increase_period / increase_period[i]) * g.score_weights[2] +
                   math.log(min_circulating_market_cap / rank_stock_list['circulating_market_cap'][i]) * g.score_weights[3] +
                   math.log(min_market_cap / rank_stock_list['market_cap'][i]) * g.score_weights[4]
                   ] for i in rank_stock_list.index]
    # 根据评分对股票进行排序
    totalcount.sort(key=lambda x: x[1])
    # 选取排名靠前的股票
    # 保留最多g.sellrank设置的个数股票代码返回
    final_list = [rank_stock_list['code'][totalcount[-1 - i][0]] for i in range(min(g.sellrank, len(rank_stock_list)))]
    # 返回最终筛选和排序后的股票列表
    stock_list = final_list
#    log.info("——————————————————————————————————")
#    for i, stock in enumerate(stock_list[:g.buyrank]):
#        rank=i+1
#        name = get_security_info(stock).display_name
#        print("今日股票池第 {}：{} {}".format(rank, stock, name))
#    log.info("——————————————————————————————————")
    return stock_list

# 获取收盘价
def get_close_price(code, n, unit='1d'):
    return attribute_history(code, n, unit, 'close')['close'][0]

# 计算给定股票在特定天数内的增长率
def get_growth_rate(days, code):
    try:
        # 使用 attribute_history 函数获取股票在过去 'days' 天的收盘价
        # '1d' 表示按日获取数据，'close' 表示获取收盘价，False 表示不填充停牌数据
        price_period = attribute_history(code, days, '1d', 'close', False)['close'][0]
        # 使用 get_close_price 函数获取股票最近一个交易日的收盘价
        # '1m' 表示最近一个交易日的数据
        pricenow = get_close_price(code, 1, '1m')
        # 检查获取到的价格是否为非NaN值，且 'price_period' 不为0,如果都满足，则计算增长率
        if not math.isnan(pricenow) and not math.isnan(price_period) and price_period != 0:
            return pricenow / price_period
        else:
            return 100  # 如果任一价格为NaN或 'price_period' 为0，则返回100作为默认增长率
    except Exception as e:
        # 如果在尝试计算增长率的过程中发生异常，则打印错误信息
        print(f"Error calculating growth rate for stock {code}: {e}")
        # 发生异常时也返回100作为默认增长率
        return 100

# 定义平仓，卖出指定持仓
def close_position(code):
    order = order_target_value(code, 0)    # 下单清仓 ；可能会因停牌失败
    if order != None and order.status == OrderStatus.held:
        g.sold_stock[code] = 0

# 定义过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]   # 使用列表推导式，返回不在停牌状态的股票列表

# 定义过滤ST及其他具有退市标签的股票        
def filter_st_stock(stock_list):
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if not current_data[stock].is_st and 'ST' not in current_data[stock].name and '*' not in current_data[stock].name and '退' not in current_data[stock].name]

# 定义过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() 
        or last_prices[stock][-1] < current_data[stock].high_limit]

# 定义过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)  # 获取最近一分钟的收盘价
    current_data = get_current_data()   # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] > current_data[stock].low_limit]

# 定义过滤次新股
def filter_new_stock(context, stock_list):
    return [stock for stock in stock_list if (context.previous_date - datetime.timedelta(days=g.tradeday)) > get_security_info(stock).start_date]

# 定义过滤昨日涨幅过高的股票    
def filter_increase1d(stock_list):
    return [stock for stock in stock_list if get_close_price(stock, 1) / get_close_price(stock, 2) < (1 + g.increase1d)]

# 定义过滤买过的股票
def filter_buyagain(stock_list):
    return [stock for stock in stock_list if stock not in g.sold_stock.keys()]

# 定义过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)
    return stock_list

# 定义过滤基本面股票    
def ffscore_stock(context,score,security_list,date):
    # 定义观察日期，即用于获取历史财务数据的日期
    my_watch_date = date
    # 计算一年前的日期
    one_year_ago = my_watch_date - datetime.timedelta(days=365)
    # 获取过去五个季度的历史财务数据,security_list 是股票列表，用于过滤股票,指定要获取的财务指标列表
    h = get_history_fundamentals(security_list,
                             [indicator.adjusted_profit,  # 调整后的净利润
                              balance.total_current_assets,  # 流动资产总额
                              balance.total_assets,  # 资产总额
                              balance.total_current_liability,  # 流动负债总额
                              balance.total_non_current_liability,  # 非流动负债总额
                              cash_flow.net_operate_cash_flow,  # 经营活动产生的现金流量净额
                              income.operating_revenue,  # 营业收入
                              income.operating_cost,  # 营业成本
                              ],
                             watch_date=my_watch_date, count=5).dropna()  # 连续的5个季度
    # 定义计算最近四个季度总和的函数
    def ttm_sum(x):
        return x.iloc[1:].sum()
    # 定义计算最近四个季度平均值的函数
    def ttm_avg(x):
        return x.iloc[1:].mean()
    # 定义计算前四个季度总和的函数
    def pre_ttm_sum(x):
        return x.iloc[:-1].sum()
    # 定义计算前四个季度平均值的函数
    def pre_ttm_avg(x):
        return x.iloc[:-1].mean()
    # 定义获取最新季度值的函数
    def val_1(x):
        return x.iloc[-1]
    # 定义获取前一个季度值的函数
    def val_2(x):
        if len(x.index) > 1:
            return x.iloc[-2]
        else:
            return nan
    # 扣非利润
    adjusted_profit_ttm = h.groupby('code')['adjusted_profit'].apply(ttm_sum)
    adjusted_profit_ttm_pre = h.groupby('code')['adjusted_profit'].apply(pre_ttm_sum)
    # 总资产平均
    total_assets_avg = h.groupby('code')['total_assets'].apply(ttm_avg)
    total_assets_avg_pre = h.groupby('code')['total_assets'].apply(pre_ttm_avg)
    # 经营活动产生的现金流量净额
    net_operate_cash_flow_ttm = h.groupby('code')['net_operate_cash_flow'].apply(ttm_sum)
    # 长期负债率: 长期负债/总资产
    long_term_debt_ratio = h.groupby('code')['total_non_current_liability'].apply(val_1) / h.groupby('code')['total_assets'].apply(val_1)
    long_term_debt_ratio_pre = h.groupby('code')['total_non_current_liability'].apply(val_2) / h.groupby('code')['total_assets'].apply(val_2)
    # 流动比率：流动资产/流动负债
    current_ratio = h.groupby('code')['total_current_assets'].apply(val_1) / h.groupby('code')['total_current_liability'].apply(val_1)
    current_ratio_pre = h.groupby('code')['total_current_assets'].apply(val_2) / h.groupby('code')['total_current_liability'].apply(val_2)
    # 营业收入
    operating_revenue_ttm = h.groupby('code')['operating_revenue'].apply(ttm_sum)
    operating_revenue_ttm_pre = h.groupby('code')['operating_revenue'].apply(pre_ttm_sum)
    # 营业成本
    operating_cost_ttm = h.groupby('code')['operating_cost'].apply(ttm_sum)
    operating_cost_ttm_pre = h.groupby('code')['operating_cost'].apply(pre_ttm_sum)
    # 1. ROA 资产收益率
    roa = adjusted_profit_ttm / total_assets_avg
    roa_pre = adjusted_profit_ttm_pre / total_assets_avg_pre
    # 2. OCFOA 经营活动产生的现金流量净额/总资产
    ocfoa = net_operate_cash_flow_ttm / total_assets_avg
    # 3. ROA_CHG 资产收益率变化
    roa_chg = roa - roa_pre
    # 4. OCFOA_ROA 应计收益率: 经营活动产生的现金流量净额/总资产 -资产收益率
    ocfoa_roa = ocfoa - roa
    # 5. LTDR_CHG 长期负债率变化 (长期负债率=长期负债/总资产)
    ltdr_chg = long_term_debt_ratio - long_term_debt_ratio_pre
    # 6. CR_CHG 流动比率变化 (流动比率=流动资产/流动负债)
    cr_chg = current_ratio - current_ratio_pre
    # 8. GPM_CHG 毛利率变化 (毛利率=1-营业成本/营业收入)
    gpm_chg = operating_cost_ttm_pre/operating_revenue_ttm_pre - operating_cost_ttm/operating_revenue_ttm
    # 9. TAT_CHG 资产周转率变化(资产周转率=营业收入/总资产)
    tat_chg = operating_revenue_ttm/total_assets_avg - operating_revenue_ttm_pre/total_assets_avg_pre
    # 查询特定股票的股本变动信息
    spo_list = list(set(finance.run_query(
        query(
            finance.STK_CAPITAL_CHANGE.code       # 查询股票代码
        ).filter(
            finance.STK_CAPITAL_CHANGE.code.in_(security_list),  # 筛选特定股票列表
            finance.STK_CAPITAL_CHANGE.pub_date.between(one_year_ago, my_watch_date),  # 筛选特定日期范围内的变动
            finance.STK_CAPITAL_CHANGE.change_reason_id == 306004)  # 筛选特定变动原因
    )['code']))
    spo_score = pd.Series(True, index = security_list)  # 初始化一个 True 值的 Series，用于标记股票是否通过初步筛选
    # 如果存在股本变动的股票，将它们标记为 False（不通过初步筛选）
    if spo_list:
        spo_score[spo_list] = False
    # 创建一个空的 DataFrame 用于存储股票评分
    df_scores = pd.DataFrame(index=security_list)# 1
    # 根据一系列财务指标和市场行为为股票打分
    df_scores['roa'] = roa>0.0   # 1. 净资产收益率大于0
    df_scores['ocfoa'] = ocfoa>0  # 2. 经营活动产生的现金流量净额大于0
    df_scores['roa_chg'] = roa_chg>0   # 3. 净资产收益率同比增长率大于0
    df_scores['ocfoa_roa'] = ocfoa_roa>0   # 4. 经营活动产生的现金流量净额与净资产收益率的比值大于0
    df_scores['ltdr_chg'] = ltdr_chg<=0   # 5. 长期负债同比增长率小于等于0
    df_scores['cr_chg'] = cr_chg>0   # 6. 流动负债同比增长率大于0
    df_scores['spo'] = spo_score  > 0   # 7. 股本变动筛选结果
    df_scores['gpm_chg'] = gpm_chg>0  # 8. 毛利率同比增长率大于0
    df_scores['tat_chg'] = tat_chg>0    # 9. 合计增长率大于0
    df_scores = df_scores.dropna() # 删除包含 NaN 值的行
    # 计算每只股票的总评分
    df_scores['total'] = df_scores['roa'] + df_scores['ocfoa'] + df_scores['roa_chg'] + \
        df_scores['ocfoa_roa'] + df_scores['ltdr_chg'] + df_scores['cr_chg'] + \
        df_scores['spo'] + df_scores['gpm_chg'] + df_scores['tat_chg']
    # 根据总评分筛选股票，并按评分降序排序
    res  = df_scores.loc[lambda df_scores: df_scores['total'] > score].sort_values(by = 'total',ascending=False).index
    # 获取筛选后股票的 ROE（净资产收益率）数据，并按 ROE 降序排序
    q = get_fundamentals(query(indicator.code,indicator.roe).filter(indicator.code.in_(res)).order_by(indicator.roe.desc()).limit(len(res)))
    # 将最终筛选出的股票代码转换为列表
    res = list(q['code'])
    return res   # 返回最终筛选出的股票列表
