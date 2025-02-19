# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42949
# 标题：动量因子加RSRS择时和ETF轮动每日调仓
# 作者：wentaogg

# 参考以下策略，感谢两位大佬
# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/26142
# 标题：基于动量因子的ETF轮动加上RSRS择时
# 作者：慕长风
# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42673
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

from jqdata import *    # 从jqdata模块导入所有内容
import numpy as np      # 导入numpy库

def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 将滑点设置为0
    set_slippage(FixedSlippage(0.004))    
    # 设置交易成本万分之五
    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0.0005, close_commission=0.0005, close_today_commission=0, min_commission=5),
                   type='fund')
    log.set_level('order', 'error')       # 过滤order中低于error级别的日志
    # 初始化全局变量g.index_pool，用于存放策略中关注的指数或ETF
    g.index_pool = [
        '518880.XSHG', #黄金ETF（大宗商品）'518880.XSHG'
        '513100.XSHG', #纳指100（海外资产）
        '159915.XSHE', #创业板100（成长股，科技股，题材性，中小盘）
        # '515080.XSHG', #红利ETF（价值股，蓝筹股，防御性，中大盘） '515080.XSHG'
        # '159928.XSHE', # 中证消费ETF
        '510180.XSHG',#上证180（价值股，蓝筹股，中大盘）
        # '159981.XSHE'#能源化工期货
    ]
    # 初始化全局变量，设置最大持仓数为1
    g.stock_num = 1
    # 设置动量天数为25天，用于计算股票的动量
    g.momentum_day = 25
    # 设置股票代码为沪深300指数
    g.stock = '000300.XSHG'
    # 设置N周期为18天，用于计算RS指标
    g.N = 18
    # 设置M周期为600天，用于计算RS指标
    g.M = 600
    # 设置均值计算天数为20天
    g.mean_day = 20 
    g.mean_diff_day = 3     # 设置比较均线时的前后天数差为3天
    g.score_threshold = 0.7 # 设置RS标准分指标阈值为0.7
    g.slope_series = initial_slope_series()[:-1]  # 初始化斜率序列，这里调用initial_slope_series函数并去掉序列中的第一个元素
     # 设置交易时间，每周任意时间执行一次
    # run_weekly(trade, weekday=3, time='9:45',reference_security='000300.XSHG')
    run_daily(trade,time='9:45')   # 设置交易时间，这里设置为每个交易日的9:45执行trade函数
  
# 定义 trade 函数，用于执行策略的交易逻辑
def trade(context):
    # 获取当前持仓的股票集合
    stock_hold = set(context.portfolio.positions.keys())
    # 调用 get_stock_pool 函数获取策略选定的股票池
    stock_pool = get_stock_pool()
    # 设置要持有的股票数量，不超过预设的最大持仓数 g.stock_num，也不超过股票池的大小
    g.stock_number = g.stock_num if (len(stock_pool) > g.stock_num) else len(stock_pool)
    # print(g.stock_number)
    # 选择股票池中的前 g.stock_num 只股票作为最终要买入的股票集合
    set_stock_pool=set(stock_pool[:g.stock_num])
    print(f'持仓股票{stock_hold}')  # 打印当前持仓的股票集合
    # print(f'所有要买入的股票{stock_pool}')
    print(f'最后要买入股票{set_stock_pool}')
    signal = get_signal()      # 获取交易信号
    print(signal)       # 打印交易信号
    
    # 如果有符合买入条件的股票池，并且当前交易信号不是卖出信号
    if stock_pool and signal != "SELL":
        # 如果当前持仓的股票集合与要买入的股票集合相同，且非空，不进行调仓
        if stock_hold == set_stock_pool and len(set_stock_pool) != 0:
            print("当前持仓和需买入集合一致，不进行调仓")
        else:
            # 否则，调用 change_position 函数调整持仓
            change_position(context, set_stock_pool)
    else:
        # 如果交易信号是卖出信号
        if signal == "SELL":
            print("RSRS择时模型发出清仓信号！")
        # 卖出当前持仓的所有股票
        for stock in stock_hold:
            # 将每只股票的目标持仓量设置为0，即全部卖出
            order_target_value(stock, 0)
    log.info('美好的一天结束')
    log.info('##############################################################')

# 定义 get_stock_pool 函数，用于获取筛选和排名后的股票池
def get_stock_pool():
    ''' 对指数池内股票进行筛选和排名
    Returns:
        tuple of stock_code
    '''
    # index_pool = [index for index, stock in g.index_pool]
    index_rank = []  # 初始化一个列表，用于存储每个指数及其评分
    for index in g.index_pool:
        score = get_socre(index)       # 调用 get_socre 函数获取当前指数的评分
        # print((index,score))
        # if score > 0:                
        index_rank.append((index, score))   # 将指数和其评分作为一个元组添加到 index_rank 列表中
    # 对 index_rank 列表进行排序，根据评分降序排列
    # 使用 lambda 函数指定按照元组中的第二个元素（评分）进行排序
    index_rank = sorted(index_rank, key=lambda x: x[1], reverse=True)
    # 将排序后的 index_rank 列表转换为字典，其中键是指数代码，值是评分
    index_dict = dict(index_rank)
    # 使用 record 函数记录每个指数的评分，以便在聚宽平台上显示
    record(黄金 = round(index_dict['518880.XSHG'], 2))
    record(纳指 = round(index_dict['513100.XSHG'], 2))
    record(成长 = round(index_dict['159915.XSHE'], 2))
    record(价值 = round(index_dict['510180.XSHG'], 2))
    # return tuple(index_dict[index[0]] for index in index_rank)
    return tuple(index[0] for index in index_rank)   # 返回一个由指数代码组成的元组，这些指数是根据评分排名靠前的

def get_socre(stock):
    ''' 基于股票年化收益和判定系数打分
    Returns:
        score (float): score of stock
    '''
    # 获取过去 g.momentum_day 天的股票收盘价格
    data = attribute_history(stock, g.momentum_day, '1d', ['close'])
    # 计算对数收益率
    y = data['log'] = np.log(data.close)
    # 创建一个与天数相同大小的数组，用于线性回归的x值
    x = data['num'] = np.arange(data.log.size)
    # 使用 numpy 的 polyfit 函数进行一次线性回归，得到斜率和截距
    slope, intercept = np.polyfit(x, y, 1)
    # 根据斜率计算年化收益率
    annualized_returns = math.pow(math.exp(slope), 250) - 1
    # 计算判定系数 R^2
    r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
    # 返回年化收益率与判定系数的乘积作为股票的评分
    return annualized_returns * r_squared

# 定义 change_position 函数，用于调整持仓
def change_position(context, target_list):
    # 获取当前持仓列表
    hold_list = list(context.portfolio.positions)
    # 卖出当前持仓的所有股票
    for etf in hold_list:
        order_target_value(etf, 0)  # 将持仓量调整为0，即全部卖出
        print('卖出' + str(etf))
        
    # 如果目标股票数量大于0，则进行买入操作
    if g.stock_number > 0:
        # 计算每只股票的买入金额
        value = context.portfolio.available_cash / g.stock_number
        # 遍历目标股票列表，买入每只股票
        for etf in target_list:
            # 如果该股票不在当前持仓中，则买入
            if context.portfolio.positions[etf].total_amount == 0:
                order_target_value(etf, value)  # 将目标金额调整为value
                print('买入' + str(etf))

# 定义 get_signal 函数，用于产生交易信号
def get_signal():
    ''' 产生交易信号
    Returns:
        str: "BUY" or "SELL" or "KEEP"
    '''
    # 获取过去 g.mean_day + g.mean_diff_day 天的收盘价数据
    close_data = attribute_history(g.stock, g.mean_day + g.mean_diff_day, '1d', ['close'])
    # 计算今天及之前 g.mean_diff_day 天的收盘价均值
    today_MA = close_data.close[g.mean_diff_day:].mean() 
    # 计算之前 g.mean_day 天的收盘价均值
    before_MA = close_data.close[:-g.mean_diff_day].mean()
    # 获取过去 g.N 天的最高价和最低价数据
    data = attribute_history(g.stock, g.N, '1d', ['high', 'low'])
    # 调用 get_ols 函数进行 OLS 回归分析，得到截距、斜率和判定系数
    intercept, slope, r2 = get_ols(data.low, data.high)
    # 将新计算的斜率添加到 g.slope_series 列表中
    g.slope_series.append(slope)
    # 计算 RS/RS 指标的 z 分数，并乘以判定系数 r2
    rsrs_score = get_zscore(g.slope_series[-g.M:]) * r2
    # 根据 RS/RS 指标的 z 分数和均线比较结果产生交易信号
    if rsrs_score > g.score_threshold and today_MA > before_MA:
        return "BUY"
    elif rsrs_score < -g.score_threshold and today_MA < before_MA:
        return "SELL"
    else:
        return "KEEP"

# 定义 get_ols 函数，用于对输入的自变量和因变量建立 OLS 回归模型
def get_ols(x, y):
    ''' 对输入的自变量和因变量建立OLS回归模型
    Args:
        x (series of x): 每日最低价
        y (series of y): 每日最高价
    Returns：
        tuple: (截距，斜率，判定系数)
    '''
    # 使用 numpy 的 polyfit 函数进行一次线性回归，得到斜率和截距
    slope, intercept = np.polyfit(x, y, 1)
    # 计算判定系数 R^2
    r2 = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
    # 返回截距、斜率和判定系数
    return (intercept, slope, r2)

# 定义 initial_slope_series 函数，用于初始化前 M 日内的斜率时间序列
def initial_slope_series():
    ''' 初始化前M日内的斜率时间序列
    Returns：
        list of slope (float)
    '''
    # 获取过去 g.N + g.M 天的最高价和最低价数据
    data = attribute_history(g.stock, g.N + g.M, '1d', ['high', 'low'])
    # 使用列表推导式计算序列中每 g.N 天的斜率
    # 对于序列中的每个 i，计算从 i 到 i+g.N 的最高价和最低价的 OLS 回归斜率
    return [get_ols(data.low[i:i+g.N], data.high[i:i+g.N])[1] for i in range(g.M)]

# 定义 get_zscore 函数，用于通过斜率序列计算标准分
def get_zscore(slope_series):
    ''' 通过斜率序列计算标准分
    Returns:
        float
    '''
    # 计算斜率序列的均值
    mean = np.mean(slope_series)
    # 计算斜率序列的标准差
    std = np.std(slope_series)
    # 计算最后一个斜率与序列均值的差，然后除以标准差，得到标准分
    return (slope_series[-1] - mean) / std