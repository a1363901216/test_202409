# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42673
# 资料：https://www.joinquant.com/view/community/detail/a31a822d1cfa7e83b1dda228d4562a70
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

import numpy as np     # 将 numpy库导入当前环境，并给它指定一个别名 np
import pandas as pd    # 将 pandas库导入当前环境，并给它指定一个别名 pd

def initialize(context):                   # 初始化函数 
    set_benchmark('000300.XSHG')           # 设定基准
    set_option('use_real_price', True)     # 用真实价格交易
    set_option("avoid_future_data", True)  # 打开防未来函数
    set_slippage(FixedSlippage(0.000))     # 设置滑点 
    # 设置交易成本           买入印花税  卖出印花税    买入佣金                卖出佣金                   平今仓佣金             每笔佣金最低扣5块钱  类型:场内基金
    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0, min_commission=5), type='fund')
    log.set_level('system', 'error')       # 过滤一定级别的日志
    # 参数
    g.etf_pool = [
        '518880.XSHG', # 黄金ETF（大宗商品）
        '513100.XSHG', # 纳指100（海外资产）
        '159915.XSHE', # 创业板100（成长股，科技股，中小盘）
        '510180.XSHG', # 上证180（价值股，蓝筹股，中大盘）
    ]
    g.m_days = 25             # 动量参考天数
    run_daily(trade, '9:30')  # 每天运行确保即时捕捉动量变化

def get_rank(etf_pool):    # 基于年化收益和判定系数打分的动量因子轮动 https://www.joinquant.com/post/26142
    score_list = []        # 初始化一个空列表，用来存储每个ETF的分数
    for etf in etf_pool:   # 遍历ETF候选池中的每个ETF
        df = attribute_history(etf, g.m_days, '1d', ['close'])  # 获取单个ETF最近g.m_days个交易日的收盘价
        y = df['log'] = np.log(df.close)         # 计算收盘价的自然对数，添加到df的'log'列
        x = df['num'] = np.arange(df.log.size)   # 创建一个从0到df.log.size-1的整数序列，添加到df的'num'列
        slope, intercept = np.polyfit(x, y, 1)   # 使用线性拟合计算斜率和截距
        annualized_returns = math.pow(math.exp(slope), 250) - 1  # 将线性模型的斜率转换成一个年化的收益率
        r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))  # 计算判定系数R平方
        score = annualized_returns * r_squared  # 计算得分，即年化收益率与R平方的乘积
        score_list.append(score)                # 将计算得到的分数添加到score_list列表中
    df = pd.DataFrame(index=etf_pool, data={'score':score_list})  # 创建一个DataFrame，其中索引是ETF代码，列是对应的分数
    df = df.sort_values(by='score', ascending=False)              # 根据分数列的值对DataFrame进行降序排序
    rank_list = list(df.index)    # 获取排序后的ETF索引，即排名
    print(df)     # 打印DataFrame
    record(黄金 = round(df.loc['518880.XSHG'], 2))  # 记录并打印'518880.XSHG'的分数，保留两位小数
    record(纳指 = round(df.loc['513100.XSHG'], 2))  # 记录并打印'513100.XSHG'的分数，保留两位小数
    record(成长 = round(df.loc['159915.XSHE'], 2))  # 记录并打印'159915.XSHE'的分数，保留两位小数
    record(价值 = round(df.loc['510180.XSHG'], 2))  # 记录并打印'510180.XSHG'的分数，保留两位小数
    return rank_list  # 返回排名列表

# 交易操作
def trade(context):
    # 获取动量最高的一只ETF
    target_num = 1    # 设置目标数量为1，即动量最高的1只ETF
    target_list = get_rank(g.etf_pool)[:target_num]  # 从get_rank函数中获取动量最高的ETF列表，并选取前target_num只
    # 卖出操作   
    hold_list = list(context.portfolio.positions)    # 获取帐户当前持仓的ETF列表
    for etf in hold_list:
        if etf not in target_list:       # 如果ETF不在目标列表中
            order_target_value(etf, 0)   # 下单将该ETF的持仓目标调整为0，即全部卖出
            print('卖出' + str(etf))     # 打印卖出的ETF代码
        else:
            print('继续持有' + str(etf)) # 打印继续持有的ETF代码
    # 买入操作
    hold_list = list(context.portfolio.positions)    # 获取帐户当前持仓的ETF列表
    if len(hold_list) < target_num:                  # 如果当前持仓的ETF数量小于目标数量
        value = context.portfolio.available_cash / (target_num - len(hold_list)) # 计算每只ETF可以分配的资金
        for etf in target_list:   # 遍历目标ETF列表
            if context.portfolio.positions[etf].total_amount == 0:  # 如果当前没有持有该ETF
                order_target_value(etf, value)       # 下单买入该ETF，分配计算得到的资金
                print('买入' + str(etf))             # 打印买入的ETF代码
