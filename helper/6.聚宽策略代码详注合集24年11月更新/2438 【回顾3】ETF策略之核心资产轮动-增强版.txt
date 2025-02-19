# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/view/community/detail/44046
# 标题：【回顾3】ETF策略之核心资产轮动-添油加醋

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/42673
# 标题：【回顾3】ETF策略之核心资产轮动
# 作者：wywy1995

import numpy as np
import pandas as pd


#初始化函数 
def initialize(context):
    # 设定基准
    set_benchmark('513100.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 设置滑点 https://www.joinquant.com/view/community/detail/a31a822d1cfa7e83b1dda228d4562a70
    set_slippage(FixedSlippage(0.002))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0, min_commission=5), type='fund')
    # 过滤一定级别的日志
    log.set_level('system', 'error')
    # 参数
    g.etf_pool = [
        '518880.XSHG', #黄金ETF（大宗商品）
        '513100.XSHG', #纳指100（海外资产）
        '159915.XSHE', #创业板100（成长股，科技股，中小盘）
        '510180.XSHG', #上证180（价值股，蓝筹股，中大盘）
    ]
    g.m_days = 25 #动量参考天数
    run_daily(trade, '9:30') #每天运行确保即时捕捉动量变化

# 基于年化收益和判定系数打分的动量因子轮动 https://www.joinquant.com/post/26142
def get_rank(etf_pool):
    score_list = []  # 初始化得分列表
    for etf in etf_pool:  # 遍历ETF池中的每个ETF
        # 获取每个ETF过去g.m_days天的收盘价，并计算对数收益率
        df = attribute_history(etf, g.m_days, '1d', ['close'])
        y = df['log'] = np.log(df.close)
        x = df['num'] = np.arange(df.log.size)
        # 使用线性回归计算斜率和截距
        slope, intercept = np.polyfit(x, y, 1)
        # 计算年化收益率
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        # 计算R平方值
        r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
        # 计算得分：年化收益率乘以R平方值
        score = annualized_returns * r_squared   # 运用线性回归算出来的年度收益率×R方
        # 计算长周期（8倍周期）的对数收益率，加入反转
        df2 = attribute_history(etf, g.m_days*8, '1d', ['close'])
        y2= df2['log'] = np.log(df2.close)
        x2 = df2['num'] = np.arange(df2.log.size)
        # 使用线性回归计算长周期的斜率和截距
        slope2, intercept2 = np.polyfit(x2, y2, 1)
        # 计算长周期的年化收益率
        annualized_returns2 = math.pow(math.exp(slope2), 250) - 1
        # 计算长周期的R平方值
        r_squared2 = 1 - (sum((y2 - (slope2 * x2 + intercept2))**2) / ((len(y2) - 1) * np.var(y2, ddof=1)))
        # 将长周期的得分以相反的符号加入到得分中，实现动量反转
        score= score - annualized_returns2 * r_squared2 / 6
        # 将计算出的得分添加到得分列表中
        score_list.append(score)
    # 将得分列表转换为DataFrame，并按得分从高到低排序
    df = pd.DataFrame(index=etf_pool, data={'score':score_list})
    df = df.sort_values(by='score', ascending=False) # 从大到小
    return df # 返回得分和排名结果的DataFrame

# 交易；定义函数 trade，用于执行ETF轮动策略和RSRS择时
def trade(context):
    # 获取动量最高的一只ETF
    target_num = 1    
    # 获取ETF动量排名
    rank_df = get_rank(g.etf_pool)
    # 计算得分的标准差
    c = max(list(rank_df.score)) - min(list(rank_df.score))
    # 如果得分的标准差在0.1到15之间，选择动量最高的ETF
    if c < 15 and c>0.1 :
        target_list = list(rank_df.index)[0:target_num]   # 选择动量最高的一只ETF
    else:
        target_list = []  # 如果得分的标准差不在该范围内，则不选择任何ETF
    
    # rsrs择时
    real_target_list = []  # 初始化实际要交易的ETF列表
    for etf in target_list:
        hl = attribute_history(etf, 18, '1d', ['high','low'])  # 获取过去18天的高价和低价数据
        # 计算高价和低价的线性回归斜率
        if np.polyfit(hl.low,hl.high,1)[0] > getBeta(context, etf) :    # 如果斜率大于该ETF的Beta值
            real_target_list.append(etf)    # 将该ETF添加到实际要交易的列表中
    target_list = real_target_list    # 更新目标ETF列表

    # 卖出    
    hold_list = list(context.portfolio.positions)  # 获取当前持有的ETF列表
    for etf in hold_list:
        if etf not in target_list:
            order_target_value(etf, 0)  # 如果ETF不在目标列表中，则卖出
            # print('卖出' + str(etf))  
        # else:
            # print('继续持有' + str(etf))
    # 买入逻辑
    if len(target_list) != 0:   # 如果目标列表不为空
        hold_list = list(context.portfolio.positions)    # 重新获取当前持有的ETF列表
        if len(hold_list) < target_num:  # 如果当前持有的ETF数量小于目标数量
            value = context.portfolio.available_cash / (target_num - len(hold_list))    # 计算每笔交易的金额
            for etf in target_list:
                if context.portfolio.positions[etf].total_amount == 0:   # 如果当前未持有该ETF
                    order_target_value(etf, value)    # 下单买入该ETF
                    # print('买入' + str(etf))

def getBeta(context, etf) :
    beta = 0
    if etf == '518880.XSHG': beta = countBeta(context, '518880.XSHG')   # 黄金ETF（大宗商品）
    if etf == '513100.XSHG': beta = countBeta(context, '513100.XSHG')   #纳指100（海外资产）
    if etf == '159915.XSHE': beta = countBeta(context, '159915.XSHE')   #创业板100（成长股，科技股，中小盘）
    if etf == '510180.XSHG': beta = countBeta(context, '510180.XSHG')   #上证180（价值股，蓝筹股，中大盘）
    return beta

# 定义函数 countBeta，用于计算给定ETF的Beta值
def countBeta(context, etf):
    # 获取过去250个交易日的高价和低价数据
    etf_data = attribute_history(etf, 250, '1d', fields=['high','low'])
    # 初始化Beta值列表
    betaList = []
    # 遍历数据，每次取20天的数据进行线性回归计算Beta
    for i in range(0,len(etf_data)-21):
        df = etf_data.iloc[i:i+20,:]    # 取从第i天起的20天数据
        betaList.append(np.polyfit(df.low,df.high,1)[0])
    # 返回调整后的Beta值，即平均值减去两倍的标准差
    # 这个调整可能是为了降低估计的波动性或噪声
    return (mean(betaList)-2*std(betaList))
