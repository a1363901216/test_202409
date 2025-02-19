# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/41917
# 标题：致敬聚宽: 机器学习多因子,50只持仓,14年37倍
# 作者：Gyro^.^

# 克隆自聚宽文章：https://www.joinquant.com/post/10778
# 标题：【量化课堂】机器学习多因子策略
# 作者：JoinQuant量化课堂

# 回测资金需要 12345678

import pandas as pd          # 将 pandas库导入当前环境，并给它指定一个别名 pd
import numpy as np
import datetime as dt
from sklearn.svm import SVR
from jqdata import *

def initialize(context):
    log.set_level('order', 'error')        # 设置日志级别，过滤order和error级别的日志
    set_option('use_real_price', True)     # 用真实价格交易
    set_option('avoid_future_data', True)  # 打开防未来函数

    run_daily(handle_training, 'before_open')  # 每天开盘前运行handle_training函数
    run_daily(handle_trader, 'open')           # 每天开盘时运行handle_trader函数

def handle_trader(context):
    # 初始化变量
    choice = g.choice  # 从全局变量g获取选择的股票列表
    psize  = g.psize   # 从全局变量g获取每只股票的买入金额
    cdata  = get_current_data()  # 获取当前行情数据
    # 卖出逻辑
    for s in context.portfolio.positions:            # 遍历当前持仓的股票
        if s not in choice and not cdata[s].paused:  # 如果股票不在列表中并且股票没有停牌
            log.info('sell', s, cdata[s].name)  # 记录卖出信息
            order_target(s, 0, LimitOrderStyle(cdata[s].last_price))  # 下单将股票卖出到0
    # 买入逻辑
    for s in choice:  # 遍历choice中的股票
        if context.portfolio.available_cash < psize:  # 如果可用现金小于设定的买入金额
            break  # 停止买入
        if s not in context.portfolio.positions and not cdata[s].paused:  # 如果当前持仓中没有该股票并且股票没有停牌
            log.info('buy', s, cdata[s].name)  # 记录买入信息
            order_value(s, psize, LimitOrderStyle(cdata[s].last_price))  # 下股票限价单买入股票

def handle_training(context):
    # 参数设置
    n_position = 50  # 设置持仓股票数量
    n_choice = int(1.2*n_position)  # 设置选股数量，为持仓数量的120%
    index = '399317.XSHE'  # 设置市场指数代码 国证A指
    cdata  = get_current_data()  # 获取当前数据
    dt_last = context.previous_date  # 获取上个交易日日期
    # 股票池
    stocks = get_index_stocks(index, dt_last)  # 获取市场指数的成分股
    # 获取股票的基本面财务数据
    q = query(
            valuation.code,        # 股票代码
            valuation.market_cap,  # 总市值
            balance.total_assets - balance.total_liability,   # 总资产减去总负债，即净资产
            income.net_profit,     # 净利润
            balance.development_expenditure,                  # 开发支出
            valuation.pe_ratio,    # 市盈率
            balance.total_assets / balance.total_liability,   # 总资产/总负债
            indicator.inc_revenue_year_on_year / 100,         # 营业收入同比增长率，除以100转换为小数形式
        ).filter(                  # 过滤条件
            valuation.code.in_(stocks),                       # 只选择股票代码在之前定义的stocks列表中的数据
            balance.total_assets > balance.total_liability,   # 过滤出那些总资产大于总负债的公司，即净资产为正的公司
            income.net_profit > 0,                            # 过滤出那些净利润大于0的公司，即盈利的公司
        )
    # 处理数据
    df = get_fundamentals(q, dt_last).fillna(0).set_index('code')  # 获取数据并设置索引
    df.columns = ['log_mc', 'log_NC', 'log_NI', 'log_RD', 'PE', 'lev', 'grow']  # 设置列名
    # 自定义函数，用于处理数据
    def _sign_ln(X):  # 定义函数，用于计算带符号的自然对数
        return sign(X) * np.log(1.0 + abs(X))
    # 应用自定义函数处理数据
    df['log_mc'] = _sign_ln(df['log_mc'])
    df['log_NC'] = _sign_ln(df['log_NC'])
    df['log_NI'] = _sign_ln(df['log_NI'])
    df['log_RD'] = _sign_ln(df['log_RD'])
    df['PE'] = _sign_ln(df['PE'])
    df['lev'] = _sign_ln(df['lev'])
    df['grow'] = _sign_ln(df['grow'])
    # 行业因子
    industry_list = get_industries('sw_l1', dt_last).index.tolist()  # 获取一级行业列表，并把这个列表转换成python列表格式。
    for sector in industry_list:  # 遍历行业
        istocks = get_industry_stocks(sector, dt_last)  # 获取行业成分股
        s = pd.Series(0, index=df.index)  # 创建0值序列
        s[set(istocks) & set(df.index)] = 1  # 将行业成分股标记为1
        df[sector] = s  # 添加到DataFrame
    # 支持向量回归模型
    svr = SVR(kernel='rbf')  # 创建SVR模型
    # 训练模型
    Y = df['log_mc']  # 目标变量
    X = df.drop('log_mc', axis=1)  # 特征变量
    model = svr.fit(X, Y)  # 训练模型
    # 选择股票
    r = Y - pd.Series(svr.predict(X), Y.index)  # 计算残差
    r = r[r < 0].sort_values().head(n_choice)  # 选择负残差最大的n_choice只股票
    choice = r.index.tolist()  # 将选择的股票代码存储到列表
    # 记录待卖出和待买入的股票
    for s in context.portfolio.positions:  # 遍历当前持仓
        if s not in choice:  # 如果不在choice列表中
            log.info('to sell', s, cdata[s].name)  # 记录卖出信息
    for s in choice:  # 遍历choice列表
        if s not in context.portfolio.positions:  # 如果不在当前持仓中
            log.info('to buy', s, cdata[s].name)  # 记录买入信息
    # 保存结果到全局变量
    g.choice = choice  # 保存选择的股票列表
    g.psize = 1.0/n_position * context.portfolio.total_value  # 保存每只股票的买入金额
# end