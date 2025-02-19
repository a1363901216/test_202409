# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/43194
# 标题：14年初到23年7月，年化37，夏普1.25，稳健高收益！
# 作者：侧耳闻鹿鸣

from jqdata import *          # 导入聚宽数据接口模块

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000002.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    g.security=get_index_stocks('000985.XSHG')  # 获取科技100指数的成分股
    g.q=query(valuation,indicator).filter(valuation.code.in_(g.security))  # 构建查询对象，用于查询这些成分股的财务数据和市场因子
    run_weekly(period,weekday=1);  # 计划每周第一个交易日运行period函数

# 定期运行的函数
def period(context):
    # 查询并获取股票的财务数据和市场因子
    df=get_fundamentals(g.q)[['code','inc_net_profit_year_on_year','roe','pb_ratio','pe_ratio','market_cap']]
    df=df[df['roe']>4]  # 过滤出ROE大于4%的股票
    # 按市净率排序，并为市净率排名
    df=df.sort_values('pb_ratio')
    df['pbrank']=df['pb_ratio'].rank()
    df=df.iloc[:100,:]     # 选取排名前100的股票
    # 按净利润同比增长排序，并为净利润同比增长排名
    df=df.sort_values('inc_net_profit_year_on_year')
    df['profitrank']=df['inc_net_profit_year_on_year'].rank()
    df=df.iloc[-50:,:]  # 选取排名后50的股票
    # 选择最终要持有的股票
    to_hold=df['code'].values
    # 获取这些股票过去400天的收盘价数据
    dff=history(count=100*20,unit='1d',field='close',security_list=to_hold)
    dfff=dff.T  # 获取最新的收盘价
    print(dfff.T)  # 输出获取的价格数据
    dfff_close=dfff.iloc[:,-1:]  # 获取最新的收盘价
    dfff=dfff.iloc[:,::20]   # 获取除了最新价格外，之前的价格数据
    #要注意这里虽然是20个一统计，但是最近的那一次是按照距离今天20日前的数据！
    dfff_jc=dfff.iloc[:,-1:]
    dfff=dfff.iloc[:,:-1]
    dfff_jc2=pd.concat([dfff_jc,dfff_close],axis=1,ignore_index=True)
    dfff_jc2.columns=['jc','close']
    dfff_jc2=dfff_jc2.T.drop_duplicates().T
    # 将价格数据与最新的收盘价合并
    dfff=pd.concat([dfff,dfff_jc2],axis=1,ignore_index=True)
    print(dfff.T)
    
    # 计算2000日均线
    mal = dfff.mean(axis=1)
    # 计算20日均线
    mas = dfff.iloc[:, -20:]
    mas = mas.mean(axis=1)
    # 将20日均线和2000日均线合并为一个DataFrame
    maa=pd.DataFrame([mas,mal])
    maa=maa.T
    #print(maa.head())
    maa.columns=['mas','mal']
    # 计算短期均线与长期均线的差
    maa['mac']=maa['mas']-maa['mal']
    # 选择短期均线低于长期均线的股票
    buy=maa[maa['mac']<0]#短均低于长均
    buy['code']=buy.index
    buy=buy['code'].values
    #print(buy)
    # 卖出不在买入列表中的股票
    for stock in context.portfolio.positions:
        if stock not in buy:
            order_target_value(stock,0)     # 如果手里有但不符合全清空
    # 买入符合条件但尚未持有的股票
    to_buy=[stock for stock in buy if stock not in context.portfolio.positions]
    #这是符合条件但没有的
    if len(to_buy)>0:
        cash_per_stock=context.portfolio.available_cash/len(to_buy)   # 把现有的钱平分给这些股票
        for stock in to_buy:
            order_value(stock, cash_per_stock)
    print("现在持有股票数量：",len(context.portfolio.positions))  # 输出当前持有的股票数量
