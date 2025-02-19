# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/43540
# 标题：根据大小盘相对强弱选择合适的板块股票进行交易
# 作者：Jacobb75

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/10246
# 标题：【量化课堂】RSRS(阻力支撑相对强度)择时策略（上）
# 作者：JoinQuant量化课堂

# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/view/community/detail/28dc6d6605ce7f7471ff05904ccf046f
# 标题：【量化课堂】RSRS(阻力支撑相对强度)择时策略（xia）
# 作者：JoinQuant量化课堂

# 回测资金选择 5000000

# 导入聚宽量化平台的函数库
from jqdata import *
# 导入聚宽因子库，用于获取因子数据
from jqfactor import get_factor_values
# 导入numpy库，用于数据处理
import numpy as np
# 导入pandas库，用于数据处理

# 初始化函数，在策略开始前运行一次
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('399317.XSHE')
    # 使用真实价格进行交易，即不使用前复权价格
    set_option('use_real_price', True)
    # 打开避免未来数据的选项，防止策略中出现未来函数
    set_option("avoid_future_data", True)
    # 设置滑点为0.02，即假设每次交易的滑点为0.02%
    set_slippage(FixedSlippage(0.02))
    # 设置交易成本，包括印花税和佣金
    # 这里设置的是基金的交易成本，买入和卖出时的佣金均为万分之五
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0005, close_commission=0.0005, close_today_commission=0, min_commission=5), type='fund')
    # 设置日志级别，只记录error级别的日志，避免太多无用信息
    log.set_level('system', 'error')
    # 定义指数池，用于后续策略中选择股票
    g.index_pool = [
        '000016.XSHG',  # 上证50指数
        '399303.XSHE',  # 国证2000指数
    ]
    # 定义动量轮动策略的参数
    g.momentum_day = 89  # 判断动量所需天数
    g.position = 1  # 仓位
    g.stock_num = 10  # 持仓数（如果切换到大盘股，则初始10个，之后根据每期选择出来的股票数量动态调整）
    g.stock_num_small = 30  # 持仓数（如果切换到小盘股，持仓数量最多30个）
    # 获取开盘前股票列表的长度
    g.length = len(get_stock_list_before_open(context)[0])
    # 设置交易时间，每月运行
    # 每月的倒数第10个交易日11:00运行卖出策略
    run_monthly(my_sell, monthday=-10, time='11:00', reference_security='399317.XSHE')
    # 每月的倒数第1个交易日11:15运行买入策略
    run_monthly(my_buy, monthday=-1, time='11:15', reference_security='399317.XSHE')

# 模型主体运行函数
def my_select(context):
    # 获取选股列表并过滤掉:st,st*,退市,涨停,跌停,停牌
    check_out_list,g.stock_num = get_stock_list_before_open(context)
    #log.info('今日自选股:%s' % check_out_list)
    log.info('今日股票数量:%s' % g.stock_num)
    return check_out_list

def my_sell(context):
    # 获取选股列表并过滤掉:st,st*,退市,涨停,跌停,停牌
    adjust_position_sell(context, my_select(context))
    
def my_buy(context):
    # 获取选股列表并过滤掉:st,st*,退市,涨停,跌停,停牌
    adjust_position_buy(context, my_select(context))
    
#0 辅助函数
# 获取前n个单位时间当时的收盘价
def get_close_price(code, n, unit='1d'):
    return attribute_history(code, n, unit, 'close', df=False)['close'][0]
  
# 定义 close_position 函数，用于尝试卖出特定股票的所有持仓 
def close_position(code):
    # 使用 order_target_value 函数提交一个订单，目标是将指定股票的持仓价值调整为0，这意味着卖出所有持有的该股票
    # code 参数是要卖出股票的股票代码，此函数可能会因为股票停牌或跌停等原因而失败
    order = order_target_value(code, 0) 
    # 检查 order 是否成功创建，并且订单状态是否为 'held'（挂起），挂起状态意味着订单已提交，但尚未成交
    if order != None and order.status == OrderStatus.held:
        # 如果订单成功提交并且处于挂起状态，记录该股票的卖出状态
        # g.sold_stock 是一个全局字典，用于记录卖出的股票，这里将股票代码对应的值设置为0，表示该股票的卖出操作已经执行
        g.sold_stock[code] = 0

#1-1 根据动量判断市场风格
def get_index_signal(index_pool):
    score_list = []    # 初始化一个列表，用于存储每个指数的得分
    for index in index_pool:
        #分别计算大小盘指数一段时间内预期收益率（最低价格和时间的线性回归斜率*相关系数）的大小，大的强
        # 获取指数过去一段时间内的最低价数据
        data = attribute_history(index, g.momentum_day, '1d', ['low'])
        y = data['log'] = np.log(data.low)
        x = data['num'] = np.arange(data.log.size)
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        # 计算决定系数（r_squared），衡量线性回归的拟合度
        r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
        # 将年化收益率与决定系数相乘，得到该指数的得分
        score = annualized_returns * r_squared
        score_list.append(score)
    index_dict=dict(zip(index_pool, score_list))    # 将指数代码和对应的得分组合成一个字典
    print(index_dict)    # 打印指数及其得分
    sort_list=sorted(index_dict.items(), key=lambda item:item[1], reverse=True)     # 根据得分对指数进行降序排序
    code_list=[]
    for i in range((len(index_pool))):
        code_list.append(sort_list[i][0])
    best_index = code_list[0]      # 选择得分最高的指数，即市场风格偏好的指数
    return best_index    # 返回市场风格偏好的指数代码

#1-2 去掉次新，创业，科创，st
def filter_stock(context):
    # 获取当前市场数据
    curr_data = get_current_data()
    # 获取前一个交易日的日期
    yesterday = context.previous_date
    # 过滤次新股
    #by_date = yesterday
    #by_date = datetime.timedelta(days=1200)
    # 计算三年前的日期，用于过滤掉次新股
    by_date = yesterday - timedelta(days=1200)
    # 获取三年前的所有上市证券代码
    initial_list = get_all_securities(date=by_date).index.tolist()
    # 0. 过滤创业板，科创板，st，今天涨跌停的，停牌的
    # 过滤掉创业板、科创板、ST股票、当天涨跌停的、停牌的股票
    # 使用列表推导式来创建一个新的列表，包含所有不符合条件的股票
    filtered_list = [stock for stock in initial_list if not (
            # 过滤当天涨跌停的股票
            (curr_data[stock].day_open == curr_data[stock].high_limit) or
            (curr_data[stock].day_open == curr_data[stock].low_limit) or
            # 过滤停牌的股票
            curr_data[stock].paused or
            # 过滤ST股票
            ('ST' in curr_data[stock].name) or
            # 过滤名称中包含'*'的股票（通常表示存在风险警示）
            ('*' in curr_data[stock].name) or
            # 过滤名称中包含'退'的股票（通常表示退市风险）
            ('退' in curr_data[stock].name) or
            # 过滤科创板股票（以'688'开头）
            (stock.startswith('688'))
            # 可以继续添加其他过滤条件
    )]
    # 返回过滤后的股票列表
    return filtered_list

#1-3 大盘价值成长风格
def get_value_stock_list(context):
    print("大盘价值成长风格")    # 打印函数运行信息
    # 初始化一个DataFrame，用于存储过滤后的股票数量
    df_stocknum = pd.DataFrame(columns=['过滤后的股票'])
    # 获取前一个交易日的日期
    yesterday = context.previous_date
    # 过滤掉不符合条件的股票，例如次新、创业板、科创板、ST等
    initial_list = filter_stock(context)
    #initial_list = get_index_stocks('000300.XSHG',date=yesterday)

    # 1，流通市值由高到低且≧市场中位数（大盘）
    df1 = get_fundamentals(
        query(
            valuation.circulating_cap,  # 流通市值
            balance.total_current_assets,  # 流动资产
            balance.total_current_liability,  # 流动负债
            indicator.inc_return,  # 扣非净利润
            income.np_parent_company_owners,  # 归母净利润
            valuation.code  # 股票代码
        ).filter(
            valuation.code.in_(initial_list)  # 只选择 initial_list 中的股票
        ),
        date=yesterday  # 使用前一个交易日的日期获取数据
    )
    # 计算流动比率，即流动资产与流动负债的比值
    df1['current'] = df1['total_current_assets'] / df1['total_current_liability']
    # 计算所有股票流动比率的中位数
    current_median = df1['current'].median()
    # 计算所有股票扣非净利润的中位数
    roe_median = df1['inc_return'].median()
    # 筛选出归母净利润大于等于0的股票，并计算这些股票归母净利润的中位数
    A=[one for one in df1['np_parent_company_owners']  if one>=0]
    np_parent_company_owners_median = np.median(A)
    # 根据流通市值降序排列股票
    df1 = df1.sort_values('circulating_cap',ascending=False)
    # 选取市值大于中位数的前50%的股票
    list_1 = list(df1.code)[:int(0.5*len(list(df1.code)))]
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_1)}, ignore_index=True)
    # 2，最近一季流动比率≧市场中位数（现时经营稳健性）
    df2 = get_fundamentals(
        query(
            balance.total_current_assets,  # 流动资产
            balance.total_current_liability,  # 流动负债
            valuation.code  # 股票代码
        ).filter(
            valuation.code.in_(list_1)  # 只选择 list_1 中的股票
        ),
        date=yesterday  # 使用前一个交易日的日期获取数据
    )
    df2['current'] = df2['total_current_assets']/df2['total_current_liability'] # 计算流动比率           
    list_2 = list(df2[df2['current']>current_median].code)    # 筛选出流动比率大于市场中位数的股票
    # 记录当前符合条件股票的数量
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_2)}, ignore_index=True)
    # 3，最近一季扣非ROE≧市场中位数（现时价值性）
    df3 = get_fundamentals(
        query(
            indicator.inc_return,  # 扣非ROE
            valuation.code  # 股票代码
        ).filter(
            valuation.code.in_(list_2)  # 只选择 list_2 中的股票
        ),
        date=yesterday  # 使用前一个交易日的日期获取数据
    )
    # 根据扣非ROE降序排列股票
    df3 = df3.sort_values('inc_return',ascending=False)
    # 筛选出扣非ROE大于市场中位数的股票
    list_3 = list(df3[df3['inc_return'] > roe_median].code)
    # 记录当前符合条件股票的数量
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_3)}, ignore_index=True)

    #4.近4个季度自由现金流量均为正值。（持续经营稳定性）
    df4 = get_history_fundamentals(
        list_3,
        fields=[indicator.code, 
                cash_flow.net_operate_cash_flow,
                cash_flow.net_invest_cash_flow,
                cash_flow.fix_intan_other_asset_acqui_cash,
                cash_flow.fix_intan_other_asset_dispo_cash],
        watch_date=yesterday,
        count=4,
        interval='1q'
    ).dropna()

    #df4['FCF'] = df4['net_operate_cash_flow']-df4['net_invest_cash_flow']
    # 计算自由现金流量（FCF），这里使用的是经营活动产生的现金流量净额减去购建固定资产、无形资产和其他长期资产支付的现金
    df4['FCF'] = df4['net_operate_cash_flow']- df4['fix_intan_other_asset_acqui_cash']
    # 筛选出近4个季度自由现金流量均为正值的股票
    s_delta_avg = df4.groupby('code')['FCF'].apply(
        lambda x: x.min()>0 
        #lambda x: x.iloc[3]  - x.mean() if len(x) == 4 else 0.0 
        #lambda x: x.iloc[11]  - x.mean() if len(x) == 12 else 0.0 
    ).sort_values(
        ascending=False
    )
    # 获取符合条件的股票列表
    list_4 = list(s_delta_avg[s_delta_avg>0].index)
    # 记录当前符合条件股票的数量
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_4)}, ignore_index=True)
    
    # 5.近四季营业利润成长率大于10%（持续成长性）
    df5 = get_history_fundamentals(
        list_4,
        fields=[indicator.code, indicator.inc_operation_profit_year_on_year],
        watch_date=yesterday,
        count=4,
        interval='1q'
    ).dropna()
   
    s_delta_avg = df5.groupby('code')['inc_operation_profit_year_on_year'].apply(
        lambda x: x.min()>10
        #lambda x: x.iloc[3]  - x.mean() if len(x) == 4 else 0.0 
        #lambda x: x.iloc[11]  - x.mean() if len(x) == 12 else 0.0 
    ).sort_values(
        ascending=False
    )
    
    list_5 = list(s_delta_avg[s_delta_avg>0].index)
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_5)}, ignore_index=True)
    
    # 6.连续4个季度归母净利润都大于行业2倍中位数（持续价值性）
    df6 = get_history_fundamentals(
        list_5,
        fields=[indicator.code, income.np_parent_company_owners],
        watch_date=yesterday,
        count=4,
        interval='1q'
    ).dropna()
   
    s_delta_avg = df6.groupby('code')['np_parent_company_owners'].apply(
        lambda x: x.min()> 2 * np_parent_company_owners_median
        #lambda x: x.iloc[3]  - x.mean() if len(x) == 4 else 0.0 
        #lambda x: x.iloc[11]  - x.mean() if len(x) == 12 else 0.0 
    ).sort_values(
        ascending=False
    )
    
    list_6 = list(s_delta_avg[s_delta_avg>0].index)
    #list_6 = list(s_delta_avg[:int(0.5 * len(s_delta_avg))].index)
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_6)}, ignore_index=True)
    
    # 7.按市净率选低的
    df7 = get_fundamentals(query(valuation.pb_ratio,
                                 valuation.code).
                           filter(valuation.code.in_(list_6)),
                           date=context.previous_date)
    
    df7 = df7.sort_values('pb_ratio',ascending=True)
    list_7 = list(df7[:g.stock_num].code)
    df_stocknum =  df_stocknum.append({'当前符合条件股票数量': len(list_7)}, ignore_index=True)
    
    print(df_stocknum)    
    
    stock_list = list_7[:g.stock_num]
    return stock_list,len(list_6)

#1-4 小盘业绩炒作风格
def get_growth_stock_list(context):
    # 打印函数运行信息
    print("小盘业绩炒作风格")
    # 获取前一个交易日的日期
    dt_last = context.previous_date
    # 获取初始股票列表，这通常是通过某种筛选条件得到的
    initial_list = filter_stock(context)  # 假设 filter_stock 函数已定义，用于过滤股票
    #initial_list = get_index_stocks('399303.XSHE',date=yesterday)  
    # all stocks
    # 创建一个空的DataFrame，用于存储符合条件的股票数量
    df_stocknum = pd.DataFrame(columns=['当前符合条件股票数量'])
    # 获取包括股票代码、市净率、净资产收益率、营业总收入同比增长率、净利润同比增长率、经营活动产生的现金流量净额/经营活动净收益等信息
    df = get_fundamentals(query(
            valuation.code
        ).filter(
            valuation.code.in_(initial_list),  # 只选择 initial_list 中的股票
            valuation.pb_ratio > 0,  # 市净率大于0
            indicator.inc_return > 0,  # 净资产收益率大于0
            indicator.inc_total_revenue_year_on_year > 0,  # 营业总收入同比增长率大于0
            indicator.inc_net_profit_year_on_year > 0,  # 净利润同比增长率大于0
            indicator.ocf_to_operating_profit > 5,  # 经营活动产生的现金流量净额/经营活动净收益大于5
        ).order_by(
            valuation.market_cap.asc()  # 市值由小到大排列
		))
    # 根据市值选择股票，选择小盘股
    choice = list(df.code)[int(0 * len(list(df.code))): (int(0 * len(list(df.code))) + g.stock_num_small)]
    #choice = df[5 :5 + g.stock_num]
    #choice = choice[::-1]
    # 记录当前符合条件股票的数量
    df_stocknum = df_stocknum.append({'当前符合条件股票数量': len(df)}, ignore_index=True)
    return choice,len(choice)  # 返回筛选后的股票列表和符合条件的股票数量

#2-1 开盘前确定自选股列表
def get_stock_list_before_open(context):
    index_signal = get_index_signal(g.index_pool)  # 获取市场风格信号，决定使用哪种股票筛选策略
    # 根据市场风格信号选择不同的股票筛选策略
    if index_signal == '000016.XSHG':
        # 如果市场风格信号指示大盘股表现较好，则调用 get_value_stock_list 函数获取大盘价值股
        stock_list, stock_num = get_value_stock_list(context)
    elif index_signal == '399303.XSHE':
        # 如果市场风格信号指示小盘股表现较好，则调用 get_growth_stock_list 函数获取小盘成长股
        stock_list, stock_num = get_growth_stock_list(context)
        # 对于小盘股，设置一个特定的股票数量上限
        stock_num = g.stock_num_small  # 小盘股最多拿30个
    # 更新全局变量，记录今日自选股列表和数量
    g.stock_list = stock_list
    g.stock_num = stock_num
    #print('今日自选股:{}'.format(stock_list[:g.stock_num]))
    return g.stock_list,g.stock_num  # 返回今日自选股列表和数量
    
# 在交易策略中进行股票的卖出操作
def adjust_position_sell(context, buy_stocks):
    index_signal = get_index_signal(g.index_pool)      # 获取市场风格信号
    for stock in context.portfolio.positions:
             # 检查股票是否在买入股票列表中
            if stock in buy_stocks:
                current_data = get_current_data()     # 获取当前市场数据
                now_price = current_data[stock].last_price   # 获取当前股票的最新价格
                open_price = current_data[stock].day_open    # 获取当天开盘价
                # 获取过去两天的收盘价、最高价和成交量数据
                close_data_1d =get_bars(stock, end_dt=context.current_dt, count=2,fields=['close', 'high','volume'], include_now=True)
                # 获取前两天的日期
                time1 = get_price(stock, count = 2, end_date=context.previous_date).index[0].strftime('%Y-%m-%d')
                # 获取持仓的初始时间
                time2 = context.portfolio.positions[stock].init_time.strftime('%Y-%m-%d')     
                # 检查是否满足卖出条件：最高价达到成本价的130%且当前价格从最高价回撤超过5%
                if close_data_1d['high'][-1]>=context.portfolio.positions[stock].avg_cost*1.3 and \
                   (close_data_1d['high'][-1]-now_price)>= context.portfolio.positions[stock].avg_cost*0.05:
                    # 卖出80%的持仓
                    p = context.portfolio.positions[stock].total_amount * 0.2
                    order_target(stock, p)
                    # 记录卖出操作
                    log.info('收益大于30%后，回撤大于5%时平80%的仓位'+str(stock)+str(get_security_info(stock).display_name))
                # 根据不同的市场风格信号和股票的亏损情况决定是否止损，大盘股亏损达到15%时止损
                if  index_signal == '000016.XSHG' and now_price < context.portfolio.positions[stock].avg_cost*0.85:
                    order_target(stock, 0)   
                    log.info('^^^^^^^^^^^^大盘股收益小于等于-15%直接平仓,止损^^^^^^^^^^^^'+str(stock)+str(get_security_info(stock).display_name))  
                # 小盘股亏损达到5%时止损
                if  index_signal == '399303.XSHE' and now_price < context.portfolio.positions[stock].avg_cost*0.95:
                    order_target(stock, 0)   
                    log.info('^^^^^^^^^^^^小盘股收益小于等于-5%直接平仓,止损^^^^^^^^^^^^'+str(stock)+str(get_security_info(stock).display_name))  
            # 如果股票不在买入股票列表中，则全部卖出
            if stock not in buy_stocks:
                order_target(stock,0)
                log.info('持仓不在股票池中，平仓 '+str(stock)+str(get_security_info(stock).display_name)) 

# 定义 adjust_position_buy 函数，用于在交易策略中买入股票
def adjust_position_buy(context, buy_stocks):                
    position_count = len(context.portfolio.positions)    # 获取当前持仓数量
    if g.stock_num >= position_count:  # 检查是否需要买入股票，即预设的股票数量是否大于当前持仓数量
        #value = context.subportfolios[0].cash * g.position / (g.stock_num - position_count)#每只股票的预期买入的价值
        for stock in buy_stocks:      # 遍历待买入股票列表
            #value = context.subportfolios[0].cash * g.position * (g.stock_num - position_count) / sum(range(1,g.stock_num - position_count+1))#每只股票预期买入的价值并非等权重，排在前边的权重高
            #value = context.subportfolios[0].total_value * (g.stock_num - buy_stocks.index(stock)) / sum(range(g.stock_num +1))
            #value = context.subportfolios[0].total_value * 2 / (g.stock_num + buy_stocks.index(stock))
            # 计算每只股票的预期买入价值
            psize = context.portfolio.total_value * 2 / (g.stock_num + 4 + buy_stocks.index(stock))

            #计算股票的p值（50日均高低开收价格为一个随机变量，这个随机变量目前偏离该分布的程度）；低的股票可以适当多买，高的适当少买
            j = []
            # 获取最近一天和过去50天的高低开收价格数据
            G = get_bars(stock, 1, '1d', ['high','low','open','close'],  end_dt=context.current_dt,include_now=True)
            r = np.mean(list(G[0]))
            H = get_bars(stock, 50, '1d', ['high','low','open','close'],  end_dt=context.current_dt,include_now=True)
        
            for a in H:
                k = list(a)
                q = np.mean(k)
                j.append(q)
            # 计算p值，用于判断股票的超买或超卖状态
            p = (r - np.mean(j))/np.std(j) # 这个随机变量理论上服从t(5)分布，5%单侧检验临界值选2.13，10%单侧检验临界值1.53
        
            #根据p值调整股票的仓位，每只股票预期买入的价值为value
            #目前价格落在10%分位数之下，即认为超卖，可以多买；落在5%分位数以上认为超买，少买写
            
            if context.portfolio.available_cash < psize:
                break  # 如果现金不足以买入，终止买入操作
                
            if stock not in context.portfolio.positions:
                # 根据p值判断股票的超买或超卖状态，并相应地调整买入量
                if p < -2:
                    log.info('短期超卖，多买点（150%预期仓位）'+str(stock)+str(get_security_info(stock).display_name))
                    order_target_value(stock, 1.5 * psize) # 短期超卖，增加买入量
   
                elif p > 2:
                    log.info('短期超买，少买点（50%预期仓位）'+str(stock)+str(get_security_info(stock).display_name))
                    order_target_value(stock, 0.5 * psize)    # 短期超买，减少买入量
                else:
                    order_value(stock, psize)  # 正常买入
                # 如果持仓数量达到预设数量，终止买入操作
                if len(context.portfolio.positions) == g.stock_num:
                    break
