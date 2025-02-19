# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/41155
# 标题：wywy1995大神机器学习策略年化提升35pt
# 作者：斯科尔斯

# https://www.joinquant.com/view/community/detail/30684f8d65a74eef0d704239f0eec8be?type=1&page=5

#导入函数库
from jqdata import *            # 导入聚宽量化平台的函数库
from jqfactor import *          # 导入聚宽因子库
import numpy as np              # 导入numpy库，用于数据处理
import pandas as pd             # 导入pandas库，用于数据处理
import statsmodels.api as sm    # 导入statsmodels库，用于统计分析

# 初始化函数，在策略开始前运行一次
def initialize(context):
    # 设定沪深300指数为策略的基准
    set_benchmark('000300.XSHG')
    # 使用真实价格进行交易，即不使用前复权价格
    set_option('use_real_price', True)
    # 打开避免未来数据的选项，防止策略中出现未来函数
    set_option("avoid_future_data", True)
    # 设置滑点为0，即假设交易没有滑点
    set_slippage(FixedSlippage(0))
    # 设置交易成本，包括印花税和佣金
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='stock')
    # 设置日志级别，只记录error级别的日志，避免太多无用信息
    log.set_level('order', 'error')
    #初始化全局变量
    g.stock_num = 10  # 策略持仓的股票数量
    g.hold_list = []  # 当前持仓的全部股票列表
    g.target_list = []  # 目标股票列表，可能用于调仓
    g.yesterday_HL_list = []  # 记录昨日涨停的股票
    g.not_buy_again = []  # 记录不再买入的股票
    g.factor_list = [
        'price_no_fq', # 不复权价格因子，用于技术分析
        'total_profit_to_cost_ratio',  # 成本费用利润率，质量类因子
        'inventory_turnover_rate'  # 存货周转率，质量类因子
        ]
    # 设置交易运行时间
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG') # 设置交易运行时间，每天9:05运行prepare_stock_list函数
    # 为了实盘的时候，买掉股票后可以立马成交，从而可以买入新股票，所以选择9:24
    run_weekly(weekly_adjustment, 1, time='9:30', reference_security='000300.XSHG') # 每周第一个交易日9:30运行weekly_adjustment函数，用于周级别的调仓
    run_daily(trade_morning,time='9:26',reference_security='000300.XSHG') # 每个交易日9:26运行trade_morning函数，用于早盘交易
    run_daily(trade_afternoon, time='13:00', reference_security='000300.XSHG')  # 每个交易日13:00运行trade_afternoon函数，用于下午检查持仓中的涨停股是否需要卖出
    #run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')

#1-1 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []   # 初始化持仓列表，g.hold_list 用于存储当前持仓的股票代码
    for position in list(context.portfolio.positions.values()):   # 遍历当前持仓的每个持仓价值
        stock = position.security    # 获取股票代码
        g.hold_list.append(stock)    # 将股票代码添加到持仓列表
    #获取昨日涨停列表
    if g.hold_list != []:            # 如果当前持仓列表不为空，即存在持仓
        # 获取每只股票的昨日收盘价和涨停价
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]  # 筛选出昨日收盘价等于涨停价的股票，即昨日涨停的股票
        g.yesterday_HL_list = list(df.code)       # 更新昨日涨停列表，存储为股票代码列表
    else:
        g.yesterday_HL_list = []                  # 如果没有持仓，昨日涨停列表为空
    g.target_list = get_stock_list(context)       # 提早准备今日股票list

#1-2 选股模块
def get_stock_list(context):
    yesterday = context.previous_date    # 昨天的日期，用于获取历史数据
    initial_list = get_all_securities().index.tolist()     # 获取所有股票的初始列表
    initial_list = filter_new_stock(context, initial_list)    # 过滤新上市的股票
    initial_list = filter_kcbj_stock(initial_list)    # 过滤科创板及创业板股票
    initial_list = filter_st_stock(initial_list)    # 过滤ST股票
    # 获取筛选后股票列表的因子值
    factor_values = get_factor_values(initial_list, [
        g.factor_list[0],  # 不复权价格因子
        g.factor_list[1],  # 成本费用利润率
        g.factor_list[2],  # 存货周转率
        ], end_date=yesterday, count=1)
    df = pd.DataFrame(index=initial_list, columns=factor_values.keys())    # 创建一个空的DataFrame，用于存储因子值
    # 将因子值填充到DataFrame中
    df[g.factor_list[0]] = list(factor_values[g.factor_list[0]].T.iloc[:,0])
    df[g.factor_list[1]] = list(factor_values[g.factor_list[1]].T.iloc[:,0])
    df[g.factor_list[2]] = list(factor_values[g.factor_list[2]].T.iloc[:,0])
    df = df.dropna()    # 删除DataFrame中的空值
    # 定义因子的系数列表
    coef_list = [
        -6.123355346008858e-05,  # 对应不复权价格因子的系数
        -0.002579342458393642,   # 对应成本费用利润率的系数
        -2.194257357346814e-06   # 对应存货周转率的系数
        ]
    # 计算每只股票的总评分
    df['total_score'] = coef_list[0]*df[g.factor_list[0]] + coef_list[1]*df[g.factor_list[1]] + coef_list[2]*df[g.factor_list[2]]
    # 根据总评分对股票进行降序排序
    df = df.sort_values(by=['total_score'], ascending=False)   # 分数越高即预测未来收益越高，排序默认降序
    # 选择得分最高的前10%或指定数量的股票
    complex_factor_list = list(df.index)[:max(int(0.1*len(list(df.index))),g.stock_num)]
    # 查询这些股票的市值和每股收益信息
    q = query(valuation.code,valuation.circulating_market_cap,indicator.eps).filter(valuation.code.in_(complex_factor_list)).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q)
    df = df[df['eps']>0]    # 过滤每股收益为负的股票
    final_list  = list(df.code)    # 获取最终的股票列表
    final_list = filter_paused_stock(final_list)    # 过滤停牌的股票
    final_list = filter_limitup_stock(context, final_list)    # 过滤涨停的股票
    final_list = filter_limitdown_stock(context, final_list)    # 过滤跌停的股票
    return final_list    # 返回最终的股票列表

#1-3 整体调整持仓
def weekly_adjustment(context):
    g.not_buy_again = []    # 清空不再买入的股票列表
    target_list = g.target_list    # 从全局变量中获取目标股票列表
    # 获取应买入列表，并确保不超过最大持仓数，取目标列表的前 g.stock_num 个股票，或者列表本身的全部（如果目标列表股票数量不足 g.stock_num）
    target_list = target_list[:min(g.stock_num, len(target_list))]
    #调仓卖出
    for stock in g.hold_list:      # 遍历当前持仓列表 g.hold_list
        if (stock not in target_list) and (stock not in g.yesterday_HL_list):  # 如果股票不在目标列表中，并且不是昨天涨停的股票
            log.info("卖出[%s]" % (stock))      # 记录卖出操作
            position = context.portfolio.positions[stock]      # 获取该股票的持仓信息
            close_position(position)    # 卖出该股票的持仓
        else:
            log.info("持有[%s]" % (stock))      # 如果股票在目标列表中或昨天涨停，记录持有操作
    # 调仓买入
    buy_security(context,target_list)
    #记录已买入的股票
    for position in list(context.portfolio.positions.values()):    # 遍历当前所有持仓
        stock = position.security    # 获取股票代码
        g.not_buy_again.append(stock)  # 将买入的股票添加到不再买入列表中
    #微信通知 
    if context.run_params == 'sim_trade':  # 如果策略是在模拟交易环境中运行，发送微信通知
        # 构造微信通知文本，卖出股票和买入股票的列表，使用 set 运算来确定
        weixin_message_text = '\n卖出:'.join(list(set(g.hold_list).difference(set(target_list))))\
            +'\n买入:'.join(list(set(target_list).difference(set(g.hold_list))))
        send_message(weixin_message_text,channel='weixin')    # 发送微信消息
        #send_email(list(set(g.hold_list).difference(set(target_list))),list(set(target_list).difference(set(g.hold_list))))

#1-4 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt   # 获取当前时间
    if g.yesterday_HL_list != []:   # 检查昨日涨停股票列表是否为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.yesterday_HL_list:  # 遍历昨日涨停股票列表
            # 获取当前股票的最新价格和涨停价
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0,0] <    current_data.iloc[0,1]:   # 检查当前价格是否小于涨停价，即判断今日是否打开涨停
                log.info("[%s]涨停打开，卖出" % (stock))             # 如果打开涨停，则记录卖出信息
                position = context.portfolio.positions[stock]        # 获取股票当前的持仓情况
                close_position(position)                             # 卖出股票持仓
            else:
                log.info("[%s]涨停，继续持有" % (stock))             # 如果仍然涨停，则记录继续持有的信息

#1-5 如果昨天有股票卖出或者买入失败，剩余的金额今天早上买入
def check_remain_amount(context):
    g.hold_list= []    # 清空当前持仓列表
    for position in list(context.portfolio.positions.values()):    # 遍历当前策略的持仓
        stock = position.security     # 获取每只股票的股票代码
        g.hold_list.append(stock)        # 将股票代码添加到持仓列表中
    if len(g.hold_list) < g.stock_num:  # 如果当前持仓的股票数量小于预设的最大持仓数 g.stock_num
        print('有余额可用，多买入一只'+str(context.portfolio.cash))    # 打印信息，提示有余额可用，并显示当前的现金余额
        target_list = g.target_list     # 获取目标股票列表
        #剔除本周一曾买入的股票，不再买入
        target_list = filter_not_buy_again(target_list)      # 从目标列表中剔除本周一曾尝试买入但失败的股票
        target_list = target_list[:min(g.stock_num, len(target_list))]        # 确定最终要购买的股票列表，数量不超过预设的最大持仓数和目标列表的长度
        buy_security(context,target_list)   # 调用 buy_security 函数来执行买入操作

# 1-6 下午检查交易
def trade_afternoon(context):
    check_limit_up(context)   # 调用 check_limit_up 函数，检查持仓中是否有涨停股票需要处理
    check_remain_amount(context)  # 调用 check_remain_amount 函数，检查是否有剩余资金可用于购买股票

# 用于每个交易日早上执行的操作
def trade_morning(context):
    # 周一不执行，因为周一主交易代码，这里只是补充,0代表周一  
    if context.current_dt.weekday() in (2,3,4,5):   # 这里 if 语句的条件是检查是否为周二至周五，因为周一的主要交易代码已经执行，这里不需要重复执行
        check_remain_amount(context)  # 如果不是周一，则调用 check_remain_amount 函数，检查是否有剩余资金可用于购买股票

# 2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()   # 获取当前所有股票的数据
	return [stock for stock in stock_list if not current_data[stock].paused]  # 使用列表推导式，返回不在停牌状态的股票列表

# 2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()       # 获取当前所有股票的数据
	return [stock for stock in stock_list   # 使用列表推导式，返回不是ST股且名称中不包含特定退市标签的股票列表
			if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name]

# 2-3 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:  # 遍历股票列表，过滤掉科创板和北交所的股票
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':  # 根据股票代码判断是否属于科创板或北交所
            stock_list.remove(stock)  # 从列表中移除该股票
    return stock_list

# 2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)   # 获取最近一分钟的收盘价
	current_data = get_current_data()  # 获取当前所有股票的数据
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()   # 使用列表推导式，返回不在涨停状态或已持有的股票列表
			or last_prices[stock][-1] < current_data[stock].high_limit]

# 2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) # 获取最近一分钟的收盘价
    current_data = get_current_data() # 获取当前所有股票的数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() # 使用列表推导式，返回不在跌停状态或已持有的股票列表
            or last_prices[stock][-1] > current_data[stock].low_limit]

# 2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date  # 获取昨日日期
    # 使用列表推导式，返回上市时间超过375日的股票列表
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=375)]

# 2-7 删除本周一买入的股票
def filter_not_buy_again(stock_list):
    return [stock for stock in stock_list if stock not in g.not_buy_again]

# 3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0:   # 检查目标价值是否为0，即是否需要清仓
        log.debug("Selling out %s" % (security)) # 日志记录清仓信息
    else:
        log.debug("Order %s to value %f" % (security, value)) # 日志记录下单到特定价值的信息
    return order_target_value(security, value)   # 调用聚宽平台的order_target_value函数执行交易

# 3-2 交易模块-开仓
def open_position(security, value):
    order = order_target_value_(security, value)  # 尝试对指定股票进行下单到特定价值
    if order != None and order.filled > 0:        # 检查订单是否创建成功且有成交
        return True  # 开仓成功
    return False  # 开仓失败

# 3-3 交易模块-平仓
def close_position(position):
    security = position.security  # 获取持仓的股票代码
    order = order_target_value_(security, 0)  # 下单清仓 ；可能会因停牌失败
    if order != None:  # 检查订单是否创建成功
        if order.status == OrderStatus.held and order.filled == order.amount:  # 检查订单状态是否为全部成交且订单数量等于下单数量
            return True  # 平仓成功
    return False  # 平仓失败

# 3-4 用于执行买入操作
def buy_security(context,target_list):
    #调仓买入
    position_count = len(context.portfolio.positions)    # 获取当前持仓的数量
    target_num = len(target_list)    # 获取目标股票列表的长度
    if target_num > position_count:    # 如果目标股票的数量大于当前持仓的数量，表示需要增加持仓
        # 计算每个股票应该分配的资金。这里将总现金除以需要买入的股票数量（target_num - position_count）
        value = context.portfolio.cash / (target_num - position_count)
        for stock in target_list:   # 遍历目标股票列表
            if stock not in context.portfolio.positions:     # 检查该股票是否已经在持仓中
                if open_position(stock, value):    # 尝试开仓买入该股票，传入股票代码和计划投入的资金
                    log.info("买入[%s]（%s元）" % (stock,value))     # 如果买入成功，记录此次操作
                    g.not_buy_again.append(stock)       # 将该股票添加到不再买入清单中，避免在后续操作中重复买入
                    if len(context.portfolio.positions) == target_num:     # 如果当前持仓数量已经达到目标数量，停止买入更多的股票
                        break

#4-1 打印每日持仓信息
def print_position_info(context):
    #打印当天成交记录
    trades = get_trades()  # 获取当天所有成交记录
    for _trade in trades.values():  # 遍历成交记录字典的值（即成交记录对象）
        print('成交记录：'+str(_trade))
    #打印账户信息
    for position in list(context.portfolio.positions.values()):  # 遍历账户中所有持仓的股票信息
        securities=position.security  # 获取股票代码
        cost=position.avg_cost        # 获取股票当前持仓成本
        price=position.price          # 获取股票最新价
        ret=100*(price/cost-1)        # 计算股票收益率
        value=position.value          # 获取股票市值
        amount=position.total_amount  # 获取股票持股数量
        print('代码:{}'.format(securities))
        print('成本价:{}'.format(format(cost,'.2f')))
        print('现价:{}'.format(price))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
        print('———————————————————————————————————')
    print('———————————————————————————————————————分割线————————————————————————————————————————')

# 4-2 发送邮件通知
def send_email(sell_list,buy_list):
    # 导入所需的库
    import smtplib 
    from email.mime.text import MIMEText 
    from email.header import Header
    
    sender = 'ch@sina.com'   # 发件人的邮箱地址
    receiver = 'si@sina.com' # 收件人的邮箱地址（注:测试中发送其他邮箱会提示错误） 
    smtpserver = 'smtp.sina.com'  # SMTP服务器地址
    username = 'ch3@sina.com' # 发件人的邮箱用户名
    password = '54e7a4'     # 发件人的邮箱密码
    subject = '量化交易信号'   # 邮件主题
    message = sell_list.to_json()+'\n'+buy_list.to_json  # 邮件内容，将卖出和买入的股票列表转换为JSON格式
    msg = MIMEText(message,'plain','utf-8')  # 创建一个MIMEText邮件对象，指定邮件内容、类型和编码
    msg['Subject'] = Header(subject, 'utf-8') # 创建邮件主题，并指定编码
    msg['from'] = sender # 设置发件人
    # 创建SMTP_SSL对象并连接到SMTP服务器
    smtp = smtplib.SMTP_SSL(smtpserver, 465) 
    try: 
        smtp.login(username, password)  # 登录邮箱
        smtp.sendmail(sender, receiver, msg.as_string())  # 发送邮件
        log.info('邮件发送成功')   # 如果发送成功，记录日志
    except smtplib.SMTPException: 
        # 如果发送失败，记录日志并打印错误信息
        log.info('邮件发送失败')  
        print("Error: 无法发送邮件")
    smtp.quit()    # 关闭SMTP连接
    
def after_code_changed(context):
    # 取消所有定时运行
    unschedule_all()
    # 设置交易运行时间
    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')
    # 为了实盘的时候，买掉股票后可以立马成交，从而可以买入新股票，所以选择9:24卖出，26买入
    run_weekly(weekly_adjustment, 1, time='9:30', reference_security='000300.XSHG')
    run_daily(trade_morning,time='9:26',reference_security='000300.XSHG')
    run_daily(trade_afternoon, time='13:00', reference_security='000300.XSHG') #检查持仓中的涨停股是否需要卖出
    #run_daily(print_position_info, time='15:10', reference_security='000300.XSHG')