#使用“避雷”查找新添加的避雷部分代码

from jqdata import *
import math
import pandas as pd



def initialize(context):
    # 设定基准
    set_benchmark('000905.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 设置滑点为理想情况，不同滑点影响可以在归因分析中查看
    set_slippage(PriceRelatedSlippage(0.00))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5),type='stock')
    # 除非需要精简信息，否则不要过滤日志，方便debug
    #log.set_level('system', 'error')
    #初始化全局变量
    g.stock_num = 50
    g.high_limit_list = []
    g.hold_list = []
    g.weights = [1.0, 1.0, 1.6, 0.8, 2.0]
    g.black_list = [] #避雷
    # 设置交易时间，每天运行
    run_daily(prepare_stock_list, '9:05')
    run_daily(get_black_list, '9:05') #避雷
    run_weekly(adjust_position, 1, '09:30')
    run_daily(check_limit_up, '14:00')
    #run_daily(print_position_info, '15:10')



#1-1 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    #获取昨日涨停列表
    if g.hold_list != []:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit'], count=1, panel=False, fill_paused=False) #原
        df = df[df['close'] == df['high_limit']]
        g.high_limit_list = list(df.code)
    else:
        g.high_limit_list = []    

#1-2 获取黑名单 #避雷
def get_black_list(context):
    
    #查看当前日期是否处于某段时间内
    def today_is_between(context, start_date, end_date):
        today = context.current_dt.strftime('%m-%d')
        if (start_date <= today) and (today <= end_date):
            return True
        else:
            return False
    
    #计算季度
    def get_fiscal_quarters(start_date):
        md_lst = ['-03-31','-06-30','-09-30','-12-31']
        y3 = str(start_date[:4])
        y2 = str(int(y3) - 1)
        y1 = str(int(y2) - 1)
        y1_lst, y2_lst, y3_lst = [], [], []
        for i in range(4):
            y1_lst.append(y1 + md_lst[i])
            y2_lst.append(y2 + md_lst[i])
            y3_lst.append(y3 + md_lst[i])
        fq_date_lst = [y1_lst, y2_lst, y3_lst]
        return fq_date_lst
    
    #初次预测风险列表
    def predict_st_stocks(stock_list, stat_date, fqd):
        tmp = []
        k1 = 'net_profit' #净利润 
        k2 = 'adjusted_profit' #扣非净利润    
        for stock in stock_list:
            try:
                df = get_history_fundamentals(stock, fields=[income.net_profit, indicator.adjusted_profit], watch_date=stat_date, count=11, interval='1q') #作
                df = df.set_index('statDate')
                #距离观察日(ed)最近一个还未披露的季度用前一年同期替代
                #由于get_history_fundamentals返回数据可能缺失，所以不要用iloc定位，会“串行”。
                y1 = df.loc[fqd[0][0]][k1] + df.loc[fqd[0][1]][k1] + df.loc[fqd[0][2]][k1] + df.loc[fqd[0][3]][k1]
                y1a = df.loc[fqd[0][0]][k2] + df.loc[fqd[0][1]][k2] + df.loc[fqd[0][2]][k2] + df.loc[fqd[0][3]][k2]
                y2 = df.loc[fqd[1][0]][k1] + df.loc[fqd[1][1]][k1] + df.loc[fqd[1][2]][k1] + df.loc[fqd[1][3]][k1]
                y2a = df.loc[fqd[1][0]][k2] + df.loc[fqd[1][1]][k2] + df.loc[fqd[1][2]][k2] + df.loc[fqd[1][3]][k2]
                y3 = df.loc[fqd[2][0]][k1] + df.loc[fqd[2][1]][k1] + df.loc[fqd[2][2]][k1] + df.loc[fqd[1][3]][k1]
                y3a = df.loc[fqd[2][0]][k2] + df.loc[fqd[2][1]][k2] + df.loc[fqd[2][2]][k2] + df.loc[fqd[1][3]][k2] 
                if (min(y1,y1a)<0) and (min(y2,y2a)<0) and (min(y3, y3a)<0):
                    tmp.append(stock)
            except:
                #如不符合上述数据结构，说明上市公司可能未按时披露信息，或上市不足3年
                pass
        return tmp
    
    #确定最近一个sd
    if today_is_between(context, '11-01', '12-31'):
        sd = context.current_dt.strftime('%Y-%m-%d')[:4] + '-11-01'
    elif today_is_between(context, '01-01', '05-01'):
        sd = str(int(context.current_dt.strftime('%Y-%m-%d')[:4])-1) + '-11-01'
    else:
        sd = 0
        #5至11月为报告真空期，不需要过滤，重置黑名单到初始状态
        g.black_list = []
    
    #计算首次预测黑名单(只计算一次)
    if (len(g.black_list) == 0) and (sd != 0):
        df = get_all_securities(types=['stock'], date=sd)
        stock_list = list(df.index)
        #由于风险警示制度与主板不同，这里过滤掉了科创板跟北交所股票(聚宽目前也没有北交所数据)
        stock_list = filter_kcbj_stock(stock_list)
        #此项过滤主要是预防被st，所以只保留在循环起始日之前正常的股票
        stock_list = filter_st_stock(stock_list)
        #上市3年以内一般不会因为连续亏损退市，所以过滤掉上市不足500个交易日的股票
        stock_list = filter_new_stock(context, stock_list, 500)
        #获取需要查询的日期列表
        fiscal_quarter_date_list = get_fiscal_quarters(sd)
        #预测当前非st但是有可能变st的股票，此列表为初次预测，之后需要随着时间推进更新
        predict_list0 = predict_st_stocks(stock_list, sd, fiscal_quarter_date_list)
        g.black_list = predict_list0
    
    #日常循环检查是否发布至少扭亏为盈的业绩预告，如果有，说明最新年度扣非前后最小净利润已经大于零，一般不会被st，可以在年报发布前提前排除出风险名单。
    #这段代码收益提升不明显，可以注释掉以提升策略运行效率
    if (len(g.black_list) != 0) and (sd != 0):
        ed = str(context.previous_date)
        predict_list1 = g.black_list.copy()
        for stock in predict_list1[:]:        
            df = finance.run_query(query(finance.STK_FIN_FORCAST).filter(finance.STK_FIN_FORCAST.code==stock))
            df = df[(df['report_type'] == '四季度预告') & (df['type_id'] <= 305004) & (df['pub_date'] < datetime.date(*map(int,ed.split('-'))))] #者
            if len(df) > 0:
                if str(df.iloc[-1,:]['end_date'])[2:4] == str(sd)[2:4]:
                    print('预增预盈或扭亏为盈', stock)
                    #在一月会产生一批预盈的股票
                    predict_list1.remove(stock)
            #每天查询更新最近一期四季报
            df = get_history_fundamentals(stock, fields=[income.net_profit, indicator.adjusted_profit], watch_date=ed, count=4, interval='1q')
            df = df.set_index('statDate')
            k1 = 'net_profit' #净利润 
            k2 = 'adjusted_profit' #扣非净利润
            fqd = get_fiscal_quarters(sd)
            try:            
                #这里与11月1日预判不同的是第四项，这里是每天查看如果有公司发布了年报，第四项就不要用估算值了
                y3 = df.loc[fqd[2][0]][k1] + df.loc[fqd[2][1]][k1] + df.loc[fqd[2][2]][k1] + df.loc[fqd[2][3]][k1]
                y3a = df.loc[fqd[2][0]][k2] + df.loc[fqd[2][1]][k2] + df.loc[fqd[2][2]][k2] + df.loc[fqd[2][3]][k2]
                if min(y3, y3a) > 0:
                    print('年报已出最近一年盈利', stock)
                    predict_list1.remove(stock)
            except:
                pass
        #最后输出的是，去除预盈和已经公布财报盈利后，仍然有被st风险的股票
        g.black_list = predict_list1

    
#1-3 选股模块
def get_stock_list(context):
    
    # 获取前N个单位时间当时的收盘价
    def get_close(stock, n, unit):
        return attribute_history(stock, n, unit, 'close')['close'][0]
    
    # 获取现价相对N个单位前价格的涨幅
    def get_return(stock, n, unit):
        price_before = attribute_history(stock, n, unit, 'close')['close'][0]
        price_now = get_close(stock, 1, '1m')
        if not isnan(price_now) and not isnan(price_before) and price_before != 0:
            return price_now / price_before
        else:
            return 100
    
    # 获得初始列表
    yesterday = context.previous_date
    initial_list = get_all_securities('stock', yesterday).index.tolist()
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_new_stock(context, initial_list, 375)
    initial_list = filter_st_stock(initial_list)
    q = query(
        valuation.code, valuation.market_cap, valuation.circulating_market_cap
    ).filter(
        valuation.code.in_(initial_list),
        indicator.inc_total_revenue_year_on_year > 0, #营业总收入同比增长率
        indicator.inc_net_profit_year_on_year > 0 #净利润同比增长率
    ).order_by(
        valuation.market_cap.asc()).limit(100)
    df = get_fundamentals(q, date=yesterday)
    df.index = df.code
    initial_list = list(df.index)
    
    #获取原始值
    MC, CMC, PN, TV, RE = [], [], [], [], []
    for stock in initial_list:
        #总市值
        mc = df.loc[stock]['market_cap']
        MC.append(mc)
        #流通市值
        cmc = df.loc[stock]['circulating_market_cap']
        CMC.append(cmc)
        #当前价格
        pricenow = get_close(stock, 1, '1m')
        PN.append(pricenow)
        #5日累计成交量
        total_volume_n = attribute_history(stock, 1200, '1m', 'volume')['volume'].sum()
        TV.append(total_volume_n)
        #60日涨幅
        m_days_return = get_return(stock, 60, '1d') 
        RE.append(m_days_return)
    #合并数据
    df = pd.DataFrame(index=initial_list,
        columns=['market_cap','circulating_market_cap','price_now','total_volume_n','m_days_return'])
    df['market_cap'] = MC
    df['circulating_market_cap'] = CMC
    df['price_now'] = PN
    df['total_volume_n'] = TV
    df['m_days_return'] = RE
    df = df.dropna()
    min0, min1, min2, min3, min4 = min(MC), min(CMC), min(PN), min(TV), min(RE)
    #计算合成因子
    temp_list = []
    for i in range(len(list(df.index))):
        score = g.weights[0] * math.log(min0 / df.iloc[i,0]) + g.weights[1] * math.log(min1 / df.iloc[i,1]) + g.weights[2] * math.log(min2 / df.iloc[i,2]) + g.weights[3] * math.log(min3 / df.iloc[i,3]) + g.weights[4] * math.log(min4 / df.iloc[i,4]) #wywy1995
        temp_list.append(score)
    df['score'] = temp_list
    
    #排序并返回最终选股列表
    df = df.sort_values(by='score', ascending=False)
    final_list = list(df.index)
    return final_list


#1-4 整体调整持仓
def adjust_position(context):
    #获取应买入列表
    target_list = get_stock_list(context)
    target_list = filter_paused_stock(target_list)
    target_list = filter_limitup_stock(context, target_list)
    target_list = filter_limitdown_stock(context, target_list)
    #截取不超过最大持仓数的股票量
    target_list = target_list[:min(g.stock_num, len(target_list))]
    #排除可能被st的股票 #避雷
    tmp = target_list
    target_list = [stock for stock in target_list if stock not in g.black_list]
    if len(target_list) < len(tmp):
        print('存在财务风险的股票', list(set(tmp)-set(target_list)))
    #调仓卖出
    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):
            log.info("卖出[%s]" % (stock))
            position = context.portfolio.positions[stock]
            close_position(position)
        else:
            log.info("已持有[%s]" % (stock))
    #调仓买入
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    if target_num > position_count:
        value = context.portfolio.cash / (target_num - position_count)
        for stock in target_list:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == target_num:
                        break

#1-5 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt
    if g.high_limit_list != []:
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.high_limit_list:
            current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
            if current_data.iloc[0,0] < current_data.iloc[0,1]:
                log.info("[%s]涨停打开，卖出" % (stock))
                position = context.portfolio.positions[stock]
                close_position(position)
            else:
                log.info("[%s]涨停，继续持有" % (stock))


#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
	current_data = get_current_data()
	return [stock for stock in stock_list if not current_data[stock].paused]

#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
	current_data = get_current_data()
	return [stock for stock in stock_list
			if not current_data[stock].is_st
			and 'ST' not in current_data[stock].name
			and '*' not in current_data[stock].name
			and '退' not in current_data[stock].name]

#2-3 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)
	current_data = get_current_data()
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] < current_data[stock].high_limit]

#2-4 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list)
	current_data = get_current_data()
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
			or last_prices[stock][-1] > current_data[stock].low_limit]

#2-5 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':
            stock_list.remove(stock)
    return stock_list

#2-6 过滤次新股
def filter_new_stock(context, stock_list, d):
    yesterday = context.previous_date
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < datetime.timedelta(days=d)]



#3-1 交易模块-自定义下单
def order_target_value_(security, value):
	if value == 0:
		log.debug("Selling out %s" % (security))
	else:
		log.debug("Order %s to value %f" % (security, value))
	return order_target_value(security, value)

#3-2 交易模块-开仓
def open_position(security, value):
	order = order_target_value_(security, value)
	if order != None and order.filled > 0:
		return True
	return False

#3-3 交易模块-平仓
def close_position(position):
	security = position.security
	order = order_target_value_(security, 0)  # 可能会因停牌失败
	if order != None:
		if order.status == OrderStatus.held and order.filled == order.amount:
			return True
	return False



#4-1 打印每日持仓信息
def print_position_info(context):
    #打印当天成交记录
    trades = get_trades()
    for _trade in trades.values():
        print('成交记录：'+str(_trade))
    #打印账户信息
    for position in list(context.portfolio.positions.values()):
        securities=position.security
        cost=position.avg_cost
        price=position.price
        ret=100*(price/cost-1)
        value=position.value
        amount=position.total_amount    
        print('代码:{}'.format(securities))
        print('成本价:{}'.format(format(cost,'.2f')))
        print('现价:{}'.format(price))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
        print('———————————————————————————————————')
    print('———————————————————————————————————————分割线————————————————————————————————————————')