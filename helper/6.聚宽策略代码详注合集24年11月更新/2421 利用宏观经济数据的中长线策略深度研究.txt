# 风险及免责提示：该策略由聚宽用户在聚宽社区分享，仅供学习交流使用。
# 原文一般包含策略说明，如有疑问请到原文和作者交流讨论。
# 原文网址：https://www.joinquant.com/post/40154
# 标题：一种宏观数据的中长线策略，年化15%，最大回撤9%
# 作者：Acne Studio
# 回测资金为300000

# 导入函数库
from jqdata import *                 # 从jqdata包中导入所有函数和对象
from jqdata import macro             # 导入jqdata包中的宏观数据模块，这通常包含经济指标、市场数据等宏观经济相关的数据
from decimal import Decimal          # 从decimal模块导入Decimal类，Decimal类用于精确的小数运算，避免浮点数运算中的精度问题
import numpy as np                   # 导入numpy包，这是一个广泛使用的科学计算库，提供了大量的数学函数和操作多维数组的方法
import pandas as pd                  # 导入pandas包，这是一个强大的数据处理和分析工具库，特别适用于处理表格数据
import datetime                      # 导入datetime模块，这是Python的标准库之一，用于处理日期和时间
from scipy import optimize as op     # 从scipy包中的optimize模块导入优化算法，scipy是一个开源的Python算法库和数学工具包

# 定义 initialize 函数，这个函数在策略启动时运行一次，用于初始化策略设置
def initialize(context):
    g.mode = 0      # 设置全局变量 g.mode 为 0，用于指示策略的运行模式，mode:0表示当前只买债券，1表示长线玩法（依据经济周期止盈），2表示中线玩法（ma5、ma20、ma120均线止盈）
    g.record = 0.0  # 设置全局变量 g.record 用于记录下单时沪深300的指数点位
    g.times = -1    # 记录翻倍加仓的次数，-1 代表这个经济周期中第一次购买股票类标的
    g.order_amount = 0.04     # 表示每次下单的基数，即资金的4%
    g.begin_date = '2013-01'  # 设置全局变量 g.begin_date 为 '2013-01'，用于指定获取经济周期数据的起始节点
    g.stock_security = '510300.XSHG'  # 设置全局变量 g.stock_security 为 '510300.XSHG'，代表沪深300ETF，用于股票类投资
    g.bond_security = '511010.XSHG'   # 设置全局变量 g.bond_security 为 '511010.XSHG'，代表国债ETF，用于债券类投资
    g.over_hot = False  # 设置全局变量 g.over_hot 为 False，用于标记市场是否处于过热状态
    g.yearMa10 = 0.0  # 设置全局变量 g.yearMa10 为 0.0，用于记录沪深300跌破10年年K线时的点位
    set_benchmark('000300.XSHG')    # 设定沪深300作为基准
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱，类型是股票
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    run_daily(handle, time = '14:55')    # 使用 run_daily 函数设置每天14:55运行 handle 函数，用于执行日常的交易逻辑

# 定义 handle 函数，用于执行策略的交易逻辑
def handle(context):
    pre_date = context.previous_date      # 获取前一个交易日的日期
    # 获取当前交易日的日期并转换为日期对象
    current_date = context.current_dt.strftime('%Y-%m-%d').split('-')     
    current_date = datetime.date(int(current_date[0]),int(current_date[1]),int(current_date[2]))
    diff_days = (current_date - pre_date).days  # 计算当前交易日和前一交易日之间的天数差，用于判断是否为本周的第一个交易日
    # 1.根据经济周期(使用制造业PMI的分项数据计算)来定投
    if g.mode in [0, 2]:  # 如果模式为0或2，防止误判经济周期，防止放水一波变成了复苏阶段
        current_date = current_date.strftime('%Y-%m')
    # 1.1.判断PMI的值是否符合条件（1是利用拟合函数计算斜率，2是PMI值大于等于50，3是PMI同比上涨）
        x = [0,1,2,3,4,5,6,7,8,9,10,11,12]  # 横坐标，表示月份
        y = get_PMI(current_date)  # 纵坐标，表示PMI值，获取过去十三个月PMI函数的值
        params = op.curve_fit(func,x,y)    # 使用拟合函数 func（需要定义）和x、y值计算PMI的趋势斜率
        k = 2 * params[0][0] * 12 + params[0][1]  # 计算斜率k，用来判断经济趋势
        if k > 0.0 and y[-1] >= 50.0 and y[0] <= y[-1]:     # 如果PMI的趋势是上升的，且当前PMI值大于等于50，且是同比上涨的
    # 1.2.如果PMI满足条件，计算当前供需格局（生产指数与新订单指数的百分位）
            provide_and_need = calc_provide_and_need(g.begin_date, current_date)
    # 1.3.如果供需格局处于复苏状态，则计算当前库存格局（原材料库存指数产成品库存指数的百分位）
            if provide_and_need[0] < 50.0 and provide_and_need[1] > 50.0:       # 如果供需格局显示生产指数在新订单指数之下，表示经济可能在复苏状态
                save = calc_save(g.begin_date, current_date)
    # 1.4.判断当前信息是否有效（供需格局在复苏阶段，库存格局在复苏或者过热阶段）
                if save[0] < 50.0:     # 如果库存格局在复苏或过热状态
                    g.mode = 1     # 切换到长线玩法模式
    # 1.5.如果有效则启动长线玩法，记录成交时的指数点位，且直至卖出前不再执行1.1、1.2、1.3、1.4步骤
    if g.mode == 1:    # 如果当前模式是长线玩法（模式1）
        if g.times == -1:    # 如果这是经济周期中的第一次购买
            # 记录下单时沪深300的指数点位
            g.record = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
        if g.bond_security in context.portfolio.positions.keys(): # 如果国债不在列表中
            order_target(g.bond_security, 0)   # 卖出所有债券
        # 获取当前价格和基金价格
        current_price = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
        fund_price = float(np.array(get_bars(g.stock_security, 1, '1d', fields = ['close'], include_now=True, df = True))[0])
        # 计算与记录时的指数点位的差距
        withdraw = Decimal(str(current_price)) / Decimal(str(g.record)) - Decimal('1.0')
    # 1.6.检查当前价格是否比记录价格低2%，如果是，则买入一笔沪深300ETF（依据1248定投法则）
        if withdraw < Decimal('-0.02') or g.times == -1:     # 如果当前价格比记录价格低2%，或者这是第一次购买
            g.times += 1     # 增加购买次数
            # 调用order_func函数下单购买基金
            order_func(context.portfolio.total_value, context.portfolio.available_cash, fund_price)
            # 更新记录的指数点位
            g.record = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
    # 1.7.指定每周第一个交易日定投一笔沪深300ETF
        else:     # 如果不是第一次购买，且当前是每周的第一个交易日
            if diff_days > 2:
                # 调用order_func函数下单购买基金
                order_func(context.portfolio.total_value, context.portfolio.available_cash, fund_price)
    # 2.根据年K图的移动平均线来定投
    if g.mode in [0, 2]:      # 如果模式是0或2，执行基于年K线的定投策略
        if g.mode == 0:       # 如果当前模式是0
    # 2.1.将月K线图转换成年K线图
            year_point = change_to_yeak_k()       # 2.1.将月K线图转换成年K线图
    # 2.2.计算年K图的移动平均线：ma10
            yearMa10 = year_move_average(year_point)    # 2.2.计算年K图的移动平均线：ma10
    # 2.3.如果跌破年K图的ma10，则记录当前ma10的价格，并且买入一个基数的沪深300ETF，尔后根据1248定投法则定投
            # 获取当前价格和基金价格
            current_price = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
            fund_price = float(np.array(get_bars(g.stock_security, 1, '1d', fields = ['close'], include_now=True, df = True))[0])
            if current_price < yearMa10:    # 如果当前价格跌破年K图的ma10
    # 2.4.启动中线玩法（更改mode的值），直至卖出前不再执行2.1、2.2、2.3、2.4步骤
                g.mode = 2     # 2.4.切换到中线玩法模式
                g.record = yearMa10     # 记录当前的ma10价格
                g.times += 1     # 增加购买次数
                if g.bond_security in context.portfolio.positions.keys():  
                    order_target(g.bond_security, 0)    # 卖出所有债券
                # 调用order_func函数下单购买基金
                order_func(context.portfolio.total_value, context.portfolio.available_cash, fund_price)
    # 2.5.如果当前价格比记录的价格低5%以上，则继续买入一笔沪深300ETF（依据1248定投法则买入）
        if g.mode == 2:    # 如果模式是2，执行中线玩法
            # 获取当前价格和基金价格
            current_price = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
            fund_price = float(np.array(get_bars(g.stock_security, 1, '1d', fields = ['close'], include_now=True, df = True))[0])
            # 计算与记录时的价格的差距
            withdraw = Decimal(str(current_price)) / Decimal(str(g.record)) - Decimal('1.0')
            if withdraw < Decimal('-0.05'):      # 如果当前价格比记录价格低5%以上
                g.times += 1   # 增加购买次数
                # 调用order_func函数下单购买基金
                order_func(context.portfolio.total_value, context.portfolio.available_cash, fund_price)
                # 更新记录的价格
                g.record = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
    # 2.6.指定每周第一个交易日定投一笔沪深300ETF
            else:   # 如果当前是每周的第一个交易日
                if diff_days > 2:
                    # 调用order_func函数下单购买基金
                    order_func(context.portfolio.total_value, context.portfolio.available_cash, fund_price)
    # 4.右侧买入（仅针对1和2）
    if g.mode in [1, 2]:   # 如果模式是1或2，执行右侧买入策略
        # 计算日K图的ma5、ma10、ma20、ma30
        ma5 = day_move_average(5, True)
        ma10 = day_move_average(10, True)
        ma20 = day_move_average(20, True)
        ma30 = day_move_average(30, True)
        ma30_pre = day_move_average(30, False)
    # 4.2. 如果ma5>ma10>ma20>ma30 且 当天的ma30比前一天高
        if ma5 > ma10 > ma20 > ma30 > ma30_pre:
            order_value(g.stock_security, context.portfolio.available_cash)     # 全仓买入股票类标的
    # 5.股票类标的止盈操作
    if g.stock_security in context.portfolio.positions.keys():      # 如果持有股票类标的，执行止盈操作
        sell_stock_security(current_date, context.portfolio.total_value, list(context.portfolio.positions.keys()))
    
# 以下是功能函数

# 获取过去十三个月PMI函数的值
def get_PMI(date:str) -> list:
    start_date = date      # 初始化 start_date 为传入的 date
    for i in range(13):    # 循环13次，每次循环将 start_date 减去一个月，以获取从当前日期起过去13个月的日期范围
        start_date = calc_last_month(start_date)
    # 使用 macro.run_query 执行查询，获取制造业PMI数据，查询条件是统计月份在 start_date 和 date 之间（不包括 date）
    df = macro.run_query(query(
        macro.MAC_MANUFACTURING_PMI.pmi    # 获取 PMI 值
        ).filter(
        macro.MAC_MANUFACTURING_PMI.stat_month >= start_date,  # 过滤条件：统计月份大于等于 start_date
        macro.MAC_MANUFACTURING_PMI.stat_month < date          # 过滤条件：统计月份小于 date
        ).order_by(                                            # 按统计月份升序排序
            macro.MAC_MANUFACTURING_PMI.stat_month.asc()
        ))
    return list(np.array(df['pmi']))    # 将查询结果中的 PMI 值转换为 numpy 数组，然后转换为列表并返回

# 这个函数返回生产指数和新订单指数的百分位
def calc_provide_and_need(start_date:str,end_date:str) -> tuple:
    end_date = calc_last_month(end_date)       # 调用 calc_last_month 函数计算 end_date 上一个月的月份
    # 使用 macro.run_query 执行查询，获取制造业PMI相关的生产指数和新订单指数数据
    df = macro.run_query(query(
        macro.MAC_MANUFACTURING_PMI.stat_month,  # 获取统计月份
        macro.MAC_MANUFACTURING_PMI.produce_idx,  # 获取生产指数
        macro.MAC_MANUFACTURING_PMI.new_orders_idx  # 获取新订单指数
        ).filter(  # 设置查询过滤条件
        macro.MAC_MANUFACTURING_PMI.stat_month >= start_date,  # 统计月份大于等于 start_date
        macro.MAC_MANUFACTURING_PMI.stat_month <= end_date     # 统计月份小于等于 end_date
        ).order_by(                                            # 按统计月份降序排序
        macro.MAC_MANUFACTURING_PMI.stat_month.desc()
        ))
    # 计算生产指数和新订单指数的百分位
    # 从查询结果中提取生产指数和新订单指数的值，转换为 numpy 数组
    produce_seq = np.array(df['produce_idx'])
    new_orders_seq = np.array(df['new_orders_idx'])
    # 分别计算生产指数和新订单指数的百分位
    produce_percent = percentile(produce_seq, df.iloc[0, 1])
    new_orders_percent = percentile(new_orders_seq, df.iloc[0, 2])
    # 返回一个包含生产指数和新订单指数百分位的元组
    return (produce_percent, new_orders_percent)

# 这个函数返回原材料和产成品库存指数的百分位
def calc_save(start_date:str,end_date:str) -> tuple:   # 它接收两个参数：start_date 和 end_date，这两个参数都是字符串类型，表示日期范围。
    end_date = calc_last_month(end_date)    # 调用 calc_last_month 函数计算 end_date 上一个月的月份，并将结果转换为字符串格式
    # 使用 macro.run_query 执行查询，获取制造业PMI相关的原材料库存指数和产成品库存指数数据
    df = macro.run_query(query(
        macro.MAC_MANUFACTURING_PMI.stat_month,           # 获取统计月份
        macro.MAC_MANUFACTURING_PMI.raw_material_idx,     # 获取原材料库存指数
        macro.MAC_MANUFACTURING_PMI.finished_produce_idx  # 获取产成品库存指数
        ).filter(  # 设置查询过滤条件
        macro.MAC_MANUFACTURING_PMI.stat_month >= start_date,  # 统计月份大于等于 start_date
        macro.MAC_MANUFACTURING_PMI.stat_month <= end_date     # 统计月份小于等于 end_date
        ).order_by(  # 按统计月份降序排序
        macro.MAC_MANUFACTURING_PMI.stat_month.desc()
        ))
    # 从查询结果中提取原材料库存指数和产成品库存指数的值，并将它们转换为 numpy 数组
    raw_material_seq = np.array(df['raw_material_idx'])
    finished_produce_seq = np.array(df['finished_produce_idx'])
    # 分别计算原材料库存指数和产成品库存指数的百分位
    # 这里假设 percentile 函数已经定义，它接受一个数组和一个索引位置，返回对应位置的值作为百分位
    raw_material_percent = percentile(raw_material_seq, df.iloc[0, 1])
    finished_produce_percent = percentile(finished_produce_seq, df.iloc[0, 2])
    # 返回一个包含原材料库存指数和产成品库存指数百分位的元组
    return (raw_material_percent, finished_produce_percent)

# 下单函数，total_cash表示总权益，available_cash表示可用资金，current_price表示当前价格
def order_func(total_cash:float, available_cash:float,fund_price:float):
    buy_cash = 2 ** g.times * g.order_amount * total_cash    # 计算买入金额，基于总权益、加仓次数和下单基数
    if buy_cash <= available_cash:      # 如果计算出的买入金额不超过可用资金，则按照计算金额下单买入
        order_value(g.stock_security, buy_cash)  
    else:      # 如果计算出的买入金额超过可用资金，则使用全部可用资金下单买入
        if fund_price * 100 <= available_cash:
            order_value(g.stock_security, available_cash)

# 卖出股票类标的止盈
def sell_stock_security(current_date, total_cash:float, stock_in_hand:list):
    if isinstance(current_date, datetime.date):       # 确保当前日期格式正确
        current_date = current_date.strftime('%Y-%m')
    # 1.如果mode==1（长线玩法）
    if g.mode == 1:    # 如果策略模式为长线玩法
    # 1.1.计算当前供需格局及其得分(1/3*供 + 2/3*需)
        provide_and_need = calc_provide_and_need(g.begin_date, current_date)
        score = 1/3 * provide_and_need[0] + 2/3 * provide_and_need[1]
    # 1.2.如果极度过热（正态分布中均值+2倍标准差，在这里百分位大于97%），则需要计算库存格局
        if score > 97.0 or g.over_hot:       # 如果得分超过97%，认为市场过热
            g.over_hot = True
            save = calc_save(g.begin_date, current_date)
    # 1.3.如果库存格局是滞涨状态，启用中线玩法
            if save[0] > 50.0 and save[1] < 50.0:    # 如果库存格局显示滞涨状态
                g.mode = 2
    # 2.如果mode==2（中线玩法）
    if g.mode == 2:      # 如果策略模式为中线玩法
    # 2.1.计算日K图ma5、ma20、ma120移动平均线
        # 获取当前价格和计算移动平均线
        current_price = float(np.array(get_bars('000300.XSHG', 1, '1d', fields = ['close'], include_now=True, df = True))[0])
        ma5 = day_move_average(5, True)   #当天的ma5
        ma5_pre = day_move_average(5, False)    #前一天的ma5
        ma20 = day_move_average(20, True)       #当天的ma20
        ma20_pre = day_move_average(20, False)    #前一天的ma20
        ma120 = day_move_average(120, True)
    # 2.2.当前价格大于ma120 且 前一天ma5>ma20,当天ma5<ma20，则卖出沪深300ETF，买入债券
        if current_price > ma120 and ma5_pre > ma20_pre and ma5 < ma20:
            order_target(g.stock_security, 0)    # 卖出所有股票类标的
            g.times = -1
            order_value(g.bond_security, total_cash)    # 用全部资金买入债券
            g.mode = 0
            g.over_hot = False

# 将月K线的收盘点位转化为年K的收盘点位
def change_to_yeak_k() -> dict:  
    year_point = dict()      # 初始化一个字典 year_point 来存储过去十年的年K线收盘点位
    # 使用 get_bars 函数获取沪深300指数（000300.XSHG）过去109个月（约9年零1个月）的数据
    # 这里使用‘1M’表示获取月度数据，fields指定获取日期（'date'）和收盘价（'close'）
    df = get_bars('000300.XSHG',109,'1M',fields=['date','close'],include_now=True,df=True)  #取九年一个月来计算每一年的收盘点
    df['date'] = df['date'].apply(lambda x:x.strftime('%Y'))      # 将日期字段（'date'）转换为字符串格式，只保留年份
    last_point = df.iloc[0,1]  # 取得df行索引为0的收盘点位
    last_time = df.iloc[0,0]   # 取得df行索引为0的年份
    current_point = df.iloc[0,1]  # 当前遍历到的收盘点位
    current_time = df.iloc[0,0]   # 当前遍历到的年份
    for i in range(1,df.shape[0]):      # 遍历DataFrame中的每一行，从第二行开始（索引为1）
        # 更新上一个点的数据和时间为当前遍历到的数据和时间
        last_time = current_time
        last_point= current_point
        # 移动到下一行
        current_time = df.iloc[i,0]
        current_point = df.iloc[i,1]
        # 如果当前年份与上一个年份不同，说明跨年了，将上一年的收盘点位存入字典
        if not current_time == last_time:
            year_point[last_time] = last_point
        # 如果是最后一项，确保将当前年份的收盘点位也存入字典
        if i == df.shape[0]-1:
            year_point[current_time] = current_point
    return year_point    # 返回包含过去十年每年收盘点位的字典

# 处理年K图的MA10
def year_move_average(year_point:dict) -> float:
    ten_year_total = Decimal('0.0')    # 初始化十年总和为0
    for point in year_point.values():    # 遍历 year_point 字典中的所有值（即每年的收盘点位）
        ten_year_total += Decimal(str(point))      # 将每个点的值转换为Decimal类型并累加到十年总和
    yearMa10 = float(ten_year_total / Decimal('10.0'))      # 计算十年移动平均值，即十年总和除以10
    return yearMa10    # 返回计算得到的MA10值

# 以下是辅助函数

# 计算指定天数的日移动平均值
def day_move_average(day:int, now:bool) -> float:
    # 获取沪深300指数（000300.XSHG）过去 'day' 天的收盘价，并包括今天的值（如果 now 为 True）
    # 返回的数据是一个 DataFrame，fields 指定获取 'close' 字段
    array = np.array(get_bars('000300.XSHG', day, '1d', fields = ['close'], include_now = now, df = True))
    ma = array.mean()    # 计算获取到的收盘价数组的平均值
    return ma      # 返回计算得到的移动平均值

# 计算序列中目标值的百分位
def percentile(seq, target) -> float:
    not_less_num = len(np.extract(seq>=target,seq))    # 计算序列中大于等于目标值的元素数量
    smaller_num = len(np.extract(seq<target,seq))      # 计算序列中小于目标值的元素数量
    percent = smaller_num * 100 / (not_less_num+smaller_num-1)    # 计算目标值在序列中的百分位位置
    return percent    # 返回计算得到的百分位值

# 计算上一个月月份的字符串，返回YYYY-MM
def calc_last_month(date:str) -> str:
    result = ''    # 初始化结果字符串
    # 解析输入日期字符串，提取年和月
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    # 计算上一个月的月份，考虑跨年的情况
    if month - 1 < 1:
        year -= 1
        month = month + 12 - 1
    else:
        month -= 1
    # 格式化月份，确保月份是两位数字
    if month < 10:
        result = str(year) + '-0' + str(month)
    else:
        result = str(year) + '-' + str(month)
    return result    # 返回计算得到的上一个月的日期字符串

# 拟合数据的二次函数
def func(x, A, B, C):
    return A * x ** 2 + B * x + C    # 根据传入的系数 A, B, C 计算二次函数的值
