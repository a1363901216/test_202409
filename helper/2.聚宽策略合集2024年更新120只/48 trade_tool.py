import os
import pickle
import atexit
import inspect
import math
from jqtrade.account.api import *
from jqtrade.common import log as logging
import numpy as np

orderlog = logging.logging.getLogger("UserOrder")
statelog = logging.logging.getLogger("UserState")


class GlobalState:
    """自定义的全局变量对象,进程结束时pickle到指定路径, 初始化时会尝试从路径恢复, 路径不存在则创建新的环境变量, 
    这里应该储存的是可序列化的变量,否则序列化会失败
    注意只有在进程正常结束时才可以保存, 异常退出或者主动停止是无法保存的!!!!!!!!!!!!!!!
    建议添加多个定时运行调用save_state(或者对耗时不敏感或者变量较小时, 在每个定时运行函数下都调用一次 save_state 方法)
    也可以实现一个装饰器/全局异常处理钩子来在异常产生时调用save_state"""

    def __init__(self, filepath):
        self.__dict__['_filepath'] = filepath
        self.load_state()
        atexit.register(self.save_state)

    def load_state(self):
        if os.path.exists(self._filepath):
            with open(self._filepath, 'rb') as file:
                try:
                    state = pickle.load(file)
                    self.__dict__.update(state)
                    statelog.info(f"已从 {self._filepath} 恢复全局变量对象")
                except Exception as e:
                    raise ValueError(f"无法加载全局变量对象: {e}")
        else:
            statelog.info(f"{self._filepath} 路径不存在, 创建新的全局变量对象")

    def save_state(self):
        with open(self._filepath, 'wb') as file:
            state_to_save = {k: v for k, v in self.__dict__.items() if k != '_filepath'}
            pickle.dump(state_to_save, file)

    def delete_global(self, global_names):
        """删除已存在的变量"""
        if isinstance(global_names,str):
            global_names = [global_names]
        for global_name in global_names :
            if global_name in self.__dict__:
                del self.__dict__[global_name]
            else:
                statelog.error(f"变量 {global_name} 不存在，无法删除。")
        # raise AttributeError(f"变量 {global_name} 不存在，无法删除。")

    def __setattr__(self, key, value):
        if key in self.__dict__:
            # Check if we are inside process_initialize
            stack = inspect.stack()[:10]  # 最多查10层
            for frame_info in stack:
                if frame_info.function == 'process_initialize':
                    statelog.warning(f"变量 g.{key} 已存在，process_initialize中重新赋值将不再生效!(如有更改需求可调用delete_global删除后重新赋值或在其他函数中赋值)")
                    return
        self.__dict__[key] = value


class RatioLimitOrderStyle(LimitOrderStyle):
    """按照价格比例/跳数进行委托
	(使用order_target/order_target_value时无法确认委托方向,需要委托价在一定限度内)"""

    def __init__(self, now_price, ratio=0.015, step=10, high_limit=np.inf, low_limit=-np.inf):
        """价格笼子 : 委托价不得偏离当前价的2%(ratio),且不得偏离当前价的 +- 10个跳点(step)
		ratio 和 step 计算出的价格 , 取偏离now_price更小的那个值
		now_price : 当前价格
		ratio : 价格比例, 买入时委托价为 now_price * (1-ratio) ,卖出时为  now_price * (1+ratio) 
		step : 条数比例
		"""
        self.now_price = now_price
        self.ratio = ratio
        self.step = step
        self.high_limit = high_limit
        self.low_limit = low_limit
        super(RatioLimitOrderStyle, self).__init__(price=None)

    def set_price(self, side, code):
        if code[0] in ['0', '3', '6']:
            price_round = 2
        else:
            price_round = 3
        step_change = self.step * 10 ** (-price_round)

        if self.now_price == 0:  # 为0时代表对手盘,不做修改
            self._price = 0

        if side == 'close':
            ratio = 1 - self.ratio
            p1 = round(self.now_price * ratio, price_round)
            p2 = round(self.now_price - step_change, price_round)
            # print(p1,p2)
            price = p1 if abs(p1 - self.now_price) < abs(p2 - self.now_price) else p2  # 取限制更严格的那一个(偏离当前价格更小的)
            self._price = self.check_limit(price, price_round)
        elif side == 'open':
            ratio = 1 + self.ratio
            p1 = round(self.now_price * ratio, price_round)
            p2 = round(self.now_price + step_change, price_round)
            # print(p1,p2)
            price = p1 if abs(p1 - self.now_price) < abs(p2 - self.now_price) else p2  # 取限制更严格的那一个(偏离当前价格更小的)
            self._price = self.check_limit(price, price_round)
        else:
            raise ValueError("未知的买卖方向 : {} ".format(side))

    def check_limit(self, price, price_round):
        """将委托价限制在最高价/最低价(一般为涨跌停价)限制之间"""
        high_limit = round(self.high_limit, price_round)
        price = min(price, high_limit)
        low_limit = round(self.low_limit, price_round)
        price = max(price, low_limit)
        return price

    def __str__(self):
        return f"RatioLimitOrderStyle(now_price={self.now_price}, ratio={self.ratio}, step={self.step})"


def handle_order(code, amount, style=None, msg=[]):
    """将amount转为整手下单"""
    if code.startswith("68"):
        if abs(amount) < 200:
            new_amount = 0
        else:
            new_amount = amount
    elif amount < 0:
        new_amount = -(abs(amount) // 100) * 100
    else:
        new_amount = (amount // 100) * 100
    new_amount = int(new_amount)
    if new_amount == 0:
        msg += [f"最终下单数量为0 ,订单未委托: {code}  ,{amount} ,  {style}"]
        orderlog.error('   '.join(msg))
        return
    msg += ["执行下单"]
    orderlog.info('   '.join(msg))
    return order(code, new_amount, style)


def safe_order(code, amount, style=None, context=None, msg=[]):
    """根据可用资金及可平仓股数调整下单"""
    if context is None:
        raise ValueError("必须传递context对象")
    if not msg:
        msg = [f"order_target({code},{amount},{style})"]

    if amount > 0:  # 买,检查资金
        if style.__class__.__name__ == "RatioLimitOrderStyle":
            style.set_price("open", code)
        available_cash = context.portfolio.available_cash
        if available_cash <= 0:
            msg += [f"可用资金不足 , 订单未委托 : {code}  , {amount} , {style}"]
            orderlog.waring('\n'.join(msg))
            return
        available_amount = available_cash // style.price
        if available_amount < amount:
            new_amount = available_amount
            msg += [f"可用资金不足 , 数量 {amount} 调整至 {new_amount} , {code}  , {style}"]
        else:
            new_amount = int(amount)
        return handle_order(code, new_amount, style, msg)

    else:
        if style.__class__.__name__ == "RatioLimitOrderStyle":
            style.set_price("close", code)

        if code in context.portfolio.long_positions:
            closeable_amount = context.portfolio.long_positions[code].closeable_amount
        else:
            closeable_amount = 0

        if closeable_amount == 0:
            msg += [f"可平仓数为0 , 订单未委托 :{code}  , {amount},  {style}"]
            orderlog.error('   '.join(msg))
            return
        if abs(amount) > closeable_amount:
            msg += [f"可平仓数不足 , 数量 {amount} 调整至 {-closeable_amount} , {code}  , {style}    执行下单"]
            orderlog.warning('   '.join(msg))
            return order(code, -int(closeable_amount), style)
        elif abs(amount) == context.portfolio.long_positions[code].total_amount:  # 全平
            orderlog.warning('   '.join(msg))
            return order(code, int(amount), style)
        else:
            return handle_order(code, amount, style, msg)


def order_value(code, value, style=None, context=None, now_price=None, msg=[]):
    """按目标价值下单,计算仓位时需要用到当前价, 如果传递now_price则直接使用now_price计算, 否则尝试从RatioLimitOrderStyle获取 """
    now_price = get_price_info(now_price, style)

    if not msg:
        msg = [f"order_value({code},{value},{style})"]

    return safe_order(code, value // now_price, style, context=context, msg=msg)


def order_target(code, amount, style=None, context=None):
    """按目标股数下单"""
    if context is None:
        raise ValueError("必须传递context对象")
    if code in context.portfolio.long_positions.keys():
        now_amount = context.portfolio.long_positions[code].total_amount
    else:
        now_amount = 0

    ex_ordes = get_orders(code=code)
    for oid, o in ex_ordes.items():
        if o.status in ('new', 'open', 'filling'):
            orderlog.error(f"您调用了 order_target({code}, {amount} ,{style} ) 函数，之前未完成的订单{o.order_id}将进行撤单")
            cancel_order(oid)
    change_amount = amount - now_amount

    msg = [f"order_target({code},{amount},{style})"]
    return safe_order(code, change_amount, style, context=context, msg=msg)


def order_target_value(code, value, style=None, context=None, now_price=None):
    """按目标价值下单,计算仓位时需要用到当前价, 如果传递now_price则直接使用now_price计算, 否则尝试从RatioLimitOrderStyle获取 """
    if context is None:
        raise ValueError("必须传递context对象")

    now_price = get_price_info(now_price, style)

    if code in context.portfolio.long_positions.keys():
        now_value = context.portfolio.long_positions[code].total_amount * now_price
    else:
        now_value = 0

    ex_ordes = get_orders(code=code)
    for oid, o in ex_ordes.items():
        if o.status in ('new', 'open', 'filling'):
            orderlog.error(f"您调用了 order_target_value({code}, {value} ,{style} ) 函数，之前未完成的订单{oid}将进行撤单")
            cancel_order(oid)
    msg = [f"order_target_value({code},{value},{style})"]
    return order_value(code, value - now_value, style, context, now_price, msg)


def get_price_info(now_price, style):
    """从订单信息中获取价格信息 ,如果传递now_price则直接使用now_price计算, 否则尝试从RatioLimitOrderStyle获取"""
    if now_price:  # 有传递价格信息时直接返回
        return now_price
    now_price = getattr(style, "now_price", None)  # 从RatioLimitOrderStyle获取
    if now_price:
        return now_price
    ####  这里也可以直接加上从jqdatasdk获取当前价的逻辑
    # now_price = getattr(style ,"price",None) #从LimitOrderStyle等获取委托价
    # if now_price :
    # 	return now_price
    else:
        raise ValueError("当调用order_target_value/order_value时,如style未使用RatioLimitOrderStyle指定当前价时需指定now_price参数")
