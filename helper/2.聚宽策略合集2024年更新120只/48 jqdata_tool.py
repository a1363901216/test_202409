import datetime
import pandas as pd
from jqdatasdk import *
# 需要先登陆jqdatasdk

def history(count, unit='1d', field='avg', security_list=None, df=True, skip_paused=False, fq='pre'):
    """获取多个标的单个数据字段,如果有查询多个字段的场景建议直接用get_price批量获取更快"""
    data = get_price(security_list, end_date=datetime.datetime.now() ,  panel=False , count=count,frequency =unit,fq=fq ,skip_paused=skip_paused ,fields=field )
    data = data.pivot_table(columns=['code'],index=['time'],values=field)
    if df is False :
        return {k:v.values for k,v in data.to_dict('series').items()}
    return data

def attribute_history(security, count, unit='1d',fields=['open', 'close', 'high', 'low', 'volume', 'money'],
                      skip_paused=True, df=True, fq='pre'):
    """获取单个标的多个数据字段,如果有查询多个标的的场景建议直接用get_price批量获取更快"""
    data = get_price(security, end_date=datetime.datetime.now() ,  panel=False , count=count,frequency =unit,fq=fq ,skip_paused=skip_paused ,fields=fields )
    if df is False:
        return {k:v.values for k,v in data.to_dict('series').items()}
    return data

def get_current_data():
    """仿照聚宽官网的get_current_data,使用时建议优化一下策略代码, 尽可能批量查询而不是轮询 ,因为本地运行时分多次从服务器拿数据会严重影响运行效率
    为了提升获取速度,支持给CurrentData对象传递多个标的来一次性获取多个标的数据,当传递为多个标的时返回Series ,index为标的代码
    如 get_current_data()['000001.XSHE','000002.XSHE'].industry_code 或 get_current_data()[['000001.XSHE','000002.XSHE']].industry_code
    返回series : 
    000001.XSHE    J66
    000002.XSHE    K70

    过滤st可以这样写 : 
    st_data = current[stocks].is_st
    filtered_stocls = st_data[st_data!=True].index.tolist()
    """
    return CurrentData()

class CurrentData:
    def __init__(self):
        self.store = {}
    def __getitem__(self, codes):
        if isinstance(codes,(list,tuple)):
            codekey = ','.join(codes)
        elif isinstance(codes,str):
            codekey = codes
        else : 
            raise ValueError("错误的标的类型(必须是一个或多个标的，或者多个标的组成的字符串")
        if codekey not in self.store:
            self.store[codekey] = self._Entry(codes)
        return self.store[codekey]
    
    class _Entry:
        def __init__(self,codes):
            if isinstance(codes,str):
                self.codes = [codes]
                self.is_one_stocks = True
            else :
                self.codes = codes
                self.is_one_stocks = False
            
            self._last_price = None
            self._high_limit = None
            self._low_limit = None
            self._paused = None
            self._is_st = None
            self._day_open = None
            self._name = None
            self._industry_code = None
            
        def get_minute_fileds(self):
            now = datetime.datetime.now()
            if now.time() < datetime.time(9,31):
                frequency = '1d'
            else :
                frequency  ='1m'

            data = get_price(self.codes, end_date=now ,  panel=False , 
                             count=1,frequency =frequency,fq='none' ,fields=['close','high_limit','low_limit','paused'] ).set_index("code")
            if self.is_one_stocks :
                self._last_price = data.close[0]
                self._high_limit = data.high_limit[0]
                self._low_limit = data.low_limit[0]
                self._paused = data.paused.fillna(1)[0] ==1
            else :
                self._last_price = data.close
                self._high_limit = data.high_limit
                self._low_limit = data.low_limit
                self._paused = data.paused.fillna(1)==1
                
        def get_daily_fields(self):
            data = get_price(self.codes, end_date=datetime.datetime.now() ,  panel=False , 
                             count=1,frequency ='1d',fq='none' ,fields=['high_limit','low_limit','paused','open'] ).set_index("code")
            if self.is_one_stocks :
                self._day_open = data.open[0]
                self._high_limit = data.high_limit[0]
                self._low_limit = data.low_limit[0]
                self._paused = data.paused.fillna(1)[0] ==1
            else :
                self._day_open = data.open
                self._high_limit = data.high_limit
                self._low_limit = data.low_limit
                self._paused = data.paused.fillna(1)==1
        
        def get_st_data(self):
            data = get_extras('is_st', self.codes, end_date = datetime.datetime.now() ,count =1).iloc[0]
            if self.is_one_stocks :
                self._is_st = data.iloc[0]
            else :
                self._is_st = data
        
        def get_industry_code(self):
            data = get_industry(self.codes)
            data = pd.Series({k:v.get("zjw",{}).get('industry_code',None) for k,v in data.items()})
            if self.is_one_stocks :
                self._industry_code = data.iloc[0]
            else :
                self._industry_code = data
        @property
        def last_price(self):
            self.get_minute_fileds()
            return self._last_price
        
        @property
        def day_open(self):
            if self._day_open is None :
                self.get_daily_fields()
            return self._day_open
        
        @property
        def high_limit(self):
            if self._high_limit is None :
                self.get_daily_fields()
            return self._high_limit
        
        @property
        def low_limit(self):
            if self._low_limit is None :
                self.get_daily_fields()
            return self._low_limit
        
        @property
        def paused(self):
            if self._paused is None :
                self.get_daily_fields()
            return self._paused
        
        @property
        def name(self):
            data = get_all_securities().loc[self.codes].display_name
            if self.is_one_stocks:
                return data.iloc[0]
            else :
                return data
        @property
        def is_st(self):
            if self._is_st is None :
                self.get_st_data()
            return self._is_st
        
        @property
        def industry_code(self):
            if self._industry_code is None :
                self.get_industry_code()
            return self._industry_code