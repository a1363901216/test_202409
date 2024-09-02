import numpy as np
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, PanTool, ZoomInTool, ResetTool, ZoomOutTool, WheelZoomTool, BoxSelectTool, \
    HoverTool, NumeralTickFormatter, FixedTicker, AdaptiveTicker, CategoricalTicker, CustomJSHover


def generate_tick_positions(data_length):
    if data_length <= 10:
        return list(range(data_length))  # 如果数据点少于等于10，则每个都标记
    else:
        # 简化处理：均匀选取10个位置作为标签，实际场景可能需要更智能的选取策略
        interval = data_length // 10
        return list(range(0, data_length, interval))[:10]

def normalization(data):
    init_value = data['y'][0]
    np_array = np.array(data["y"])
    data["y"] = ((np_array - init_value) * 100.0 /init_value).tolist()
    return data
def do_plot(data, shangzheng):
    data['x'] = [str(item) for item in data['x']]
    data = normalization(data)
    data['y_f'] = ['{:.3f}'.format(val) for val in data['y']]  # 控制小数点后有6位

    shangzheng_dict={}
    shangzheng_dict['x'] = [row['trade_date']  for index, row in shangzheng.iterrows()]
    shangzheng_dict['y'] = [row['close_qfq']  for index, row in shangzheng.iterrows()]
    shangzheng_dict = normalization(shangzheng_dict)
    shangzheng_dict['y_f'] = ['{:.3f}'.format(val) for val in shangzheng_dict['y']]

    source = ColumnDataSource(data=dict(x=data['x'], y1=data['y'], y_f1=data['y_f'], y2=shangzheng_dict['y'], y_f2=shangzheng_dict['y_f']))

    # 创建Figure对象
    # p = figure(x_axis_label='x', y_axis_label='y', title='Stock Price Over Time', tools='', width=1400)
    p = figure(x_range=data['x'], y_axis_label='y', title='Stock Price Over Time', tools='', width=1400)
    p.line('x', 'y1', source=source, color="red", legend_label='收益', line_width=2)

    p.line('x', 'y2', source=source, color="blue", legend_label='上证', line_width=2)
    # p.scatter('Date', 'Close', size=5, source=source2, fill_color="green", line_color=None)

    # 添加交互式工具
    wheel_zoom_tool = WheelZoomTool()
    hover = HoverTool(
        tooltips=[
            # ("索引", "$index"),
            ("x", "@x"),
            # 直接使用已经格式化的y值
            ("收益", "@y_f1 %"),
            ("上证", "@y_f2 %"),
        ],
        mode='vline'
    )

    p.add_tools(PanTool(), ZoomInTool(), ZoomOutTool(), wheel_zoom_tool, BoxSelectTool(),ResetTool(), hover)
    # p.x_range.start = df['x'].min()
    # p.x_range.end = df['x'].max()

    # p.xaxis.ticker = FixedTicker(ticks=data['x'])

    p.yaxis.formatter = NumeralTickFormatter(format='0')
    # p.xaxis.formatter = NumeralTickFormatter(format='0')
    p.toolbar.active_scroll = wheel_zoom_tool

    # tick_positions = generate_tick_positions(len(data['x']))
    # p.xaxis.ticker = tick_positions

    # 显示图形
    show(p)


if __name__ == '__main__':
    # 假设我们有如下股票价格数据（日期和收盘价）
    size = 1800
    data = {
        'Date': pd.date_range(start='2017-01-01', periods=size),
        'Close': [round(100 + 20 * (i / size) + 5 * np.random.rand(), 2) for i in range(size)]
    }
    df = pd.DataFrame(data)

    # 将数据转换为ColumnDataSource，这是Bokeh用于数据绑定的对象
    source = ColumnDataSource(df)

    data2 = {
        'Date': pd.date_range(start='2017-01-01', periods=size),
        'Close': [round(100 + 10 * (i / size) + 1 * np.random.rand(), 2) for i in range(size)]
    }
    df2 = pd.DataFrame(data2)

    # 将数据转换为ColumnDataSource，这是Bokeh用于数据绑定的对象
    source2 = ColumnDataSource(df2)

    # 创建Figure对象
    # p = figure(x_axis_type='datetime', title='Stock Price Over Time', tools='', width=1400)
    p = figure(x_axis_label='Date', y_axis_label='Close', title='Stock Price Over Time', tools='', width=1400)

    # 添加股票价格折线
    p.line('Date', 'Close', source=source, color="red", legend_label='Close Price', line_width=2)
    p.line('Date', 'Close', source=source2, color="blue", legend_label='Close Price', line_width=2)

    p.scatter('Date', 'Close', size=5, source=source2, fill_color="green", line_color=None)

    # 添加交互式工具
    p.add_tools(PanTool(), ZoomInTool(), ZoomOutTool(), WheelZoomTool(), ResetTool(), HoverTool())

    # 设置x轴的范围以允许拖动查看不同时间段
    p.x_range.start = df['Date'].min()
    p.x_range.end = df['Date'].max()

    # 显示图形
    show(p)
