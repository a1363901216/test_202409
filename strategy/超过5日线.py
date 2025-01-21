# coding:utf-8

# from backtrader_bokeh import bt


# import talib




# 最近3天都在5日线上，5日线最近10天超过至少两条其他均线
def do_get_signal(nums_df, open, close, emaList):
    if len(close) < 10:
        return 1

    state_enum = ['can_buy', 'has_buy']
    state = 0
    total_money = 1
    buy_price = 0
    ema5 = emaList[0]
    ema10 = emaList[1]
    ema20 = emaList[2]
    ema30 = emaList[3]
    ema60 = emaList[4]
    ema250 = emaList[5]

    for i in range(10, len(close)):
        if state == 0:
            match_over_ema5 = ((close[i] > ema5[i]) and
                         (close[i - 1] > ema5[i - 1]) and
                         (close[i - 2] > ema5[i - 2]))
            match_cross_ema = 0
            if close[i] > ema10[i] and close[i-10] < ema10[i-10]:
                match_cross_ema = match_cross_ema +1
            if close[i] > ema20[i] and close[i-10] < ema20[i-10]:
                match_cross_ema = match_cross_ema +1
            if close[i] > ema30[i] and close[i-10] < ema30[i-10]:
                match_cross_ema = match_cross_ema +1
            if close[i] > ema60[i] and close[i-10] < ema60[i-10]:
                match_cross_ema = match_cross_ema +1
            if close[i] > ema250[i] and close[i-10] < ema250[i-10]:
                match_cross_ema = match_cross_ema +1

            if match_over_ema5 and match_cross_ema >= 3 :
                state = 1
                buy_price = close[i]
        elif state == 1:
            # match_below_ema5 = ((close[i] < ema5[i]) and
            #                    (close[i - 1] < ema5[i - 1]) and
            #                    (close[i - 2] < ema5[i - 2]))
            match_below_ema5 = False
            profit_over = close[i] > buy_price * 1.50
            profit_blow = close[i] < buy_price * 0.8
            if match_below_ema5 or profit_over or profit_blow:
                total_money = total_money / buy_price * close[i]
                state = 0

    if state == 1:
        total_money = total_money / buy_price * close[len(close)-1]
    return total_money
