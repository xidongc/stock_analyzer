import os
import datetime as dt
import time
from typing import Any, Dict, Optional, List

import requests
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import talib
import multiprocessing as mp
from requests.exceptions import ConnectionError, Timeout


import akshare as ak


stock_info_sh_name_code_df = ak.stock_info_sh_name_code(indicator="主板A股")
stock_info_sh_name_code_df = stock_info_sh_name_code_df[["证券代码", "证券简称", "上市日期"]]
stock_info_sh_name_code_df.columns = ["symbol", "name", "time to market"]

symbols = stock_info_sh_name_code_df.symbol.to_list()

start_date = "20211203"
end_date = "20230202"
adjust = ""
min_klines = 260

ohlc_list = []
for symbol in symbols:
    try:
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date,
                                                adjust=adjust)
        assert len(stock_zh_a_hist_df) != 0

        if stock_zh_a_hist_df is not None and len(stock_zh_a_hist_df) >= min_klines:
            stock_zh_a_hist_df = stock_zh_a_hist_df[["日期", "开盘", "最高", "最低", "收盘", "成交额"]]
            stock_zh_a_hist_df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            stock_zh_a_hist_df['Symbol'] = symbol
            stock_zh_a_hist_df.index.name = "Date"
            stock_zh_a_hist_df.set_index('Date', inplace=True)
            ohlc_list.append(stock_zh_a_hist_df)
        else:
            print(symbol)
    except Exception as e:
        print(e)

ohlc_joined.to_csv("cnstock_daily_ohlc.csv", index=True)

# 上证指数

benchmark = ak.stock_zh_a_daily(symbol="sh000001", start_date=start_date, end_date=end_date)
benchmark.head()

benchmark_ann_ret = benchmark.close.pct_change(252).iloc[-1]


def screen(close: pd.Series, benchmark_ann_ret: float) -> pd.Series:
    """实现MM选股模型的逻辑，评估单只股票是否满足筛选条件

    Args:
        close(pd.Series): 股票收盘价，默认时间序列索引
        benchmark_ann_ret(float): 基准指数1年收益率，用于计算相对强弱
    """

    # 计算50，150，200日均线
    ema_50 = talib.EMA(close, 50).iloc[-1]
    ema_150 = talib.EMA(close, 150).iloc[-1]
    ema_200 = talib.EMA(close, 200).iloc[-1]

    # 200日均线的20日移动平滑，用于判断200日均线是否上升
    ema_200_smooth = talib.EMA(talib.EMA(close, 200), 20).iloc[-1]

    # 收盘价的52周高点和52周低点
    high_52week = close.rolling(52 * 5).max().iloc[-1]
    low_52week = close.rolling(52 * 5).min().iloc[-1]

    # 最新收盘价
    cl = close.iloc[-1]

    # 筛选条件1：收盘价高于150日均线和200日均线
    if cl > ema_150 and cl > ema_200:
        condition_1 = True
    else:
        condition_1 = False

    # 筛选条件2：150日均线高于200日均线
    if ema_150 > ema_200:
        condition_2 = True
    else:
        condition_2 = False

    # 筛选条件3：200日均线上升1个月
    if ema_200 > ema_200_smooth:
        condition_3 = True
    else:
        condition_3 = False

    # 筛选条件4：50日均线高于150日均线和200日均线
    if ema_50 > ema_150 and ema_50 > ema_200:
        condition_4 = True
    else:
        condition_4 = False

    # 筛选条件5：收盘价高于50日均线
    if cl > ema_50:
        condition_5 = True
    else:
        condition_5 = False

    # 筛选条件6：收盘价比52周低点高30%
    if cl >= low_52week * 1.3:
        condition_6 = True
    else:
        condition_6 = False

    # 筛选条件7：收盘价在52周高点的25%以内
    if cl >= high_52week * 0.75 and cl <= high_52week * 1.25:
        condition_7 = True
    else:
        condition_7 = False

    # 筛选条件8：相对强弱指数大于等于70
    rs = close.pct_change(252).iloc[-1] / benchmark_ann_ret * 100
    if rs >= 70:
        condition_8 = True
    else:
        condition_8 = False

    # 判断股票是否符合标准
    if (condition_1 and condition_2 and condition_3 and
            condition_4 and condition_5 and condition_6 and
            condition_7 and condition_8):
        meet_criterion = True
    else:
        meet_criterion = False

    out = {
        "rs": round(rs, 2),
        "close": cl,
        "ema_50": ema_50,
        "ema_150": ema_150,
        "ema_200": ema_200,
        "high_52week": high_52week,
        "low_52week": low_52week,
        "meet_criterion": meet_criterion
    }

    return pd.Series(out)


symbols_to_screen = list(ohlc_joined.Symbol.unique())

# 将数据框的格式从long-format转化为wide-format
ohlc_joined_wide = ohlc_joined.pivot(columns="Symbol", values="Close").fillna(method="ffill")

results = ohlc_joined_wide.apply(screen, benchmark_ann_ret=benchmark_ann_ret)
results = results.T

selected_stock = results.query("meet_criterion == True").sort_values("rs", ascending=False)

stock_info_sh_name_code_df.columns = stock_info_sh_name_code_df.columns.str.replace('symbol', 'Symbol')
selected_stock_df = pd.merge(selected_stock, stock_info_sh_name_code_df, how='left', on='Symbol')
print(selected_stock_df)
