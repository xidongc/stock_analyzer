import akshare as ak
from backtrader.feeds import YahooFinanceCSVData
import pandas as pd
import csv
import os


def load_daily_data(symbol, start_date, end_date):
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date,
                                            end_date=end_date, adjust="")
    assert len(stock_zh_a_hist_df) != 0
    stock_zh_a_hist_df_adj = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date,
                                                end_date=end_date, adjust="")
    assert len(stock_zh_a_hist_df_adj) == len(stock_zh_a_hist_df)

    stock_zh_a_hist_df_adj = stock_zh_a_hist_df_adj[["收盘"]]
    stock_zh_a_hist_df_adj.columns = ["Adj Close"]

    stock_zh_a_hist_df = stock_zh_a_hist_df[["日期", "开盘", "最高", "最低", "收盘", "成交额"]]
    stock_zh_a_hist_df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]

    stock_zh_a_hist_df = pd.concat([stock_zh_a_hist_df, stock_zh_a_hist_df_adj], axis=1)

    order = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    stock_zh_a_hist_df = stock_zh_a_hist_df[order]
    stock_zh_a_hist_df.index.name = "Date"
    stock_zh_a_hist_df.set_index('Date', inplace=True)
    file_name = "tmp/{}_{}_{}.csv".format(symbol, start_date, end_date)
    stock_zh_a_hist_df.to_csv(file_name)
    print("{} lines of data imported".format(len(stock_zh_a_hist_df)))
    return stock_zh_a_hist_df


def load_dataset(symbol, start_date, end_date):
    file_name = "tmp/{}_{}_{}.csv".format(symbol, start_date, end_date)
    data = YahooFinanceCSVData(dataname=file_name, reverse=False)
    return data


def get_daily_dataset(symbol, start_date, end_date):
    load_daily_data(symbol, start_date, end_date)
    return load_dataset(symbol, start_date, end_date)


def init_report(algorithm):
    path = "report/{}.csv".format(algorithm)
    with open(path, 'w') as f:
        csv_write = csv.writer(f)
        data = ["instrument", "start_date", "end_date", "start_amount", "final_amount"]
        csv_write.writerow(data)


def write_report(algorithm, instrument, start_date, end_date, start_amount, final_amount):
    path = "report/{}.csv".format(algorithm)
    with open(path, 'a') as f:
        csv_write = csv.writer(f)
        data = [instrument, start_date, end_date, start_amount, final_amount]
        csv_write.writerow(data)


def delete_report(algorithm):
    path = "report/{}.csv".format(algorithm)
    if os.path.exists(path):
        os.remove(path)
