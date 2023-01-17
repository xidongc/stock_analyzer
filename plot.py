import mplfinance as mpf


def plot_data(df):
    mpf.plot(df, type='candle', mav=(3, 6, 9), volume=True, show_nontrading=False)
