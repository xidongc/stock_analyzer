import backtrader as bt


class CrossOverStrategy(bt.Strategy):

    # 金叉死叉策略
    #     当短期均线上穿长期均线，出现金叉，买入
    #     当短期均线下穿长期均线，出现死叉，卖出

    params = (
        ('short_period', 5),
        ('long_period', 50),
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        self.sma_short = bt.indicators.MovingAverageSimple(self.data, period=self.params.short_period)
        self.sma_long = bt.indicators.MovingAverageSimple(self.data, period=self.params.long_period)
        self.gold_signal = self.sma_short > self.sma_long
        self.dead_signal = self.sma_long > self.sma_short
        rsi = bt.indicators.RSI(self.datas[0])
        bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                            subplot=True)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                # self.log(
                #     'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                #     (order.executed.price,
                #      order.executed.value,
                #      order.executed.comm))
                pass

                self.buy_price = order.executed.price
                self.buy_comm = order.executed.comm
            else:  # Sell
                # self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                #          (order.executed.price,
                #           order.executed.value,
                #           order.executed.comm))
                pass

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            # self.log('Order Canceled/Margin/Rejected')
            pass

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
        #          (trade.pnl, trade.pnlcomm))

    def next(self):
        if self.order:
            return
        if not self.position:
            if self.gold_signal[0] and self.dead_signal[-1]:
                # self.log('BUY CREATE, %.2f' % self.data.close[0])
                self.order = self.buy()
        else:
            if self.dead_signal[0] and self.gold_signal[-1]:
                # self.log('SELL CREATE, %.2f' % self.data.close[0])
                self.order = self.sell()

    def stop(self):
        self.log('(MA Short Period %2d, MA Long Period %2d) Ending Value %.2f' %
                 (self.params.short_period, self.params.long_period, self.broker.getvalue()))
