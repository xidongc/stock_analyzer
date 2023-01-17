from pyalgotrade import strategy
from pyalgotrade.barfeed import quandlfeed
from pyalgotrade.technical import ma, rsi
import iteround


class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, smaPeriod):
        super(MyStrategy, self).__init__(feed, 1000)

        self.__rsi = rsi.RSI(feed[instrument].getCloseDataSeries(), 14)
        self.__instrument = instrument
        self.__position = None
        # self.setUseAdjustedValues(True)
        self.__sma = ma.SMA(feed[instrument].getCloseDataSeries(), smaPeriod)

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("BUY at $%.2f" % (execInfo.getPrice()))

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("SELL at $%.2f" % (execInfo.getPrice()))
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was canceled, re-submit it.
        self.__position.exitMarket()

    def onBars(self, bars):
        if self.__sma[-1] is None:
            return

        bar = bars[self.__instrument]
        if self.__position is None:
            if bar.getPrice() > self.__sma[-1]:
                # Enter a buy market order for 10 shares. The order is good till canceled.
                self.__position = self.enterLong(self.__instrument, 10, True)
            # Check if we have to exit the position.
        elif bar.getPrice() < self.__sma[-1] and not self.__position.exitActive():
            self.__position.exitMarket()

        self.info("%s %s %s" % (bar.getClose(), self.__sma[-1], self.__rsi[-1]))


def run_strategy(smaPeriod):
    # Load the bar feed from the CSV file
    feed = quandlfeed.Feed()
    feed.addBarsFromCSV("aapl", "1.csv")

    # Evaluate the strategy with the feed.
    myStrategy = MyStrategy(feed, "aapl", smaPeriod)
    myStrategy.run()
    print("Final portfolio value: $%.2f" % myStrategy.getBroker().getEquity())


run_strategy(30)
