from pyalgotrade.optimizer import worker
from pyalgo import rsi

# The if __name__ == '__main__' part is necessary if running on Windows.
if __name__ == '__main__':
    worker.run(rsi.RSI2, "localhost", 5000, workerName="localworker")
