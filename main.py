import concurrent.futures

import backtrader as bt

import dataset
from strategies.cross_over import CrossOverStrategy
from dataset import *

test = True


def run_instrument(ins,
                   use_exist_data=False,
                   enable_plot=False,
                   enable_stats=False):
    cerebro = bt.Cerebro()
    if enable_stats:
        stats = cerebro.optstrategy(CrossOverStrategy,
                                    short_period=range(1, 5),
                                    long_period=range(50, 60))
    cerebro.addstrategy(CrossOverStrategy)
    cerebro.broker.setcash(cash_amount)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcommission(commission=commission_rate)
    if not use_exist_data:
        load_daily_data(ins, start_date, end_date)
    data = load_dataset(ins, start_date, end_date)
    cerebro.adddata(data)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    dataset.write_report("crossover", ins, start_date, end_date, cash_amount, cerebro.broker.getvalue())
    if enable_plot:
        print("Enable plotting")
        cerebro.plot()


if __name__ == '__main__':

    if test is True:
        instruments = ["600010"]
    else:
        instruments = ["600000", "600010", "600015", "600016",
                       "600028", "600029", "600030", "600036",
                       "600048", "600050", "600089", "600100",
                       "600104", "600109", "600111", "600150",
                       "600196", "600256", "600332", "600340",
                       "600372", "600406", "600485", "600518",
                       "600519", "600547", "600583", "600585",
                       "600606", "600637", "600690", "600703",
                       "600795", "600837", "600887",
                       "600893", "600919", "600958", "600999",
                       "601006", "601088", "601118", "601166",
                       "601169", "601186", "601198", "601211",
                       "601288", "601318",
                       "601328", "601336", "601377", "601390",
                       "601398", "601601", "601628", "601668",
                       "601669", "601688", "601727", "601766",
                       "601788", "601800", "601818", "601857",
                       "601901", "601919", "601985",
                       "600018", "601988", "601989", "601998"]

    start_date = "20180104"
    end_date = "20230113"

    cash_amount = 1000.0
    commission_rate = 0.0015

    dataset.delete_report("crossover")
    dataset.init_report("crossover")

    if not test:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for ins in instruments:
                futures.append(executor.submit(run_instrument, ins=ins))
            # for future in concurrent.futures.as_completed(futures):
            #     print(future.result())
    else:
        for ins in instruments:
            run_instrument(ins, use_exist_data=True, enable_plot=True, enable_stats=True)
