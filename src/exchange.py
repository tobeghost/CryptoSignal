import sys
import re
import ccxt
import structlog
import time
from datetime import datetime, timedelta, timezone
from tenacity import retry, retry_if_exception_type, stop_after_attempt

class ExchangeService():
    """
    Service for performing queries against exchange API's
    """

    def __init__(self, config):
        """
        Initializes Exchange Service class
        :param config: A dictionary containing configuration
        """

        self.logger = structlog.get_logger()
        self.exchanges = dict()
        self.markets = dict()
        self.exclude = []

        # Loads the exchanges using ccxt
        for exchange in config['exchanges']:
            if exchange['enabled']:
                parameters = {'enableRateLimit': True}
                if 'future' in exchange.keys():
                    if exchange['future']:
                        parameters['options'] = {'defaultType': 'future'}
                new_exchange = getattr(ccxt, exchange['name'])(parameters)

                # sets up api permissions for user if given
                if new_exchange:
                    self.exchanges[new_exchange.id] = new_exchange

                    if 'pairs' in exchange:
                        if len(exchange['pairs']) > 0:
                            self.markets[new_exchange.id] = exchange['pairs']
                        else:
                            self.markets[new_exchange.id] = list()

                    if 'exclude' in exchange:
                        self.exclude = exchange['exclude']
                else:
                    self.logger.error("Unable to load exchange %s", exchange['name'])

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def getHistoricalData(self, market_pair, exchange, time_unit, start_date=None, max_periods=240):
        """
        Get historical OHLCV for a symbol pair
        :param market_pair: Contains the symbol pair to operate on i.e. BURST/BTC
        :param exchange: Contains the exchange to fetch the historical data from.
        :param time_unit: A string specifying the ccxt time unit i.e. 5m or 1d.
        :param start_date: Timestamp in milliseconds.
        :param max_periods: Defaults to 100. Maximum number of time periods back to fetch data for.
        :return: Contains a list of lists which contain timestamp, open, high, low, close, volume.
        """

        try:
            if time_unit not in self.exchanges[exchange].timeframes:
                raise ValueError(
                    "{} does not support {} time frame for OHLCV data. Possible values are: {}".format(
                        exchange,
                        time_unit,
                        list(self.exchanges[exchange].timeframes)
                    )
                )
        except AttributeError:
            self.logger.error('%s interface does not support time frame queries! We are unable to fetch data!', exchange)
            raise  AttributeError(sys.exc_info())

        if not start_date:
            timeframe = re.compile('([0-9]+)([a-zA-Z])').match(time_unit)
            time_qty = timeframe.group(1)
            time_period = timeframe.group(2)

            timedelta_values = {
                'm': 'minutes',
                'h': 'hours',
                'd': 'days',
                'w': 'weeks',
                'M': 'months',
                'y': 'years'
            }

            timedelta_args = {
                timedelta_values[time_period]: int(time_qty)
            }

            start_date_delta = timedelta(**timedelta_args)

            max_days_date = datetime.now() - (max_periods * start_date_delta)

            start_date = int(max_days_date.replace(tzinfo=timezone.utc).timestamp() * 1000)

            historical_data = self.exchanges[exchange].fetch_ohlcv(market_pair, timeframe=time_unit, since=start_date)

            if not historical_data:
                raise ValueError('No historical data provided returned by exchange.')

            historical_data.sort(key=lambda d: d[0])

            time.sleep(self.exchanges[exchange].rateLimit / 1000)

            return historical_data

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def getExchangeMarkets(self, exchanges=[], markets=[]):
        """
        Get market data for all symbol pairs listed on all configured exchanges.
        :param exchanges: A list of markets to get from the exchanges. Default is all markets.
        :param markets: A list of exchanges to collect market data from. Default is all enabled exchanges.
        :return: A dictionary containing market data for all symbol pairs.
        """

        result = dict()

        if not exchanges:
            exchanges = self.exchanges

        for exchange in exchanges:
            result[exchange] = self.exchanges[exchange].load_markets()
            curr_markets = {
                k: v for k, v in result[exchange].items() if v['active'] == True
            }

            if markets:
                # Only retrieve markets the users specified
                result[exchange] = {
                    key: curr_markets[key] for key in curr_markets if key in markets
                }

                for market in markets:
                    if market not in result[exchange]:
                        self.logger.info('%s has no market %s, ignoring.', exchange, market)
            else:
                # Retrieve all markets
                if self.markets[exchange]:
                    self.logger.info('Getting all %s market pairs for %s ' % (self.markets[exchange], exchange))
                    result[exchange] = {
                        key: curr_markets[key] for key in curr_markets if curr_markets[key]['quote'] in self.markets[exchange]
                    }

                    if isinstance(self.exclude, list) and len(self.exclude) > 0:
                        for market in self.markets[exchange]:
                            for pair_to_exclude in self.exclude:
                                result[exchange].pop(pair_to_exclude, None)
                                result[exchange].pop(pair_to_exclude + '/' + market, None)

            time.sleep(self.exchanges[exchange].rateLimit / 1000)

        return result
