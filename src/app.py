import sys
import structlog
import conf
from exchange import ExchangeService
from analysis import AnalysisWorker

def main():
    """
    Initializes the application
    """

    # Load settings and create the config object
    config = conf.getAll()

    # Setup logger
    logger = structlog.get_logger()

    # Configure and run configured behaviour
    exchange_service = ExchangeService(config)

    if 'pairs' in config:
        market_pairs = config['pairs']
        logger.info("Found configured markets: {}".format(market_pairs))
        markets = exchange_service.getExchangeMarkets(markets=market_pairs)
    else:
        logger.info("No configured markets, using all available on exchange.")
        markets = exchange_service.getExchangeMarkets()

    # Configure threads
    threads = []
    for exchange in markets:
        for pair in markets[exchange].keys():
            worker_name = "Worker for {}".format(pair)
            worker = AnalysisWorker(worker_name, config, logger)
            threads.append(worker)
            worker.daemon = True
            worker.start()

    logger.info('All workers are running!')

    for worker in threads:
        worker.join()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
