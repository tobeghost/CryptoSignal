import time
import structlog
from threading import Thread

class AnalysisWorker(Thread):
    def __init__(self, workerName, config, logger):
        Thread.__init__(self)
        self.workerName = workerName
        self.config = config
        self.logger = logger

    def run(self):
        update_interval = 300

        if 'settings' in self.config:
            settings = self.config['settings']
            if 'update_interval' in settings:
                update_interval = settings['update_interval']

        while True:
            self.logger.info("Starting {}".format(self.workerName))
            self.logger.info("{} sleeping for {} seconds".format(self.workerName, update_interval))
            time.sleep(update_interval)
