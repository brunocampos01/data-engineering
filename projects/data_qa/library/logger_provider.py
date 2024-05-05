import logging
from typing import Optional


class LoggerProvider:
    """
    This class provide a standard output format for logging messages
    """
    @staticmethod
    def get_logger():
        logging.basicConfig(format='%(asctime)s - %(filename)s - %(funcName)s -> %(message)s')
        logger = logging.getLogger('driver_logger')
        logger.setLevel(logging.INFO)
        return logger
