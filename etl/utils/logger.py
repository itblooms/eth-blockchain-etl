from pyspark.logger import PySparkLogger
import logging


def get_logger(name: str) -> PySparkLogger:
    logger = PySparkLogger.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger
