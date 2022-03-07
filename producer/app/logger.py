import logging

from pythonjsonlogger import jsonlogger


def create_logger(name: str) -> logging.Logger:
    """Create a formatted logger that logs to the console."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    consoleHandler.setLevel(logging.INFO)

    logger.addHandler(consoleHandler)

    return logger
