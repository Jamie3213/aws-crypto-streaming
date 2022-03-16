import logging

from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

format = "%(asctime)s %(name)s %(levelname)s %(message)s"
formatter = jsonlogger.JsonFormatter(format)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)
