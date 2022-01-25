import logging

logger = logging.getLogger("firehose-producer")
logger.setLevel(logging.INFO)

# Format output
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Console handler
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.INFO)

# Add handlers
logger.addHandler(consoleHandler)
