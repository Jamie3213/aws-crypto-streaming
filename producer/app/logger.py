import logging


def create_logger(name: str) -> logging.Logger:
    """Create a formatted logger that logs to the console.
    
    Attributes:
        name (str): The name of the logger.

    Returns:
        Logger: The configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    consoleHandler.setLevel(logging.INFO)

    logger.addHandler(consoleHandler)

    return logger
