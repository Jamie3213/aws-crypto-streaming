import base64
import logging

import boto3
from pythonjsonlogger import jsonlogger


def get_secrets_manager_secret(name: str) -> str:
    """Returns the secret from AWS Secrets Manager."""
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")

    return secret_string if secret_string else base64.b64decode(secret_binary)


def create_logger(name: str) -> logging.Logger:
    """Create a formatted logger that logs to the console."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    consoleHandler.setLevel(logging.INFO)

    logger.addHandler(consoleHandler)

    return logger
