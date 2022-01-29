import base64
import logging
from dataclasses import asdict
from logging import Logger

import boto3


def create_logger(name: str) -> Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    consoleHandler.setLevel(logging.INFO)

    logger.addHandler(consoleHandler)
    
    return logger


def get_secrets_manager_secret(name: str) -> str:
    """Returns the secret from AWS Secrets Manager.

    Args:
      name (str): The name of the secret or the secret ARN

    Returns:
      str: The secret string
    """
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")

    return secret_string if secret_string else base64.b64decode(secret_binary)
