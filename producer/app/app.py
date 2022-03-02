import base64
import logging
import traceback

import boto3
import yaml
from botocore.exceptions import ClientError
from logger import create_logger
from tiingo import TiingoClient, TiingoClientError, TiingoSubscriptionError


logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.INFO)

logger.addHandler(consoleHandler)

def get_secrets_manager_secret(name: str) -> str:
    """Returns the secret from AWS Secrets Manager."""
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")

    return secret_string if secret_string else base64.b64decode(secret_binary)


def load_config_vars() -> tuple:
    with open("config.yml", "r") as stream:
        config = yaml.safe_load(stream)

    url = config["Api"]["Url"]
    secret_name = config["Api"]["SecretName"]
    stream_name = config["Firehose"]["StreamName"]
    batch_size = config["Firehose"]["BatchSize"]
    total_retries = config["Firehose"]["TotalRetries"]

    return (url, secret_name, stream_name, batch_size, total_retries)


def main() -> None:
    logger = create_logger("main")
    logger.info("Reading YAML config and extracting variables...")
    url, secret_name, stream_name, batch_size, total_retries = load_config_vars()

    logger.info("Getting API token from Secrets Manager...")

    try:
        token = get_secrets_manager_secret(secret_name)
    except ClientError as e:
        logger.error(traceback.format_exc())
        raise e

    logger.info("Connecting to Tiingo websocket crypto API...")

    try:
        client = TiingoClient(url, token)
    except TiingoSubscriptionError as e:
        logger.error(traceback.format_exc())
        raise e

    logger.info("Beginning stream...")

    while True:
        try:
            batch = client.get_next_batch(batch_size)
        except TiingoClientError as e:
            logger.error(traceback.format_exc())
            raise e

        compressed_batch = batch.compress_batch()

        try:
            compressed_batch.put_to_kinesis_stream(stream_name, total_retries)
            logger.info(
                f"Successfully put batch of size {batch_size} to stream '{stream_name}'."
            )
        except ClientError as e:
            logger.error(traceback.format_exc())
            raise e


if __name__ == "__main__":
    main()
