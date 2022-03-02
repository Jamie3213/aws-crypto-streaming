import traceback

import yaml
from aws_helpers import get_secrets_manager_secret
from botocore.exceptions import ClientError
from logger import create_logger
from tiingo import TiingoClient, TiingoSubscriptionError, TiingoMessageError


def load_config_vars(file: str) -> tuple:
    with open(file, "r") as stream:
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
    url, secret_name, stream_name, batch_size, total_retries = load_config_vars(
        "config.yml"
    )

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
        except TiingoMessageError as e:
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
