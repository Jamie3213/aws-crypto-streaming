import traceback

import yaml
from aws_helpers import get_secrets_manager_secret
from botocore.exceptions import ClientError
from logger import create_logger
from tiingo import TiingoClient, TiingoSubscriptionError, TiingoMessageError

import time


def main() -> None:
    logger = create_logger("main")

    logger.info("Reading YAML config and extracting variables...")
    with open("config.yml", "r") as stream:
        config = yaml.safe_load(stream)

    URL = config["Api"]["Url"]
    SECRET_NAME = config["Api"]["SecretName"]
    FIREHOSE_STREAM_NAME = config["Firehose"]["StreamName"]
    BATCH_SIZE = config["Firehose"]["BatchSize"]
    TOTAL_RETRIES = config["Firehose"]["TotalRetries"]

    try:
        logger.info("Getting API token from Secrets Manager...")
        token = get_secrets_manager_secret(SECRET_NAME)
    except ClientError as e:
        logger.error(traceback.format_exc())
        raise e

    try:
        logger.info("Connecting to Tiingo websocket crypto API...")
        client = TiingoClient(URL, token)
    except TiingoSubscriptionError as e:
        logger.error(traceback.format_exc())
        raise e

    logger.info("Beginning stream...")
    start = time.time()
    # while True:
    for _ in range(30):
        try:
            batch = client.get_next_batch(BATCH_SIZE)
        except TiingoMessageError as e:
            logger.error(traceback.format_exc())
            raise e

        compressed_batch = batch.compress_batch()
        try:
            compressed_batch.put_to_kinesis_stream(FIREHOSE_STREAM_NAME, TOTAL_RETRIES)
            logger.info(f"Successfully put batch of size {BATCH_SIZE} to stream '{FIREHOSE_STREAM_NAME}'.")
        except ClientError as e:
            logger.error(traceback.format_exc())
            raise e
    end = time.time()
    logger.info(f"Wrote 30,000 records in {end - start}")


if __name__ == "__main__":
    main()
