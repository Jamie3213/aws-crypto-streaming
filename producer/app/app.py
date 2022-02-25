import traceback

import yaml
from botocore.exceptions import ClientError

from helpers import get_secrets_manager_secret
from logger import create_logger
from tiingo import TiingoClient, TiingoClientError


def main() -> None:
    logger = create_logger("main")

    logger.info("Reading YAML config and extracting variables...")
    with open("config.yml", "r") as stream:
        config = yaml.safe_load(stream)

    url = config["Api"]["Url"]
    secret_name = config["Api"]["SecretName"]
    firehose_stream_name = config["Firehose"]["StreamName"]
    batch_size = config["Firehose"]["BatchSize"]

    try:
        logger.info("Getting API token from Secrets Manager...")
        token = get_secrets_manager_secret(secret_name)
    except ClientError as e:
        logger.error(traceback.format_exc())
        raise e

    try:
        logger.info("Connecting to Tiingo websocket crypto API...")
        client = TiingoClient(url, token)
    except TiingoClientError as e:
        logger.error(traceback.format_exc())
        raise e

    logger.info("Beginning stream...")
    while True:
        batch = client.get_next_batch(batch_size)
        compressed_batch = batch.compress_batch()
        try:
            compressed_batch.put_to_kinesis_stream(firehose_stream_name)
            logger.info(f"Successfully put batch of size {batch_size} to stream '{firehose_stream_name}'.")
        except ClientError as e:
            logger.error(traceback.format_exc())
            raise e


if __name__ == "__main__":
    main()
