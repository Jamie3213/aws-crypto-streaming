import os

import yaml
from botocore.exceptions import ClientError

from tiingo.client import TiingoClient
from tiingo.exceptions import TiingoClientError, TiingoSubscriptionError
from tiingo.logger import create_logger


def load_config_vars() -> tuple:
    with open("config.yml", "r") as stream:
        config = yaml.safe_load(stream)

    url = config["Api"]["Url"]
    stream_name = config["Firehose"]["StreamName"]
    batch_size = config["Firehose"]["BatchSize"]

    return (url, stream_name, batch_size)


def main() -> None:
    logger = create_logger(__name__)
    logger.info("Reading YAML config and extracting variables...")
    url, stream_name, batch_size = load_config_vars()

    logger.info("Connecting and subscribing to the Tiingo websocket crypto API...")
    token = os.environ["TIINGO_API_TOKEN"]

    try:
        client = TiingoClient(url, token)
    except TiingoSubscriptionError as e:
        logger.exception(e)
        raise e

    logger.info("Beginning stream...")

    while True:
        try:
            batch = client.get_next_batch(batch_size)
        except TiingoClientError as e:
            logger.exception(e)
            raise e

        compressed_batch = batch.compress()

        try:
            response = compressed_batch.put_to_kinesis_stream(stream_name)
            logger.info(f"Put batch {response.record_id!r} of size {batch_size} to Delivery Stream {stream_name!r}")
        except ClientError as e:
            logger.exception(e)
            raise e


if __name__ == "__main__":
    main()
