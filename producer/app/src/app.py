import yaml
from botocore.exceptions import ClientError

import tiingo.helpers as helpers
from tiingo.client import TiingoClient
from tiingo.exceptions import TiingoClientError, TiingoSubscriptionError


def load_config_vars() -> tuple:
    with open("config.yml", "r") as stream:
        config = yaml.safe_load(stream)

    url = config["Api"]["Url"]
    secret_name = config["Api"]["SecretName"]
    stream_name = config["Firehose"]["StreamName"]
    batch_size = config["Firehose"]["BatchSize"]

    return (url, secret_name, stream_name, batch_size)


def main() -> None:
    logger = helpers.create_logger(__name__)
    logger.info("Reading YAML config and extracting variables...")
    url, secret_name, stream_name, batch_size = load_config_vars()

    logger.info("Getting API token from Secrets Manager...")

    try:
        token = helpers.get_secrets_manager_secret(secret_name)
    except ClientError as e:
        logger.exception(e)
        raise e

    logger.info("Connecting and subscribing to the Tiingo websocket crypto API...")

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
