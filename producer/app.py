import traceback

import yaml
from botocore.exceptions import ClientError

import helpers
from logger import logger
from tiingo_session import TiingoSession, WebSocketSubscribeError


def main() -> None:
    logger.info("Reading YAML config and extracting variables.")
    with open("config.yml", "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.error(traceback.format_exc())
            raise (e)

    api_secret_name = config["Api"]["SecretName"]
    firehose_stream_name = config["Firehose"]["StreamName"]
    batch_size = config["Firehose"]["BatchSize"]

    try:
        logger.info("Getting API token from Secrets Manager.")
        token = helpers.get_secrets_manager_secret(api_secret_name)
    except ClientError as e:
        logger.error(traceback.format_exc())
        raise e

    try:
        logger.info("Connecting to Tiingo WebSocket Crypto API.")
        session = TiingoSession(token).subscribe_and_flush()
    except WebSocketSubscribeError as e:
        logger.error(traceback.format_exc())
        raise (e)

    while True:
        batch = [session.next_trade() for _ in range(batch_size)]
        compressed_batch = helpers.compress_message_batch(batch)

        try:
            helpers.put_record_to_firehose(firehose_stream_name, compressed_batch)
        except ClientError as e:
            logger.error(traceback.format_exc())
            raise (e)


if __name__ == "__main__":
    main()
