import os

from tiingo.client import TiingoClient
from tiingo.exceptions import (
    TiingoClientError,
    TiingoSubscriptionError,
    TiingoWriteDestinationError,
)
from tiingo.logger import create_logger

url = os.environ["TIINGO_API_URL"]
stream_name = os.environ["FIREHOSE_DELIVERY_STREAM"]
batch_size = int(os.environ["FIREHOSE_BATCH_SIZE"])
token = os.environ["TIINGO_API_TOKEN"]


def main() -> None:
    logger = create_logger(__name__)
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
            logger.info(
                f"Put batch {response.record_id!r} of size {batch_size} to Delivery Stream {stream_name!r}"
            )
        except TiingoWriteDestinationError as e:
            logger.exception(e)
            raise e


if __name__ == "__main__":
    main()
