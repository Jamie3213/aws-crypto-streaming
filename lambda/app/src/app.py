import asyncio
import os
import urllib.parse

from botocore.exceptions import ClientError
from trade_processor.exceptions import DynamoDbWriteError, S3ReadFileError
from trade_processor.files import S3File
from trade_processor.logger import logger

dynamodb_table = os.environ["DYNAMODB_TABLE_NAME"]


def on_error(future: asyncio.Future) -> None:
    error = future.exception()
    if error:
        logger.exception(error)
        raise DynamoDbWriteError(
            f"Failed to write trades to DynamoDB table {dynamodb_table!r}."
        )


async def async_lambda_handler(event, context) -> None:
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    try:
        compressed_trades = S3File(bucket, object_key).get_file()
    except ClientError as e:
        logger.exception(e)
        raise S3ReadFileError(
            f"Failed to read object {object_key!r} from S3 bucket {bucket!r}."
        )

    decompressed_trades = compressed_trades.decompress()
    parsed_trades = decompressed_trades.parse()
    trade_batches = parsed_trades.batch()

    loop = asyncio.get_running_loop()
    futures = []
    for batch in trade_batches:
        future = loop.run_in_executor(None, batch.put_to_dynamodb, dynamodb_table)
        future.add_done_callback(on_error)
        futures.append(future)

    await asyncio.gather(*futures)

    logger.info(
        f"Successfully put {len(parsed_trades)} trade records to DynamoDB "
        f"table {dynamodb_table!r} in {len(trade_batches)} batches."
    )

    return "Success"


def lambda_handler(event, context) -> str:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_lambda_handler(event, context))
