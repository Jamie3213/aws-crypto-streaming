import os
from typing import List

import utils.functions as uf
from aws_lambda_powertools.utilities.data_classes import S3Event, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError
from utils.data_structs import LambdaResponse, Trade
from utils.logger import create_logger

logger = create_logger(__name__)
dynamodb_table = os.environ["DYNAMODB_TABLE_NAME"]


@event_source(data_class=S3Event)
def lambda_handler(event: S3Event, context: LambdaContext) -> LambdaResponse:
    """Reads the event file from S3, decompresses, parses and writes to DynamoDB."""

    logger.info("Extracting object information from S3 change event...")
    bucket, object_key = (
        "s3-jamie-general-data-lake",
        "bronze/crypto/2022/03/15/14/firehose-jamie-crypto-stream-1-2022-03-15-14-13-47-7827cb19-975c-47f5-a732-8e77e7e27d3f",
    )

    logger.info(f"Attempting to read file {object_key!r} from bucket {bucket!r}...")

    try:
        file = uf.get_file_from_s3(bucket, object_key)
    except ClientError as e:
        logger.exception(e)
        return LambdaResponse(
            isBase64Encoded=False,
            statusCode=400,
            body=f"Failed to read object {object_key!r} from S3 bucket {bucket!r}.",
        )

    logger.info("Decompressing file and parsing trade data...")

    decompressed_file = uf.decompress_file(file)
    trades: List[Trade] = uf.parse_file_to_trade_list(decompressed_file)

    logger.info(
        f"Attempting to write trade data to DynamoDB table {dynamodb_table!r}..."
    )

    try:
        uf.put_trades_to_dynamodb(trades, dynamodb_table)
    except ClientError as e:
        logger.exception(e)
        return LambdaResponse(
            isBase64Encoded=False,
            statusCode=400,
            body=f"Failed to trade to DynamoDB table {dynamodb_table!r}.",
        )

    return LambdaResponse(
        isBase64Encoded=False,
        statusCode=200,
        body=f"Successfully put {len(trades)} trade records to DynamoDB table {dynamodb_table!r}.",
    )
