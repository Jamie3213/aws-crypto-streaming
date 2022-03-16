import os
from http import HTTPStatus

from botocore.exceptions import ClientError
from trade_processor._lambda import LambdaResponse
from trade_processor.files import S3File
from trade_processor.logger import logger

dynamodb_table = os.environ["DYNAMODB_TABLE_NAME"]


def lambda_handler(event, context) -> LambdaResponse:
    """Reads the event file from S3, decompresses, parses and writes to DynamoDB."""

    logger.info("Extracting metadata from S3 event...")

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    logger.info(f"Attempting to read file {object_key!r} from bucket {bucket!r}...")

    try:
        file = S3File(bucket, object_key).get_file()
    except ClientError as e:
        logger.exception(e)
        return LambdaResponse(
            isBase64Encoded=False,
            statusCode=HTTPStatus.INTERNAL_SERVER_ERROR,
            body=f"Failed to read object {object_key!r} from S3 bucket {bucket!r}.",
        )

    logger.info("Decompressing file and parsing trade data...")

    decompressed_file = file.decompress()
    trades = decompressed_file.parse()
    batches = trades.create_batches()

    logger.info(f"Writing trades to DynamoDB table {dynamodb_table!r}...")

    for batch in batches:
        try:
            batch.put_to_dynamodb()
        except ClientError as e:
            logger.exception(e)
            return LambdaResponse(
                isBase64Encoded=False,
                statusCode=400,
                body=f"Failed to write trades to DynamoDB table {dynamodb_table!r}.",
            )

    return LambdaResponse(
        isBase64Encoded=False,
        statusCode=HTTPStatus.OK,
        body=f"Successfully put {len(trades)} trade records to DynamoDB table {dynamodb_table!r} in {len(batches)} batches.",
    )
