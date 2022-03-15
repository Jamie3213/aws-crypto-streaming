import json
from typing import List

import utils.functions as uf
from aws_lambda_powertools.utilities.data_classes import S3Event, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError
from utils.data_structs import LambdaResponse, Trade
from utils.logger import create_logger

logger = create_logger(__name__)


# @event_source(data_class=S3Event)
# def lambda_handler(event: S3Event, context: LambdaContext) -> LambdaResponse:
#     """Reads the event file from S3, decompresses, parses and writes to DynamoDB."""

if __name__ == "__main__":

    logger.info("Extracting object information from S3 change event...")
    bucket, object_key = (
        "s3-jamie-general-data-lake",
        "bronze/crypto/2022/03/11/17/firehose-jamie-crypto-stream-1-2022-03-11-17-06-57-b8cc1876-9373-4175-81e2-fe51faae43d8",
    )

    logger.info(f"Attempting to read file {object_key!r} from bucket {bucket!r}...")

    try:
        file = uf.get_file_from_s3(bucket, object_key)
    except ClientError as e:
        logger.exception(e)
        # return LambdaResponse(
        #     isBase64Encoded=False,
        #     statusCode=400,
        #     body=f"Failed to read object {object_key!r} from S3 bucket {bucket!r}."
        # )

    logger.info("Decompressing file and parsing trade data...")

    decompressed_file = uf.decompress_file(file)
    trades: List[Trade] = uf.parse_file_to_trade_list(decompressed_file)

    print(trades[0])

    # return LambdaResponse(
    #     isBase64Encoded=False,
    #     statusCode=200,
    #     body=json.dumps(trades[0])
    # )

    # logger.info(f"Attempting to write trade data to DynamoDB table {dynamodb_table!r}...")
