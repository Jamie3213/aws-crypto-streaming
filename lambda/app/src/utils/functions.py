import gzip
import json
from dataclasses import asdict
from datetime import datetime
from io import BytesIO
from typing import List, Tuple
from urllib.parse import unquote_plus

import boto3
from aws_lambda_powertools.utilities.data_classes import S3Event

from .data_structs import (
    DynamoDbAttributeMapping,
    DynamoDbAttributeType,
    DynamoDbItem,
    DynamoDbPutRequest,
    Trade,
)


def get_event_bucket_and_object_key(event: S3Event) -> Tuple[str, str]:
    bucket = event.bucket_name
    record = event.record[0]
    object_key = unquote_plus(record.s3.get_object.key)
    return (bucket, object_key)


def get_file_from_s3(bucket: str, object_key: str) -> bytes:
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=object_key)
    return response["Body"].read()


def decompress_file(file: bytes) -> BytesIO:
    decompressed = gzip.decompress(file)
    return BytesIO(decompressed)


def parse_file_to_trade_list(file: BytesIO) -> List[Trade]:
    lines = [line.decode("utf-8").strip("\n") for line in file.readlines()]
    trades = [json.loads(line) for line in lines]

    parsed_trades = []
    for trade in trades:
        ticker = trade["ticker"]
        executed_at = datetime.strptime(
            trade["date"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).timestamp()
        exchange = trade["exchange"]
        size = trade["size"]
        price = trade["price"]
        producer_processed_at = datetime.strptime(
            trade["processed_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).timestamp()

        trade = Trade(ticker, executed_at, exchange, size, price, producer_processed_at)
        parsed_trades.append(trade)

    return parsed_trades


def _format_trade_list_for_dynamodb(
    trades: List[Trade],
) -> List[DynamoDbPutRequest]:
    put_requests = []
    for trade in trades:
        attributes = {
            "ticker": DynamoDbAttributeMapping(
                DynamoDbAttributeType.STRING.value, trade.ticker
            ),
            "executed_at": DynamoDbAttributeMapping(
                DynamoDbAttributeType.NUMBER.value, trade.executed_at
            ),
            "exchange": DynamoDbAttributeMapping(
                DynamoDbAttributeType.STRING.value, trade.exchange
            ),
            "size": DynamoDbAttributeMapping(
                DynamoDbAttributeType.NUMBER.value, trade.size
            ),
            "price": DynamoDbAttributeMapping(
                DynamoDbAttributeType.NUMBER.value, trade.price
            ),
            "producer_produced_at": DynamoDbAttributeMapping(
                DynamoDbAttributeType.NUMBER.value, trade.producer_processed_at
            ),
            "lambda_produced_at": DynamoDbAttributeMapping(
                DynamoDbAttributeType.NUMBER.value, trade.lambda_processed_at
            ),
        }

        item = DynamoDbItem(attributes)
        put_request = DynamoDbPutRequest(item)
        put_requests.append(put_request)

    return put_requests


def _create_put_request_batches(
    put_requests: List[DynamoDbPutRequest],
) -> List[List[DynamoDbPutRequest]]:
    pass


def put_trades_to_dynamodb(trades: List[Trade], table: str) -> None:
    client = boto3.client("dynamodb")
