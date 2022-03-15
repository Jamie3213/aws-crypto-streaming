import gzip
import json
from datetime import datetime
from io import BytesIO
from typing import List, Tuple
from urllib.parse import unquote_plus

import boto3
from aws_lambda_powertools.utilities.data_classes import S3Event

from .data_structs import Trade


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


def _formatted_utc_now() -> str:
    return datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")


def parse_file_to_trade_list(file: BytesIO) -> List[Trade]:
    lines = [line.decode("utf-8").strip("\n") for line in file.readlines()]
    trades = [json.loads(line) for line in lines]
    return [Trade(*trade.values(), _formatted_utc_now()) for trade in trades]


def put_trade_to_dynamodb(trades: List[Trade], table: str) -> None:
    pass
