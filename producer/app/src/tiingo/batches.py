import gzip
import json
import time
from typing import Callable, List

import boto3
from botocore.exceptions import ClientError
from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder

from .data_structs import TradeMessage


@dataclass
class FirehoseResponse:
    record_id: str
    encrypted: bool


def aws_retry(total_retries: int = 2, delay: int = 5) -> Callable:
    """
    Retries the function if a 'ServiceUnavailableError' is raised, increasing the time
    between retries as a factor of the number of retries. After the total retries value
    is exceeded, an error is raised.
    """

    def decorator(function: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> None:
            retries = 0
            while retries <= total_retries:
                try:
                    return function(*args, **kwargs)
                except ClientError as e:
                    error_type = e.response["Error"]["Code"]
                    if (
                        error_type == "ServiceUnavailableError"
                        and retries < total_retries
                    ):
                        retries += 1
                        time.sleep(retries * delay)
                    else:
                        raise e

        return wrapper

    return decorator


class CompressedBatch(bytes):
    """Represents a compressed set of trade update messages."""

    def __init__(self, batch: bytes) -> None:
        self.batch = batch

    @aws_retry()
    def put_to_kinesis_stream(self, stream: str) -> FirehoseResponse:
        """Writes a record to a Kinesis Data Firehose Delivery Stream."""
        firehose_client = boto3.client("firehose")
        record = {"Data": self.batch}
        response = firehose_client.put_record(DeliveryStreamName=stream, Record=record)
        return FirehoseResponse(
            record_id=response["RecordId"], encrypted=response["Encrypted"]
        )


class TradeBatch(list):
    """Holds batches of trade updates in a list."""

    def __init__(self, batch: List[TradeMessage]) -> None:
        self.batch = batch

    def __len__(self) -> int:
        return len(self.batch)

    def compress(self) -> CompressedBatch:
        """
        Converts a list of TradeUpdate dataclasses to a string of new line delimited
        JSON documents and GZIP compresses the result.
        """
        json_strings = [
            json.dumps(trade, default=pydantic_encoder) for trade in self.batch
        ]
        new_line_delimited_trades = "\n".join(json_strings)
        encoded_trades = new_line_delimited_trades.encode("utf-8")
        compressed_record = gzip.compress(encoded_trades)
        return CompressedBatch(compressed_record)
