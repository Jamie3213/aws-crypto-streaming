import gzip
import json
import re
import time
import traceback
from dataclasses import asdict
from datetime import datetime
from typing import List, Optional, Tuple
from urllib import response

import boto3
from botocore.exceptions import ClientError
from pydantic import validator
from pydantic.dataclasses import dataclass
from websocket import WebSocket

import helpers


logger = helpers.create_logger("tiingo")


@dataclass
class _TradeUpdate:
    """Trade update from the Tiingo crypto API.

    Attributes:
        ticker (str): The asset ticker.
        date (str): The trade timestamp in the form "%Y-%m-%dT%H:%M:%S.%fZ".
        price (str): The price the trade was executed at.
        processed_at (str): Application timestamp in the form "%Y-%m-%dT%H:%M:%SZ".
    """

    ticker: str
    date: str
    price: float
    processed_at: str

    @validator("date", "processed_at")
    def validate_date_format(cls, timestamp) -> Optional[str]:
        # Ensure timestamps adhere to a specific format.
        pattern = re.compile("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z")
        regex_match = re.match(pattern, timestamp)
        if not regex_match:
            raise ValueError(
                "Incorrect date format: should be '%Y-%m-%dT:%H:%M:%S.%fZ'"
            )
        else:
            return timestamp


@dataclass
class TradeUpdateBatch:
    """Holds batches of trade updates.

    Attributes:
        batch (List[_TradeUpdate]): A list of APi response messages.
    """

    batch: List[_TradeUpdate]

    def put_to_stream(self, stream: str) -> None:
        """Puts records to the Kinesis Firehose Stream. If a service error occurs,
        then the put is retried after a delay.

        Args:
            stream (str): The name of the Firehose stream.

        Returns:
            None
        """

        compressed_batch = self._compress_batch()
        retries = 0
        total_retries = 3
        while retries < total_retries:
            try:
                self._put_record_to_stream(compressed_batch, stream)
                break
            except ClientError as e:
                error_type = e.response["Error"]["Code"]
                if error_type == "ServiceUnavailableError" and retries < total_retries:
                    retries += 1
                    logger.warn(
                        f"Service unavailable, initiating retry {retries} of {total_retries}"
                    )
                    time.sleep(5)
                else:
                    logger.error(traceback.format_exc())
                    raise e

    def _compress_batch(self) -> bytes:
        # Converts a list of TradeUpdate dataclasses to a string of new line delimited
        # JSON documents and GZIP compresses the result.
        trade_dicts = [asdict(message) for message in self.batch]
        json_strings = [json.dumps(dict_) for dict_ in trade_dicts]
        new_line_delimited = "\n".join(json_strings) + "\n"
        batch_bytes = new_line_delimited.encode("utf-8")

        return gzip.compress(batch_bytes)

    @staticmethod
    def _put_record_to_stream(record: bytes, stream: str) -> None:
        # Writes a record to a Kinesis Data Firehose Delivery Stream.
        firehose_client = boto3.client("firehose")
        put_record = {"Data": record}
        firehose_client.put_record(DeliveryStreamName=stream, Record=put_record)


class TiingoSubscribeError(Exception):
    pass


class TiingoSession(WebSocket):
    """A websocket session connecting to a Tiingo WSS API.

    Attributes:
      url (str): The API URL.
      token (str): The API auth token.
    """

    def __init__(self, url: str, token: str) -> None:
        """Inits TiingoSession with the specified API auth token."""
        self.url = url
        self.token = token
        self.ws = self._subscribe_and_flush()

    def _subscribe_and_flush(self) -> WebSocket:
        # Subscribes to the crypto API and flushes initial responses.
        ws = WebSocket()
        ws.connect(self.url)

        subscribe = {
            "eventName": "subscribe",
            "authorization": self.token,
            "eventData": {"thresholdLevel": 5},
        }

        message = json.dumps(subscribe)
        ws.send(message)

        # Get the first API message
        response = json.loads(ws.recv())["response"]
        response_code = response["code"]
        response_message = response["message"]

        if response_code != 200:
            raise TiingoSubscribeError(
                f"Failed with error code {response_code} and message '{response_message}'."
            )

        return ws

    def get_batch(self, size: int) -> TradeUpdateBatch:
        """Creates a batch of trade update messages.

        Attributes:
            size (int): The size of the batch.

        Returns:
            TradeUpdateBatch: List of messages from the API.
        """
        batch = [self._next_trade() for _ in range(size)]
        return TradeUpdateBatch(batch)

    def _next_trade(self) -> _TradeUpdate:
        # Returns the next trade from the API.
        data, update_type = self._get_response()

        # Only return trades, not top-of-book quotes
        while update_type != "T":
            data, update_type = self._get_response()

        return self._process_response_data(data)

    def _get_response(self) -> Tuple[List[str], str]:
        # Returns the next API response and the update type.
        record, response_type = self._filter_response_type()

        while response_type != "A":
            record, response_type = self._filter_response_type()

        data = record["data"]
        update_type = data[0]

        return (data, update_type)

    def _filter_response_type(self) -> Tuple[dict, str]:
        # Extract the message type from the API response and return it
        # along with the response itself.
        response = self.ws.recv()
        record = json.loads(response)
        response_type = record["messageType"]

        return (record, response_type)

    def _process_response_data(self, data: List[str]) -> _TradeUpdate:
        # Creates a TradeUpdateMessage dataclass from a data array.
        ticker = data[1]
        date = data[2]
        price = data[5]

        output_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        api_format = "%Y-%m-%dT%H:%M:%S.%f%z"

        formatted_date = self._process_date(date, api_format, output_format)
        now = datetime.strftime(datetime.utcnow(), output_format)

        return _TradeUpdate(ticker, formatted_date, float(price), now)

    @staticmethod
    def _process_date(date_as_string: str, format_in: str, format_out: str) -> str:
        # Trade timestamps returned from the Tiingo API have a standard format
        # except in instances where the microsecond, "%f", part is zero, in
        # which case it is omitted. In this case, the method adds the correct
        # ".000000" part to the timestamp.
        try:
            date_as_datetime = datetime.strptime(date_as_string, format_in)
        except ValueError:
            first_part = date_as_string[:19]
            last_part = date_as_string[19:]
            f_part = f".{6 * '0'}"
            date_as_datetime = datetime.strptime(f"{first_part}{f_part}{last_part}", format_in)

        return datetime.strftime(date_as_datetime, format_out)
