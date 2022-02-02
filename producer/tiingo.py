from __future__ import annotations

import gzip
import json
import re
import time
import traceback
from collections.abc import Iterable
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, Literal, Optional, Tuple, TypedDict

import boto3
from botocore.exceptions import ClientError
from pydantic import validator
from pydantic.dataclasses import dataclass
from websocket import WebSocket

import helpers


logger = helpers.create_logger("tiingo")

# -------------------------------- New classes ------------------------------- #


class _Message:
    # Holds the response message from the Tiingo API.
    def __init__(self, raw_message: str):
        self.raw_message: str = raw_message
        self.serialized_message: Dict[str, Any] = json.loads(self.raw_message)
        self.message_type: Literal["I", "H", "A"] = self.serialized_message["messageType"]

    def parse_raw_message(self) -> _SubscriptionMessage | _HeartbeatMessage | _TradeMessage:
        # Convert the raw message into one of three allowable message types.
        Switch = TypedDict(
            "Switch",
            {"I": _SubscriptionMessage, "H": _HeartbeatMessage, "A": _TradeMessage},
        )

        switch: Switch = {
            "I": self.parse_subscription_message(),
            "H": self.parse_heartbeat_message(),
            "A": self.parse_trade_message()
        }

        return switch[self.message_type]

    def parse_subscription_message(self) -> _SubscriptionMessage:
        data = self.serialized_message["data"]
        response = self.serialized_message["response"]
        subscription_id = data["subscription_id"]

        return _SubscriptionMessage(
            subscription_id,
            _Response(response["message"], response["code"])
        )

    def parse_heartbeat_message(self) -> _HeartbeatMessage:
        response = self.serialized_message["response"]

        return _HeartbeatMessage(
            _Response(response["message"], response["code"])
        )

    def parse_trade_message(self) -> _TradeMessage:
        service = self.serialized_message["service"]
        data = self.serialized_message["data"]
        trade_data = TradeData(*data[1:])
        
        return _TradeMessage(service, trade_data)


@dataclass
class _SubscriptionMessage:
    subscription_id: int
    response: _Response


@dataclass
class _Response:
    message: str
    code: int


@dataclass
class _HeartbeatMessage:
    response: _Response


@dataclass
class _TradeMessage:
    service: Literal["crypto_data"]
    data: TradeData


@dataclass
class TradeData:
    """Trade data extracted from the API response message.

    Attibutes:
        ticker (str): The ticker for the crypto asset.
        date (str): The trade execution timestamp.
        exchange (str): The exchange on which the trade took place.
        size (float): The volume done at the last price.
        price (float): The price the trade was executed at.
        processed_at (str): Timestamp when the record was processed by the application.
    """

    ticker: str
    date: str
    exchange: str
    size: float
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


class TiingoSubscribeError(Exception):
    pass


class TiingoClient(WebSocket):
    """Extends WebSocket methods to add Tiingo specific functionality for use
    in connecting to Tiingo WSS APIs.

    Attributes:
      url (str): The API URL.
      token (str): The API auth token.
    """

    def __init__(self, url: str, token: str):
        """Inits the client and subscribes to the specified API endpoint.

        Attributes:
            url (str): The API URL.
            token (str): The API auth token that should be used to subscribe to the API.
        """

        super().__init__()

        self._token: str = token
        self._url: str = url

        self.connect(self._url)
        self._subscribe()

    def _subscribe(self) -> None:
        # Subscribes to the API using the instance auth token.
        subscribe = {
            "eventName": "subscribe",
            "authorization": self._token,
            "eventData": {"thresholdLevel": 5},
        }

        subscription_message = json.dumps(subscribe)
        self.send(subscription_message)

        # Get the subscription response.
        record = json.loads(self.recv())
        message_type = record["messageType"]

        if message_type != "I":
            raise TiingoSubscribeError(
                f"Expected message type 'I' but found '{message_type}'."
            )

        response_message = record["response"]["code"]
        response_code = record["response"]["code"]
        if response_code != 200:
            raise TiingoSubscribeError(
                f"Failed with error code {response_code} and message '{response_message}'."
            )

    def get_next_trade(self) -> TradeData:
        """Returns the next trade message from the API."""
        message_type, message = self._get_next_message()

        while message_type == "H":
            message_type, message = self._get_next_message()

        _, ticker, date, exchange, size, price = message["data"]

        output_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        api_format = "%Y-%m-%dT%H:%M:%S.%f%z"

        formatted_date = self._process_date(date, api_format, output_format)
        now = datetime.strftime(datetime.utcnow(), output_format)

        return TradeData(ticker, formatted_date, exchange, size, price, now)

    def _get_next_message(self) -> Tuple[str, Dict[str, Any]]:
        message = json.loads(self.recv())
        message_type = message["messageType"]

        return (message_type, message)

    @staticmethod
    def _process_date(date_as_string: str, format_in: str, format_out: str) -> str:
        # Trade timestamps returned from the Tiingo API have a standard format
        # except in instances where the microsecond, "%f", part is zero, in
        # which case it is omitted. In this case, the method adds the correct
        # a zero "%f" part to the timestamp.
        try:
            date_as_datetime = datetime.strptime(date_as_string, format_in)
        except ValueError:
            first_part = date_as_string[:19]
            last_part = date_as_string[19:]
            f_part = f".{6 * '0'}"
            date_as_datetime = datetime.strptime(
                f"{first_part}{f_part}{last_part}", format_in
            )

        return datetime.strftime(date_as_datetime, format_out)

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages.
        Attributes:
            size (int): The size of the batch.
        Returns:
            TradeBatch: List of messages from the API.
        """
        batch = (self.get_next_trade() for _ in range(size))
        return TradeBatch(batch)


class TradeBatch:
    """Holds batches of trade updates.

    Attributes:
        batch (Iterable[Trade]): An Iterable of Trades from the API.
    """

    def __init__(self, batch):
        self.batch: Iterable[TradeData] = batch

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
