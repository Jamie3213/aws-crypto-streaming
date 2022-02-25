from __future__ import annotations

import gzip
import json
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List

from botocore.exceptions import ClientError
from websocket import WebSocket

from helpers import _put_record_to_kinesis_stream, _ensure_date
from logger import create_logger

logger = create_logger("tiingo")


class _MessageParser(ABC):
    # Abstract base class defining a parser class from API messages.
    def __init__(self, message: Dict[Any]) -> None:
        self.message = message

    @abstractmethod
    def parse(self) -> None:
        pass


class ErrorMessageParser(_MessageParser):
    """Represents an error response from the API."""

    def __init__(self, message: Dict[Any]) -> None:
        super().__init__(message)

    def parse(self) -> None:
        response = self.serialized_message["response"]
        code = response["code"]
        message = response["message"]
        raise TiingoClientError(
            f"API call failed with error code {code} and error message '{message}'"
        )


class HeartbeatMessageParser(_MessageParser):
    """Represents a heartbeat response from the API."""

    def __init__(self, message: Dict[Any]) -> None:
        super().__init__(message)

    def parse(self) -> HeartbeatMessage:
        # Sets instance attributes from the serialized API response.
        response = self.serialized_message["response"]
        return HeartbeatMessage(response["code"], response["message"])


@dataclass
class HeartbeatMessage:
    response_code: int
    response_message: str


class SubscriptionMessageParser(_MessageParser):
    """Represents a subscription confirmation response from the API."""

    def __init__(self, message: Dict[Any]) -> None:
        super().__init__(message)

    def parse(self) -> SubscriptionMessage:
        # Sets instance attributes from the serialized API response.
        data = self.serialized_message.get("data")
        response = self.serialized_message["response"]
        return SubscriptionMessage(
            data["subscriptionId"], response["code"], response["message"]
        )


@dataclass
class SubscriptionMessage:
    subscription_id: int
    response_code: int
    response_message: str


class TradeMessageParser(_MessageParser):
    """Represents a trade update response from the API."""

    def __init__(self, message: Dict[Any]) -> None:
        super().__init__(message)

    def parse(self) -> TradeMessage:
        # Sets instance attributes from the serialized API response.
        _, ticker, date, exchange, size, price = self.serialized_message["data"]

        date_format_in = "%Y-%m-%dT%H:%M:%S.%f%z"
        date_format_out = "%Y-%m-%dT%H:%M:%S.%fZ"
        formatted_date = _ensure_date(date, date_format_in, date_format_out)

        service = self.serialized_message["service"]
        trade_data = TradeData(ticker, formatted_date, exchange, size, price)
        return TradeMessage(service, trade_data)


@dataclass
class TradeMessage:
    service: str
    trade_data: TradeData


@dataclass
class TradeData:
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")


class MessageParserFactory:
    """Factory to create message parsers from API messages..

    Attributes:
        raw_message (str): The raw string response returned from the API.
    """

    def __init__(self, raw_message: str) -> None:
        self._raw_message = raw_message
        self._load()
        self._set_type()

    def _load(self) -> None:
        # Loads the raw JSON string into a dictionary
        self._serialized_message = json.loads(self._raw_message)

    def _set_type(self) -> None:
        # Extracts the message type from the serialize message.
        self._message_type = self.serialized_message["messageType"]

    def get_parser(self) -> _MessageParser:
        """Uses the message type to determine the correct message sub-class to instantiate."""
        factory = {
            "E": ErrorMessageParser,
            "H": HeartbeatMessageParser,
            "I": SubscriptionMessageParser,
            "A": TradeMessageParser,
        }
        parser = factory[self._message_type]
        return parser(self._serialized_message)


class TiingoClientError(Exception):
    pass


class TiingoClient(WebSocket):
    """Extends WebSocket methods to add Tiingo specific functionality for use
    in connecting to Tiingo WSS APIs.

    Attributes:
      url (str): The API URL.
      token (str): The API auth token.
    """

    def __init__(self, url: str, token: str):
        super().__init__()
        self._token = token
        self._url = url
        self.connect(self._url)
        self._subscribe()

    def _subscribe(self) -> None:
        # Subscribes to the API using the instance auth token.
        subscribe = {
            "eventName": "subscribe",
            "authorization": self._token,
            "eventData": {"thresholdLevel": 5},
        }

        self.send(json.dumps(subscribe))

        # Get the subscription response to make sure no errors occurred.
        raw_message = self.recv()
        MessageParserFactory(raw_message).get_parser().parse()

    def get_next_trade(self) -> TradeData:
        """Returns the next trade message from the API."""
        message = self._get_next_message()

        while isinstance(message, HeartbeatMessage):
            message = self._get_next_message()

        return message.trade_data

    def _get_next_message(self) -> HeartbeatMessage | TradeMessage:
        # Gets the next message from the API.
        raw_message = self.recv()
        next_message = MessageParserFactory(raw_message) \
            .get_parser() \
            .parse()
            
        return next_message

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages."""
        batch = [self.get_next_trade() for _ in range(size)]
        return TradeBatch(batch)


class TradeBatch:
    """Holds batches of trade updates in a list.
    
    Attributes:
        batch (List[TradeData]): Batch of trade messages as a list of TradeData classes.
    """

    def __init__(self, batch: List[TradeData]):
        self.batch = batch

    def compress_batch(self) -> RecordWriter:
        """
        Converts a list of TradeUpdate dataclasses to a string of new line delimited
        JSON documents and GZIP compresses the result.
        """
        trade_dicts = [asdict(message) for message in self.batch]
        json_strings = [json.dumps(dict_) for dict_ in trade_dicts]
        new_line_delimited = "\n".join(json_strings) + "\n"
        batch_bytes = new_line_delimited.encode("utf-8")
        compressed_record = gzip.compress(batch_bytes)
        return RecordWriter(compressed_record)


class RecordWriter:
    """Contains functionality to write a record to a destination, e.g. Kinesis Data Firehose."""

    def __init__(self, record: bytes) -> None:
        self.record = record

    def put_to_kinesis_stream(self, stream: str) -> None:
        """
        Puts records to the Kinesis Firehose Stream. If a service error occurs,
        then the put is retried after a delay.
        """
        retries = 0
        total_retries = 3
        while retries < total_retries:
            try:
                _put_record_to_kinesis_stream(self.record, stream)
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
