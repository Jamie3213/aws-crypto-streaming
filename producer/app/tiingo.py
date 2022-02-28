import gzip
import json
import time
from datetime import datetime
from enum import auto, Enum
from typing import Any, Callable, Dict, List

from botocore.exceptions import ClientError
from pydantic import validator
from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder
from websocket import WebSocket

from aws_helpers import _put_record_to_kinesis_stream


class TiingoMessageError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"Failed to get next API message with error {self.code}: '{self.message}'.")


class TiingoSubscriptionError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"API subscription failed with error {self.code}: '{self.message}'.")


@dataclass
class _Message:
    pass


@dataclass
class _ErrorMessage(_Message):
    response_code: int
    response_message: str


@dataclass
class _HeartbeatMessage(_Message):
    response_code: int
    response_message: str


@dataclass
class _SubscriptionMessage(_Message):
    subscription_id: int
    response_code: int
    response_message: str


@dataclass
class TradeMessage(_Message):
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")

    @validator("date", "processed_at")
    @classmethod
    def _ensure_timestamp(cls, value) -> str:
        """
        Validates the format of timestamps and corrects if malformed. Specifically, if the
        micro-second part of a timestamp is missing, then zeros are added. This occurs in
        instances where a trade was executed when the micro-second part was zero, in which
        case the API removes it completely.
        """
        FORMAT_IN = "%Y-%m-%dT%H:%M:%S.%f%z"
        FORMAT_OUT = "%Y-%m-%dT%H:%M:%S.%fZ"

        try:
            processed_date = datetime.strptime(value, FORMAT_IN)
        except ValueError:
            first_part = value[:19]
            last_part = value[19:]
            new_date = f"{first_part}.000000{last_part}"
            processed_date = datetime.strptime(new_date, FORMAT_IN)

        return datetime.strftime(processed_date, FORMAT_OUT)


class MessageType(Enum):
    ERROR = auto()
    HEARTBEAT = auto()
    SUBSCRIPTION = auto()
    TRADE = auto()


def _message_type_to_enum(message_type: str) -> MessageType:
    type_map = {
        "E": MessageType.ERROR,
        "I": MessageType.SUBSCRIPTION,
        "H": MessageType.HEARTBEAT,
        "A": MessageType.TRADE
    }
    return type_map[message_type]

def _parse_error_message(message: Dict[str, Any])-> _ErrorMessage:
    response_code = message["response"]["code"]
    response_message = message["response"]["message"]
    return _ErrorMessage(response_code, response_message)

def _parse_heartbeat_message(message: Dict[str, Any]) -> _HeartbeatMessage:
    # Sets instance attributes from the serialized API response.
    response_code = message["response"]["code"]
    response_message = message["response"]["message"]
    return _HeartbeatMessage(response_code, response_message)

def _parse_subscription_message(message: Dict[str, Any]) -> _SubscriptionMessage:
    # Returns a subscription message class instance from a serialized API message.
    subscription_id = message["data"]["subscriptionId"]
    response_code = message["response"]["code"]
    response_message = message["response"]["message"]
    return _SubscriptionMessage(subscription_id, response_code, response_message)

def _parse_trade_message(message: Dict[str, Any]) -> TradeMessage:
    # Returns a trade update message class instance from a serialized API message.
    _, ticker, date, exchange, size, price = message["data"]
    return TradeMessage(ticker, date, exchange, size, price)

def _create_message_parser(message: Dict[str, Any]) -> Callable[[Dict[str, Any]], _Message]:
    """Returns a parser based on the type of the message provided."""
    message_type = message["messageType"]
    factory_map = {
        MessageType.ERROR: _parse_error_message,
        MessageType.SUBSCRIPTION: _parse_subscription_message,
        MessageType.HEARTBEAT: _parse_heartbeat_message,
        MessageType.TRADE: _parse_trade_message
    }
    return factory_map[_message_type_to_enum(message_type)]

    
class RecordWriter:
    """Contains functionality to write a record to a destination, e.g. Kinesis Data Firehose."""

    def __init__(self, record: bytes) -> None:
        self.record = record

    def put_to_kinesis_stream(self, stream: str, total_retries: int) -> None:
        """
        Puts records to the Kinesis Firehose Stream. If a service error occurs,
        then the put is retried after a delay.
        """
        retries = 0
        while retries < total_retries:
            try:
                _put_record_to_kinesis_stream(self.record, stream)
                break
            except ClientError as e:
                error_type = e.response["Error"]["Code"]
                if error_type == "ServiceUnavailableError" and retries < total_retries:
                    retries += 1
                    time.sleep(5)
                else:
                    raise e


class TradeBatch:
    """Holds batches of trade updates in a list."""
    def __init__(self, batch: List[TradeMessage]) -> None:
        self.trades = batch

    def compress_batch(self) -> RecordWriter:
        """
        Converts a list of TradeUpdate dataclasses to a string of new line delimited
        JSON documents and GZIP compresses the result.
        """
        json_strings = [json.dumps(trade, default=pydantic_encoder) for trade in self.trades]
        new_line_delimited_trades = "\n".join(json_strings)
        encoded_trades = new_line_delimited_trades.encode("utf-8")
        compressed_record = gzip.compress(encoded_trades)
        return RecordWriter(compressed_record)


class TiingoClient(WebSocket):
    """Extends WebSocket methods to add Tiingo specific functionality for use
    in connecting to Tiingo WSS APIs.

    Attributes:
      url (str): The API URL.
      token (str): The API auth token.
    """

    def __init__(self, url: str, token: str):
        super().__init__()
        self._url = url
        self._token = token
        self._subscribe()

    def _connect(self, url: str) -> None:
        self.connect(url)

    def _send_payload(self, payload: str) -> None:
        self.send(payload)

    def _recv_payload(self) -> str:
        return self.recv()

    def _get_next_message(self) -> _Message:
        # Returns the next message from the API.
        raw_message = self._recv_payload()
        serialized_message = json.loads(raw_message)
        parser = _create_message_parser(serialized_message)
        next_message = parser(serialized_message)

        if isinstance(next_message, _ErrorMessage):
            raise TiingoMessageError(next_message.response_code, next_message.response_message)
        
        return next_message

    def _validate_subscription(self) -> None:
        # Returns the subscription response from the API after a subscription message has
        # been sent to the API.
        try:
            self._get_next_message()
        except TiingoMessageError as e:
            raise TiingoSubscriptionError(e.code, e.message)

    def _subscribe(self) -> None:
        # Subscribes to the API using the instance auth token.
        subscribe = {
            "eventName": "subscribe",
            "authorization": self._token,
            "eventData": {
                "thresholdLevel": 5
            },
        }

        self._connect(self._url)
        self._send_payload(json.dumps(subscribe))
        self._validate_subscription()

    def get_next_trade(self) -> TradeMessage:
        """Returns the next trade message from the API."""
        message = self._get_next_message()

        while not isinstance(message, TradeMessage):
            message = self._get_next_message()

        return message

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages."""
        batch = [self.get_next_trade() for _ in range(size)]
        return TradeBatch(batch)
