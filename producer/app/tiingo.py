import gzip
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List

import boto3
from accessify import private
from botocore.exceptions import ClientError
from pydantic import validator
from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder
from websocket import WebSocket


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
class Message:
    pass


@dataclass
class ErrorMessage(Message):
    response_code: int
    response_message: str


@dataclass
class HeartbeatMessage(Message):
    response_code: int
    response_message: str


@dataclass
class SubscriptionMessage(Message):
    subscription_id: int
    response_code: int
    response_message: str


@dataclass
class TradeMessage(Message):
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")

    @validator("date", "processed_at")
    @classmethod
    @private
    def ensure_timestamp(cls, value) -> str:
        """
        Validates the format of timestamps and corrects if malformed. Specifically, if the
        micro-second part of a timestamp is missing, then zeros are added. This occurs in
        instances where a trade was executed when the micro-second part was zero, in which
        case the API removes it completely.
        """
        format_in = "%Y-%m-%dT%H:%M:%S.%f%z"
        format_out = "%Y-%m-%dT%H:%M:%S.%fZ"

        try:
            processed_date = datetime.strptime(value, format_in)
        except ValueError:
            first_part = value[:19]
            last_part = value[19:]
            new_date = f"{first_part}.000000{last_part}"
            processed_date = datetime.strptime(new_date, format_in)

        return datetime.strftime(processed_date, format_out)


class MessageParser(ABC):
    @abstractmethod
    @classmethod
    def parse(cls, message: Dict[str, Any]) -> Message:
        pass


class ErrorMessageParser(MessageParser):
    @classmethod
    def parse(cls, message: Dict[str, Any]) -> ErrorMessage:
        response_code = message["response"]["code"]
        response_message = message["response"]["message"]
        return ErrorMessage(response_code, response_message)


class HeartbeatMessageParser(MessageParser):
    @classmethod
    def parse(cls, message: Dict[str, Any]) -> HeartbeatMessage:
        # Sets instance attributes from the serialized API response.
        response_code = message["response"]["code"]
        response_message = message["response"]["message"]
        return HeartbeatMessage(response_code, response_message)


class SubscriptionMessageParser(MessageParser):
    @classmethod
    def parse(cls, message: Dict[str, Any]) -> SubscriptionMessage:
        # Returns a subscription message class instance from a serialized API message.
        subscription_id = message["data"]["subscriptionId"]
        response_code = message["response"]["code"]
        response_message = message["response"]["message"]
        return SubscriptionMessage(subscription_id, response_code, response_message)


class TradeMessageParser(MessageParser):
    @classmethod
    def parse(cls, message: Dict[str, Any]) -> TradeMessage:
        # Returns a trade update message class instance from a serialized API message.
        _, ticker, date, exchange, size, price = message["data"]
        return TradeMessage(ticker, date, exchange, size, price)


class MessageParserFactory:
    """Used to pick a messgae parser dynamically from the message type."""

    def create(self, message: Dict[str, Any]) -> MessageParser:
        """Returns a parser based on the type of the message provided."""
        match message["messageType"]:
            case "E": parser = ErrorMessageParser
            case "I": parser = SubscriptionMessageParser
            case "H": parser = HeartbeatMessageParser
            case "A": parser = TradeMessageParser

        return parser


def retry(total_retries: int = 3, delay: int = 5) -> Callable:
    """ Retries the function if a 'ServiceUnavailableError' is raised, increasing the time
        between retries as a factor of the number of retries. After the total retries value
        is exceeded, an error is raised.
    """
    def decorator(function: Callable):
        def wrapper(*args, **kwargs) -> None:
            retries = 0
            while retries <= total_retries:
                try:
                    function(*args, **kwargs)
                    break
                except ClientError as e:
                    error_type = e.response["Error"]["Code"]
                    if error_type == "ServiceUnavailableError" and retries < total_retries:
                        retries += 1
                        time.sleep(retries * delay)
                    else:
                        raise e
        return wrapper
    return decorator


class CompressedBatch(bytes):
    def __init__(self, batch: bytes) -> None:
        self.batch = batch

    @retry
    def put_to_kinesis_stream(self, stream: str) -> None:
        """Writes a record to a Kinesis Data Firehose Delivery Stream."""
        firehose_client = boto3.client("firehose")
        put_record = {"Data": self.batch}
        firehose_client.put_record(DeliveryStreamName=stream, Record=put_record)


class TradeBatch:
    """Holds batches of trade updates in a list."""
    def __init__(self, batch: List[TradeMessage]) -> None:
        self.batch = batch

    def compress_batch(self) -> CompressedBatch:
        """
        Converts a list of TradeUpdate dataclasses to a string of new line delimited
        JSON documents and GZIP compresses the result.
        """
        json_strings = [json.dumps(trade, default=pydantic_encoder) for trade in self.batch]
        new_line_delimited_trades = "\n".join(json_strings)
        encoded_trades = new_line_delimited_trades.encode("utf-8")
        compressed_record = gzip.compress(encoded_trades)
        return CompressedBatch(compressed_record)


class TiingoClient(WebSocket):
    """Extends WebSocket methods to add Tiingo specific functionality for use
    in connecting to Tiingo WSS APIs.

    Attributes:
      url (str): The API URL.
      token (str): The API auth token.
    """

    def __init__(self, url: str, token: str):
        super().__init__()
        self.url = url
        self.token = token
        self.subscribe()

    @private
    def make_connection(self) -> None:
        self.connect(self.url)

    @private
    def send_payload(self, payload: str) -> None:
        self.send(payload)

    @private
    def recv_payload(self) -> str:
        return self.recv()

    @private
    def get_next_message(self) -> Message:
        # Returns the next message from the API.
        raw_message = self.recv_payload()
        serialized_message = json.loads(raw_message)
        factory = MessageParserFactory()
        parser = factory.create_message_parser(serialized_message)
        next_message = parser.parse(serialized_message)

        if isinstance(next_message, ErrorMessage):
            raise TiingoMessageError(next_message.response_code, next_message.response_message)
        
        return next_message

    @private
    def validate_subscription(self) -> None:
        # Returns the subscription response from the API after a subscription message has
        # been sent to the API.
        try:
            self._get_next_message()
        except TiingoMessageError as e:
            raise TiingoSubscriptionError(e.code, e.message)

    @private
    def subscribe(self) -> None:
        # Subscribes to the API using the instance auth token.
        subscribe = {
            "eventName": "subscribe",
            "authorization": self.token,
            "eventData": {
                "thresholdLevel": 5
            },
        }

        self.make_connection(self._url)
        self.send_payload(json.dumps(subscribe))
        self.validate_subscription()

    def get_next_trade(self) -> TradeMessage:
        """Returns the next trade message from the API."""
        message = self.get_next_message()

        while not isinstance(message, TradeMessage):
            message = self.get_next_message()

        return message

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages."""
        batch = [self.get_next_trade() for _ in range(size)]
        return TradeBatch(batch)
