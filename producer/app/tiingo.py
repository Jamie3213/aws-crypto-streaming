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


class TiingoClientError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"Failed to get next API message with error {self.code}: '{self.message}'.")


class TiingoSubscriptionError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"API subscription failed with error {self.code}: '{self.message}'.")


RawTiingoRecord = str
SerializedTiingoRecord = Dict[str, Any]


@dataclass
class NonTradeMessage:
    code: int
    message: int

    def raise_for_status(self) -> None:
        if self.code != 200:
            raise TiingoClientError(self.code, self.message)


@dataclass
class TradeMessage:
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")

    @validator("date")
    @classmethod
    @private
    def ensure_timestamp(cls, timestamp) -> str:
        naked_timestamp = str.replace(timestamp, "+00:00", "")

        match len(naked_timestamp):
            case 32: corrected_timestamp = f"{naked_timestamp}Z"
            case _: corrected_timestamp = f"{naked_timestamp}.000000Z"

        return corrected_timestamp


Message = NonTradeMessage | TradeMessage
    

class MessageParser(ABC):
    @classmethod
    @abstractmethod
    def parse(cls, record: SerializedTiingoRecord) -> Message:
        pass


class NonTradeMessageParser(MessageParser):
    @classmethod
    def parse(cls, record: SerializedTiingoRecord) -> NonTradeMessage:
        code = record.serialized["response"]["code"]
        message = record.serialized["response"]["message"]
        return NonTradeMessage(code, message)


class TradeMessageParser(MessageParser):
    @classmethod
    def parse(cls, record: SerializedTiingoRecord) -> TradeMessage:
        # Returns a trade update message class instance from a serialized API message.
        _, ticker, date, exchange, size, price = record.serialized["data"]
        return TradeMessage(ticker, date, exchange, size, price)


class MessageParserFactory:
    """Used to pick a message parser dynamically from the message type."""

    def create(self, record: SerializedTiingoRecord) -> MessageParser:
        """Returns a parser based on the type of the message provided."""
        message_type = record.serialized["messageType"]

        match message_type:
            case ["E", "I", "H"]: parser = NonTradeMessageParser
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
    """
    Extends WebSocket methods to add Tiingo specific functionality for use
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
    def recv_payload(self) -> RawTiingoRecord:
        return self.recv()

    @private
    def get_next_message(self) -> Message:
        # Returns the next message from the API.
        response = self.recv_payload()
        factory = MessageParserFactory()
        parser = factory.create(response)
        next_message = parser.parse(response)
        return next_message

    @private
    def validate_subscription(self) -> None:
        # Returns the subscription response from the API after a subscription message has
        # been sent to the API.
        try:
            self.get_next_message()
        except TiingoClientError as e:
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

        while isinstance(message, NonTradeMessage):
            message = self.get_next_message()

        return message

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages."""
        batch = [self.get_next_trade() for _ in range(size)]
        return TradeBatch(batch)
