from __future__ import annotations

import gzip
import json
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List

import boto3
from botocore.exceptions import ClientError
from websocket import WebSocket

import helpers


logger = helpers.create_logger("tiingo")


class _BaseMessage(ABC):
    # Holds the response message from the Tiingo API.
    def __init__(self, raw_message: str) -> None:
        self._raw_message = raw_message
        self._load()

    def _load(self) -> None:
        # Loads the JSON string as a dictionary.
        self.serialized_message = json.loads(self._raw_message)

    @abstractmethod
    def parse(self) -> None:
        pass


class _SubscriptionMessage(_BaseMessage):
    # Represents a subscription confirmation response from the API.
    def __init__(self, raw_message: str) -> None:
        super().__init__(raw_message)
        self.parse()
    
    def parse(self) -> None:
        # Sets class attributes from the serialized API response.
        data = self.serialized_message["data"]
        response = self.serialized_message["response"]
        self.subscription_id = data["subscriptionId"]
        self.response = _Response(response["message"], response["code"])


class _HeartbeatMessage(_BaseMessage):
    # Represents a heartbeat response from the API.
    def __init__(self, raw_message: str) -> None:
        super().__init__(raw_message)
        self.parse()

    def parse(self) -> None:
        # Sets class attributes from the serialized API response.
        response = self.serialized_message["response"]
        self.response = _Response(response["message"], response["code"])


class _TradeMessage(_BaseMessage):
    # Represents a trade update response from the API.
    def __init__(self, raw_message: str) -> None:
        super().__init__(raw_message)
        self.parse()

    def parse(self) -> None:
        # Sets class attributes from the serialized API response.
        _, ticker, date, exchange, size, price = self.serialized_message["data"]
        
        date_format_in = "%Y-%m-%dT%H:%M:%S.%f%z"
        date_format_out = "%Y-%m-%dT%H:%M:%S.%fZ"
        formatted_date = self._ensure_date(date, date_format_in, date_format_out)
        
        self.service = self.serialized_message["service"]
        self.trade_data = TradeData(
            ticker,
            formatted_date,
            exchange,
            size,
            price
        )

    @staticmethod
    def _ensure_date(date_as_string: str, format_in: str, format_out: str) -> str:
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


@dataclass
class _Response:
    message: str
    code: int


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

    now = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")

    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = now


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

        # Get the subscription response.
        subscription_message = _SubscriptionMessage(self.recv())
        if subscription_message.response.code != 200:
            raise TiingoSubscribeError(
                f"""Failed with error code {subscription_message.response.code} 
                and message '{subscription_message.response.message}'."""
            )

    def get_next_trade(self) -> TradeData:
        """Returns the next trade message from the API."""
        message = self._get_next_message()
        
        while isinstance(message, _HeartbeatMessage):
            message = self._get_next_message()

        return message.trade_data

    def _get_next_message(self) -> _TradeMessage | _HeartbeatMessage:
        # Gets the next message from the API, either trade update or heartbeat.
        raw_message = self.recv()
        try:
            return _TradeMessage(raw_message)
        except KeyError:
            return _HeartbeatMessage(raw_message)

    def get_next_batch(self, size: int) -> TradeBatch:
        """Creates a batch of trade update messages.
        Attributes:
            size (int): The size of the batch.
        Returns:
            TradeBatch: List of messages from the API.
        """
        batch = [self.get_next_trade() for _ in range(size)]
        return TradeBatch(batch)


class TradeBatch:
    """Holds batches of trade updates.

    Attributes:
        batch (Iterable[Trade]): An Iterable of Trades from the API.
    """

    def __init__(self, batch: List[TradeData]):
        self.batch = batch

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
