import json

from websocket import WebSocket

from .batches import TradeBatch
from .data_structs import Message, TiingoRecord, TradeMessage
from .exceptions import (
    TiingoBatchSizeError,
    TiingoClientError,
    TiingoMessageError,
    TiingoSubscriptionError,
)
from .parsers import MessageParserFactory


class TiingoClient(WebSocket):
    """
    Extends WebSocket methods to add Tiingo specific functionality for use
    in connecting to Tiingo WSS APIs.
    """

    def __init__(self, url: str, token: str):
        super().__init__()

        self.url = url
        self.token = token

        self._subscribe()

    def _make_connection(self) -> None:
        self.connect(self.url)

    def _send_payload(self, payload: str) -> None:
        self.send(payload)

    def _get_record(self) -> TiingoRecord:
        raw_record = self.recv()
        return json.loads(raw_record)

    def _get_next_message(self) -> Message:
        # Returns the next message from the API.
        response = self._get_record()
        factory = MessageParserFactory()
        parser = factory.create(response)
        next_message = parser.parse(response)

        try:
            next_message.raise_for_status()
        except TiingoClientError as e:
            raise TiingoMessageError(e.code, e.message) from None

        return next_message

    def _validate_subscription(self) -> None:
        # Returns the subscription response from the API after a subscription message has
        # been sent to the API.
        try:
            self._get_next_message()
        except TiingoMessageError as e:
            raise TiingoSubscriptionError(e.code, e.message) from None

    def _subscribe(self) -> None:
        """Subscribes to the API using the instance auth token."""
        subscribe = {
            "eventName": "subscribe",
            "authorization": self.token,
            "eventData": {"thresholdLevel": 5},
        }

        self._make_connection()
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
        if size < 1:
            raise TiingoBatchSizeError(size)
        else:
            batch = [self.get_next_trade() for _ in range(size)]

        return TradeBatch(batch)
