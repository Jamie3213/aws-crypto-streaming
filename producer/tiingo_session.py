from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from websocket import WebSocket


class TiingoSession(WebSocket):
    """A websocket session connecting to the Tiingo crypto API.

    Attributes:
      token (str): The API auth token.
    """

    url = "wss://api.tiingo.com/crypto"
    ws = WebSocket()

    ws.connect(url)

    def __init__(self, token: str) -> None:
        """Inits TiingoSession with the specified API auth token."""
        self.token = token

    def subscribe_and_flush(self) -> TiingoSession:
        """Subscribes to the crypto API and fluses initial responses."""
        subscribe = {
            "eventName": "subscribe",
            "authorization": self.token,
            "eventData": {"thresholdLevel": 5},
        }

        message = json.dumps(subscribe)
        self.ws.send(message)

        # Get the first API message
        response = json.loads(self.ws.recv())["response"]
        response_code = response["code"]
        response_message = response["message"]

        if response_code != 200:
            error = f"Subscription failed with error code {response_code} and message '{response_message}'."
            raise WebSocketSubscribeError(error)

        # Get the heartbeat response
        next_response = json.loads(self.ws.recv())["response"]
        next_response_code = next_response["code"]
        next_response_message = next_response["message"]

        if next_response_code != 200:
            error = f"Subscription failed with error code {next_response_code} and message '{next_response_message}'."
            raise WebSocketSubscribeError(error)

        return self

    def next_trade(self) -> TradeUpdateMessage:
        """Returns the next trade from the API."""
        data, update_type = self.__get_response()

        # Only return trades, not top-of-book quotes
        while update_type != "T":
            data, update_type = self.__get_response()

        return self.__process_response_data(data)

    def __get_response(self) -> Tuple[List[str], str]:
        """Returns the next API response and the update type."""
        response = self.ws.recv()
        record = json.loads(response)
        data = record["data"]
        update_type = data[0]

        return (data, update_type)

    @staticmethod
    def __process_response_data(data: List[str]) -> TradeUpdateMessage:
        """Creates a TradeUpdateMessage dataclass from a data list.

        Attributes:
          data (List[str]): Data array returned from the Tiingo API.

        Returns:
          TradeUpdateMessage: Dataclass to hold trade update data.
        """
        ticker = data[1]
        date = data[2]
        price = data[5]

        now = datetime.utcnow()
        output_date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        api_date_format = "%Y-%m-%dT%H:%M:%S.%f%z"

        # Format trade date - when the microsecond '%f' part of the timesatemp is
        # zero, the API omits it which causes issues when we try to convert the
        # string into a datetime object.
        try:
            date_dt = datetime.strptime(date, api_date_format)
        except ValueError:
            first_part = date[:19]
            last_part = date[19:]
            f_part = ".000000"
            date_dt = datetime.strptime(
                f"{first_part}{f_part}{last_part}", api_date_format
            )

        return TradeUpdateMessage(
            ticker,
            datetime.strftime(date_dt, output_date_format),
            float(price),
            datetime.strftime(now, output_date_format),
        )


class WebSocketSubscribeError(Exception):
    pass


@dataclass
class TradeUpdateMessage:
    """Trade update from the Tiingo crypto API.

    Attributes:
        ticker (str): The asset ticker.
        date (str): The trade timestamp.
        price (str): The price the trade was executed at.
        processed_at (str): Application timestamp.
    """

    ticker: str
    date: str
    price: float
    processed_at: str
