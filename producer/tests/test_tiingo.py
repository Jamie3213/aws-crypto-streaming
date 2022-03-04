import gzip
import json
import sys
import unittest

sys.path.append("..")

from typing import Any, Dict
from unittest.mock import MagicMock, patch

from app.tiingo import (
    TiingoClient,
    TiingoBatchSizeError,
    TiingoMessageError,
    TiingoSubscriptionError,
    TradeBatch,
    TradeMessage,
)
from freezegun import freeze_time
from moto import mock_firehose

import data

MOCK_NOW = "2022-03-04T12:34:48.648888"


class TestTiingoClient(unittest.TestCase):
    """Tests public API of the tiingo module."""

    url = "ws://test.com/test"
    token = "test_api_token"

    error_record: Dict[str, Any] = data.error
    heartbeat_record: Dict[str, Any] = data.heartbeat
    subscription_record: Dict[str, Any] = data.subscription
    trade_record: Dict[str, Any] = data.trade

    @patch.object(TiingoClient, "_get_record")
    @patch.object(TiingoClient, "_send_payload")
    @patch.object(TiingoClient, "_make_connection")
    def test_should_raise_subscription_error(
        self,
        mock_make_connection: MagicMock,
        mock_send_payload: MagicMock,
        mock_get_record: MagicMock,
    ) -> None:
        """Ensure that an exception is raised if the initial API subscription fails."""

        mock_make_connection.return_value = None
        mock_send_payload.return_value = None
        mock_get_record.return_value = self.error_record

        self.assertRaises(TiingoSubscriptionError, TiingoClient, self.url, self.token)

    @patch.object(TiingoClient, "_get_record")
    @patch.object(TiingoClient, "_subscribe")
    def test_should_raise_message_error(
        self, mock_subscribe: MagicMock, mock_get_record: MagicMock
    ) -> None:
        """Ensure that an exception is raised if an error response is received from the API."""

        mock_subscribe.return_value = None
        mock_get_record.return_value = self.error_record

        client = TiingoClient(self.url, self.token)

        self.assertRaises(TiingoMessageError, client.get_next_trade)

    @patch.object(TiingoClient, "_get_record")
    @patch.object(TiingoClient, "_subscribe")
    def test_should_instantiate_trade_message(
        self, mock_subscribe: MagicMock, mock_get_record: MagicMock
    ) -> None:
        """Ensure that trades are instantiated correctly from the factory."""

        mock_subscribe.return_value = None
        mock_get_record.side_effect = (
            self.subscription_record,
            self.heartbeat_record,
            self.trade_record,
        )

        client = TiingoClient(self.url, self.token)
        next_trade = client.get_next_trade()

        self.assertIsInstance(next_trade, TradeMessage)

    @freeze_time(MOCK_NOW)
    @patch.object(TiingoClient, "_get_record")
    @patch.object(TiingoClient, "_subscribe")
    def test_should_correctly_parse_trade_messages(
        self,
        mock_subscribe: MagicMock,
        mock_get_record: MagicMock,
    ) -> None:
        """Ensure that trades correctly parsed into the TradeMessage class."""

        mock_subscribe.return_value = None
        mock_get_record.return_value = self.trade_record

        dummy_trade_data = self.trade_record["data"]

        ticker = dummy_trade_data[1]
        date = dummy_trade_data[2]
        exchange = dummy_trade_data[3]
        size = dummy_trade_data[4]
        price = dummy_trade_data[5]

        expected_trade = TradeMessage(ticker, date, exchange, size, price)
        client = TiingoClient(self.url, self.token)
        actual_trade = client.get_next_trade()

        self.assertEqual(actual_trade, expected_trade)

    @patch.object(TiingoClient, "_subscribe")
    def test_should_raise_batch_size_error(self, mock_subscribe: MagicMock) -> None:
        """Ensures that a exception is raised when a batch size less than 1 is requested."""

        mock_subscribe.return_value = None
        client = TiingoClient(self.url, self.token)

        self.assertRaises(TiingoBatchSizeError, client.get_next_batch, 0)

    @patch.object(TiingoClient, "get_next_trade")
    @patch.object(TiingoClient, "_subscribe")
    def test_should_instantiate_trade_batch(
        self, mock_subscribe: MagicMock, mock_get_next_trade: MagicMock
    ) -> None:
        """Ensure that trade batches are correctly instantiated."""

        mock_subscribe.return_value = None
        mock_get_next_trade.side_effect = (
            TradeMessage("BTC", "2022-01-01T00:00:00+00:00", "Bitfenix", 123.4, 567.8),
            TradeMessage("BTC", "2022-01-01T00:01:13+00:00", "Binance", 345.6, 789.10),
        )

        client = TiingoClient(self.url, self.token)
        batch = client.get_next_batch(2)

        self.assertIsInstance(batch, TradeBatch)

    @patch.object(TiingoClient, "get_next_trade")
    @patch.object(TiingoClient, "_subscribe")
    def test_should_create_correct_length_batch(
        self, mock_subscribe: MagicMock, mock_get_next_trade: MagicMock
    ) -> None:
        """Ensure that trade batches are correctly instantiated."""

        mock_subscribe.return_value = None
        mock_get_next_trade.side_effect = (
            TradeMessage("BTC", "2022-01-01T00:00:00+00:00", "Bitfenix", 123.4, 567.8),
            TradeMessage("BTC", "2022-01-01T00:01:13+00:00", "Binance", 345.6, 789.10),
        )

        client = TiingoClient(self.url, self.token)
        batch = client.get_next_batch(2)

        self.assertEqual(len(batch), 2)


class TestTradeBatch(unittest.TestCase):
    @freeze_time(MOCK_NOW)
    def test_should_compress_batch(self) -> None:
        """Ensure that trade batches are correctly compressed to byte objects."""

        client = MagicMock()
        trade_messages = (
            TradeMessage("BTC", "2022-01-01T00:00:00+00:00", "Bitfenix", 123.4, 567.8),
            TradeMessage("BTC", "2022-01-01T00:01:13+00:00", "Binance", 345.6, 789.10),
        )
        client.get_next_batch.return_value = TradeBatch([*trade_messages])

        strings = (
            json.dumps(
                {
                    "ticker": "BTC",
                    "date": "2022-01-01T00:00:00.000000Z",
                    "exchange": "Bitfenix",
                    "size": 123.4,
                    "price": 567.8,
                    "processed_at": f"{MOCK_NOW}Z",
                }
            ),
            "\n",
            json.dumps(
                {
                    "ticker": "BTC",
                    "date": "2022-01-01T00:01:13.000000Z",
                    "exchange": "Binance",
                    "size": 345.6,
                    "price": 789.10,
                    "processed_at": f"{MOCK_NOW}Z",
                }
            ),
        )
        expected_string = "".join(strings)

        batch: TradeBatch = client.get_next_batch(2)
        compressed_batch = batch.compress()
        decompressed_batch = gzip.decompress(compressed_batch)
        actual_string = decompressed_batch.decode("utf-8")

        self.assertEqual(actual_string, expected_string)


class TestCompressedTradeBatch(unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
