import sys
import unittest

sys.path.append("..")

from typing import Any, Dict
from unittest.mock import MagicMock, patch

from app.tiingo import (
    TiingoClient,
    TradeMessage,
    TiingoMessageError,
    TiingoSubscriptionError,
)

import data


class TestTiingoClient(unittest.TestCase):
    """Tests public API of the tiingo module."""

    url = "ws://test.com/test"
    token = "test_api_token"

    error_record: Dict[str, Any] = data.error
    heartbeat_record: Dict[str, Any] = data.heartbeat
    subscription_record: Dict[str, Any] = data.subscription
    trade_record: Dict[str, Any] = data.trade


    @patch("app.tiingo.TiingoClient._get_record")
    @patch("app.tiingo.TiingoClient._send_payload")
    @patch("app.tiingo.TiingoClient._make_connection")
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

    @patch("app.tiingo.TiingoClient._get_record")
    @patch("app.tiingo.TiingoClient._subscribe")
    def test_should_raise_message_error(
        self, mock_subscribe: MagicMock, mock_get_record: MagicMock
    ) -> None:
        """Ensure that trades are instantiated correctly from the factory."""

        mock_subscribe.return_value = None
        mock_get_record.return_value = self.error_record

        client = TiingoClient(self.url, self.token)

        self.assertRaises(TiingoMessageError, client.get_next_trade)

    @patch("app.tiingo.TiingoClient._get_record")
    @patch("app.tiingo.TiingoClient._subscribe")
    def test_should_instantiate_trade_message(
        self, mock_subscribe: MagicMock, mock_get_record: MagicMock
    ) -> None:
        """Ensure that trades are instantiated correctly from the factory."""

        mock_subscribe.return_value = None
        mock_get_record.side_effect = (self.subscription_record, self.heartbeat_record, self.trade_record)

        client = TiingoClient(self.url, self.token)
        next_trade = client.get_next_trade()

        self.assertIsInstance(next_trade, TradeMessage)

    
    @patch("app.tiingo.TiingoClient._get_record")
    @patch("app.tiingo.TiingoClient._subscribe")
    def test_should_correctly_parse_trade_messages(
        self, mock_subscribe: MagicMock, mock_get_record: MagicMock
    ) -> None:
        """Ensure that trades are instantiated correctly from the factory."""

        mock_subscribe.return_value = None
        mock_get_record.return_value = self.trade_record

        dummy_trade_data = self.trade_record["data"]

        ticker = dummy_trade_data[1]
        date = dummy_trade_data[2]
        exchange = dummy_trade_data[3]
        size = dummy_trade_data[4]
        price = dummy_trade_data[5]

        expected_values = (ticker, f"{str.replace(date, '+00:00', '')}.000000Z", exchange, size, price)

        client = TiingoClient(self.url, self.token)
        trade = client.get_next_trade()
        actual_values = (trade.ticker, trade.date, trade.exchange, trade.size, trade.price)

        self.assertEqual(actual_values, expected_values)


if __name__ == "__main__":
    unittest.main()
