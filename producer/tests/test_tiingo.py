import os
import sys


parent = os.path.abspath(".")
sys.path.append(os.path.join(parent, "producer", "app"))


import unittest

from tiingo import TiingoClient, TiingoSubscribeError
from unittest.mock import MagicMock, patch


class TestTiingoClient(unittest.TestCase):
    @patch("tiingo.TiingoClient.recv")
    @patch("tiingo.TiingoClient.send")
    @patch("tiingo.TiingoClient.connect")
    def test_should_correctly_parse_trade_data(
        self, mock_connect: MagicMock, mock_send: MagicMock, mock_recv: MagicMock
    ) -> None:
        raw_subscription_message = '{"messageType": "I", "response": {"message": "Success", "code": 200}, "data": {"subscriptionId": 1234}}'
        raw_heartbeat_message = (
            '{"messageType": "H", "response": {"message": "Success", "code": 200}}'
        )
        raw_trade_message = '{"messageType": "A", "service": "crypto_data", "data": ["A", "BTC", "2022-01-01T00:00:00+00:00", "Bitfenix", 123.4, 567.8]}'

        mock_connect.return_value = None
        mock_send.return_value = 0
        mock_recv.side_effect = (
            raw_subscription_message,
            raw_heartbeat_message,
            raw_trade_message,
        )

        url = "ws://test.com/test"
        token = "test_auth_token"
        client = TiingoClient(url, token)
        next_trade = client.get_next_trade()

        trade_values = (
            next_trade.ticker,
            next_trade.date,
            next_trade.exchange,
            next_trade.size,
            next_trade.price,
        )

        expected_values = (
            "BTC",
            "2022-01-01T00:00:00.000000Z",
            "Bitfenix",
            123.4,
            567.8,
        )

        self.assertEqual(trade_values, expected_values)
    
    @patch("tiingo.TiingoClient.recv")
    @patch("tiingo.TiingoClient.send")
    @patch("tiingo.TiingoClient.connect")
    def test_should_raise_subscription_error(
        self, mock_connect: MagicMock, mock_send: MagicMock, mock_recv: MagicMock
    ) -> None:
        raw_subscription_message = '{"messageType": "I", "response": {"message": "Failure", "code": 500}, "data": {"subscriptionId": 1234}}'
        mock_connect.return_value = None
        mock_send.return_value = 0
        mock_recv.return_value = raw_subscription_message

        url = "ws://test.com/test"
        token = "test_auth_token"

        with self.assertRaises(TiingoSubscribeError):
            TiingoClient(url, token)

if __name__ == "__main__":
    unittest.main()
