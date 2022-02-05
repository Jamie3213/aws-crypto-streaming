import os
import sys

parent = os.path.abspath(".")
sys.path.append(os.path.join(parent, "producer", "app"))

import unittest
import tiingo


class TestSubscriptionMessageMethods(unittest.TestCase):

    raw_message = '{"messageType": "I", "data": {"subscriptionId": 1234}, "response": {"message": "Success", "code": 200}}'
    subscription_message = tiingo._SubscriptionMessage(raw_message)
    subscription_response = tiingo._Response("Success", 200)

    def test_parse_subscription_id(self):
        self.assertEqual(self.subscription_message.subscription_id, 1234)

    def test_parse_response(self):
        self.assertEqual(self.subscription_message.response, self.subscription_response)


class TestHeartbeatMessageMethods(unittest.TestCase):

    raw_message = (
        '{"messageType": "H", "response": {"message": "Success", "code": 200}}'
    )
    heartbeat_message = tiingo._HeartbeatMessage(raw_message)
    heartbeat_response = tiingo._Response("Success", 200)

    def test_parse_response(self):
        self.assertEqual(self.heartbeat_message.response, self.heartbeat_response)


class TestTradeMessageMethods(unittest.TestCase):

    # Dummy date
    badly_formatted_date = "2022-01-01T00:00:00+00:00"
    date_format_in = "%Y-%m-%dT%H:%M:%S.%f%z"
    date_format_out = "%Y-%m-%dT%H:%M:%S.%fZ"

    # Dummy trade update message
    raw_message = """{"messageType": "A", "service": "crypto_data", 
                    "data": ["A", "BTC", "2022-01-01T00:00:00.123456+00:00", "Bitfenix", 123.4, 567.8]}
                """
    trade_message = tiingo._TradeMessage(raw_message)
    trade_data = trade_message.trade_data
    actual_data = (
        trade_data.ticker,
        trade_data.date,
        trade_data.exchange,
        trade_data.size,
        trade_data.price,
    )
    expected_data = ("BTC", "2022-01-01T00:00:00.123456Z", "Bitfenix", 123.4, 567.8)

    def test_ensure_date(self):
        self.assertEqual(
            tiingo._TradeMessage._ensure_date(
                self.badly_formatted_date, self.date_format_in, self.date_format_out
            ),
            "2022-01-01T00:00:00.000000Z",
        )

    def test_parse_service(self):
        self.assertEqual(self.trade_message.service, "crypto_data")

    def test_parse_trade_data(self):
        self.assertEqual(self.actual_data, self.expected_data)


if __name__ == "__main__":
    unittest.main()
