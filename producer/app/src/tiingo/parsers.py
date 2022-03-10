from abc import ABC, abstractmethod

from .data_structs import Message, NonTradeMessage, TiingoRecord, TradeMessage


class MessageParser(ABC):
    @abstractmethod
    def parse(self, record: TiingoRecord) -> Message:
        pass


class NonTradeMessageParser(MessageParser):
    def parse(self, record: TiingoRecord) -> NonTradeMessage:
        code = record["response"]["code"]
        message = record["response"]["message"]
        return NonTradeMessage(code, message)


class TradeMessageParser(MessageParser):
    def parse(self, record: TiingoRecord) -> TradeMessage:
        _, ticker, date, exchange, size, price = record["data"]
        return TradeMessage(ticker, date, exchange, size, price)


class MessageParserFactory:
    """Used to pick a message parser dynamically from the message type."""

    def create(self, record: TiingoRecord) -> MessageParser:
        """Returns a parser based on the type of the message provided."""
        message_type = record["messageType"]

        factory = {
            "E": NonTradeMessageParser(),
            "I": NonTradeMessageParser(),
            "H": NonTradeMessageParser(),
            "A": TradeMessageParser(),
        }

        return factory[message_type]
