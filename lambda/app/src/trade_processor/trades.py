import gzip
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Union

import boto3

from .dynamodb import AttributeMapping, AttributeType, Item, PutRequest


def _utc_now_unix() -> str:
    return datetime.utcnow().timestamp()


@dataclass
class Trade:
    ticker: str
    executed_at: float
    exchange: str
    size: float
    price: float
    producer_processed_at: float
    lambda_processed_at: float = field(default_factory=_utc_now_unix)


class TradeList:
    def __init__(self, trades: List[Trade]) -> None:
        self.trades = trades

    def __len__(self) -> int:
        return len(self.trades)

    def create_batches(self, size: int = 25) -> List[List["TradeList"]]:
        batches = []
        for i in range(0, len(self.trades), size):
            batches.append(self.trades[i : i + size])
        return batches

    def _create_dynamodb_attributes(self, trade: Trade) -> Dict[str, AttributeMapping]:
        return {
            "Ticker": AttributeMapping(AttributeType.STRING.value, trade.ticker),
            "ExecutedAt": AttributeMapping(
                AttributeType.NUMBER.value, trade.executed_at
            ),
            "Exchange": AttributeMapping(AttributeType.STRING.value, trade.exechange),
            "Size": AttributeMapping(AttributeType.NUMBER.value, trade.size),
            "Price": AttributeMapping(AttributeType.NUMBER.value, trade.price),
            "ProducerProcessedAt": AttributeMapping(
                AttributeType.NUMBER.value, trade.producer_processed_at
            ),
        }

    def _create_dynamodb_put_request(self, trade: Trade) -> PutRequest:
        attributes = self._create_dynamodb_attributes(trade)
        item = Item(attributes)
        return PutRequest(item)

    def put_to_dynamodb(self, table: str) -> None:
        client = boto3.client("dynamodb")
        put_requests = [
            self._create_dynamodb_put_request(trade) for trade in self.trades
        ]
        request_item = {table: put_requests}
        client.batch_write_item(RequestItem=request_item)


class RawTrades:
    def __init__(self, file: BytesIO) -> None:
        self.file = file

    def _iso8601_date_to_unix(self, date: str) -> float:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()

    def _parse_trade(self, trade: Dict[str, Union[str, float]]) -> Trade:
        ticker = trade["ticker"]
        executed_at = self._iso8601_date_to_unix(trade["date"])
        exchange = trade["exchange"]
        size = trade["size"]
        price = trade["price"]
        producer_processed_at = self._iso8601_date_to_unix(trade["processed_at"])
        return Trade(ticker, executed_at, exchange, size, price, producer_processed_at)

    def parse(self) -> TradeList:
        lines = [line.decode("utf-8").strip("\n") for line in self.file.readlines()]
        trades = [json.loads(line) for line in lines]
        return TradeList([self._parse_trade(trade) for trade in trades])


class CompressedTrades(ABC):
    def __init__(self, file: bytes) -> None:
        self.file = file

    @abstractmethod
    def decompress(self) -> RawTrades:
        pass


class GzipCompressedTrades(CompressedTrades):
    def decompress(self) -> RawTrades:
        decompressed = gzip.decompress(self.file)
        file_like_trades = BytesIO(decompressed)
        return RawTrades(file_like_trades)
