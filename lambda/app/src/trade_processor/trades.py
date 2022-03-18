import gzip
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Union

import boto3

from .dynamodb import Attributes, AttributeType, PutRequest, exhaust_unprocessed_items


def _utc_now_unix() -> str:
    return str(datetime.utcnow().timestamp())


@dataclass
class Trade:
    ticker: str
    executed_at: str
    exchange: str
    size: str
    price: str
    producer_processed_at: str
    lambda_processed_at: str = field(default_factory=_utc_now_unix)


class TradeList:
    def __init__(self, trades: List[Trade]) -> None:
        self.trades = trades

    def __len__(self) -> int:
        return len(self.trades)

    def batch(self, size: int = 25) -> List["TradeList"]:
        batches = []
        for i in range(0, len(self.trades), size):
            batch = TradeList(self.trades[i : i + size])
            batches.append(batch)
        return batches

    def _create_dynamodb_attributes(self, trade: Trade) -> Attributes:
        return {
            "Ticker": {AttributeType.STRING.value: trade.ticker},
            "ExecutedAt": {AttributeType.NUMBER.value: trade.executed_at},
            "Exchange": {AttributeType.STRING.value: trade.exchange},
            "Size": {AttributeType.NUMBER.value: trade.size},
            "Price": {AttributeType.NUMBER.value: trade.price},
            "ProducerProcessedAt": {
                AttributeType.NUMBER.value: trade.producer_processed_at
            },
        }

    def _create_dynamodb_put_request(self, trade: Trade) -> PutRequest:
        attributes = self._create_dynamodb_attributes(trade)
        item = {"Item": attributes}
        return {"PutRequest": item}

    @exhaust_unprocessed_items
    def _put_batch_to_dynamodb(
        self, request_items: Dict[str, List[PutRequest]]
    ) -> Dict[str, Any]:
        client = boto3.client("dynamodb")
        return client.batch_write_item(RequestItems=request_items)

    def put_to_dynamodb(self, table: str) -> None:
        client = boto3.client("dynamodb")
        put_requests = [
            self._create_dynamodb_put_request(trade) for trade in self.trades
        ]
        request_items = {table: put_requests}
        self._put_batch_to_dynamodb(request_items)


class RawTrades:
    def __init__(self, file: BytesIO) -> None:
        self.file = file

    def _iso8601_date_to_unix(self, date: str) -> float:
        iso_date = date.strip("Z")
        return datetime.fromisoformat(iso_date).timestamp()

    def _parse_trade(self, trade: Dict[str, Union[str, float]]) -> Trade:
        ticker = trade["ticker"]
        executed_at = str(self._iso8601_date_to_unix(trade["date"]))
        exchange = trade["exchange"]
        size = str(trade["size"])
        price = str(trade["price"])
        producer_processed_at = str(self._iso8601_date_to_unix(trade["processed_at"]))
        return Trade(ticker, executed_at, exchange, size, price, producer_processed_at)

    def parse(self) -> TradeList:
        lines = [line.decode("utf-8").strip("\n") for line in self.file.readlines()]
        trades = [json.loads(line) for line in lines]
        parsed_trades = [self._parse_trade(trade) for trade in trades]
        return TradeList(parsed_trades)


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
