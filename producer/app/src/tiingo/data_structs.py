from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict

from pydantic import Field, validator
from pydantic.dataclasses import dataclass

from .exceptions import TiingoClientError

TiingoRecord = Dict[str, Any]


class Message(ABC):
    @abstractmethod
    def raise_for_status(self) -> None:
        pass


@dataclass
class NonTradeMessage(Message):
    code: int
    message: str

    def raise_for_status(self) -> None:
        if self.code != 200:
            raise TiingoClientError(self.code, self.message)


def _formatted_utc_now() -> str:
    return datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class TradeMessage(Message):
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    processed_at: str = Field(default_factory=_formatted_utc_now)

    def raise_for_status(self) -> None:
        pass

    @validator("date")
    @classmethod
    def ensure_timestamp(cls, timestamp) -> str:
        naked_timestamp = str.replace(timestamp, "+00:00", "")
        format_timestamp = {32: f"{naked_timestamp}Z", 25: f"{naked_timestamp}.000000Z"}
        return format_timestamp[len(timestamp)]
