from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, Literal, TypedDict


class LambdaResponse(TypedDict):
    isBase64Encoded: bool
    statusCode: int
    body: str


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


class DynamoDbAttributeType(Enum):
    STRING = "S"
    NUMBER = "N"


class DynamoDbAttributeMapping(TypedDict):
    attr_type: DynamoDbAttributeType
    attr_value: str | float


class DynamoDbItem(TypedDict):
    Item: Dict[str, DynamoDbAttributeMapping]


class DynamoDbPutRequest(TypedDict):
    PutRequest: DynamoDbItem
