from typing import TypedDict


class LambdaResponse(TypedDict):
    isBase64Encoded: bool
    statusCode: int
    body: str


class Trade(TypedDict):
    ticker: str
    date: str
    exchange: str
    size: float
    price: float
    producer_processed_at: str
    lambda_processed_at: str
