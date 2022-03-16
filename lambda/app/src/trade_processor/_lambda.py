from typing import Any, Dict, List, TypedDict


class LambdaResponse(TypedDict):
    isBase64Encoded: bool
    statusCode: int
    body: str


class S3Event(TypedDict):
    Records: List[Dict[str, Any]]
