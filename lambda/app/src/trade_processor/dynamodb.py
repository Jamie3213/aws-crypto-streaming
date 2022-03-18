from enum import Enum
from typing import Callable, Dict, List, Literal

Attributes = Dict[str, Dict[str, str]]
PutRequest = Dict[Literal["PutRequest"], Dict[Literal["Item"], List[Attributes]]]


class AttributeType(Enum):
    STRING = "S"
    NUMBER = "N"


def exhaust_unprocessed_items(function: Callable) -> Callable:
    def wrapper(*args, **kwargs) -> None:
        response = function(*args, **kwargs)
        unprocessed_items = response.get("UnprocessedKeys")

        while unprocessed_items is not None:
            response = function(unprocessed_items)
            unprocessed_items = response.get("UnprocessedKeys")

    return wrapper
