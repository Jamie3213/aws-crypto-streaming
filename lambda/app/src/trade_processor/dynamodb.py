from enum import Enum
from typing import Dict, TypedDict, Union


class AttributeType(Enum):
    STRING = "S"
    NUMBER = "N"


class AttributeMapping(TypedDict):
    attr_type: AttributeType
    attr_value: Union[str, float]


class Item(TypedDict):
    Item: Dict[str, AttributeMapping]


class PutRequest(TypedDict):
    PutRequest: Item
