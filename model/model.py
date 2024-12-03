from typing import Any, List

from pydantic import BaseModel


class KeyValue(BaseModel):
    key: str
    value: Any


class BatchPutRequest(BaseModel):
    items: List[KeyValue]


class KeyRange(BaseModel):
    start_key: str
    end_key: str
