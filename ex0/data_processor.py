#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass



    def output(self) -> tuple[int, str]:
        ...


    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass


    def _store(self, value: str) -> None:
        ...


class NumericProcessor:
    ...

class TextProcessor:
    ...

class LogProcessor:
    ...
