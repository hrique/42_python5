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


    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass


    def _store(self, value: str) -> None:
        self._data.append((self._rank, value))
        self._rank += 1


    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data left on processor")
        return self._data.pop(0)


class NumericProcessor:
    ...

class TextProcessor:
    ...

class LogProcessor:
    ...
