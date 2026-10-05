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


class NumericProcessor(DataProcessor):
    def _is_number(self, value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(self._is_number(item) for item in data)
        return self._is_number(data)

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._store(str(item))
        else:
            self._store(str(data))


class TextProcessor(DataProcessor):
    def _is_text(self, value: Any) -> bool:
        return isinstance(value, str)

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(self._is_text(item) for item in data)
        return self._is_text(data)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._store(item)
        else:
            self._store(data)


class LogProcessor(DataProcessor):
    def _is_log(self, value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        if "log_level" not in value or "log_message" not in value:
            return False
        return all(isinstance(k, str) and isinstance(v, str)
                   for k, v in value.items())

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(self._is_log(item) for item in data)
        return self._is_log(data)

    def _format(self, log: dict[str, str]) -> str:
        return f"{log['log_level']}: {log['log_message']}"

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for item in data:
                self._store(self._format(item))
        else:
            self._store(self._format(data))


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    np = NumericProcessor()
    val_msg = " Trying to validate input"
    num = 42
    txt = 'Hello'
    print(f"{val_msg} '{num}': {np.validate(num)}")
    print(f"{val_msg} '{txt}': {np.validate(txt)}")
    print(" Test invalid ingestion of string "
          "'foo' without prior validation:")
    try:
        np.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {(e)}")
    numbers: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {numbers}")
    np.ingest(numbers)
    print(" Extracting 3 values...")
    for _ in range(3):
        rank, value = np.output()
        print(f" Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    tp = TextProcessor()
    print(f"{val_msg} '{num}': {tp.validate(num)}")
    print(f"{val_msg} '{txt}': {tp.validate(txt)}")
    words = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {words}")
    tp.ingest(words)
    print(" Extracting 1 value...")
    rank, value = tp.output()
    print(f" Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    lp = LogProcessor()
    print(f"{val_msg} '{txt}': {lp.validate(txt)}")
    logs = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f"{val_msg} '{logs[0]}': {lp.validate(logs[0])}")
    print(f" Processing data: {logs}")
    lp.ingest(logs)
    print(" Extracting 2 values...")
    for _ in range(2):
        rank, value = lp.output()
        print(f" Log entry {rank}: {value}")


if __name__ == "__main__":
    main()
