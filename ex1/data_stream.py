#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    name: str = "Data Processor"

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

    def total(self) -> int:
        return self._rank

    def remaining(self) -> int:
        return len(self._data)


class NumericProcessor(DataProcessor):
    name: str = "Numeric Processor"

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
    name: str = "Text Processor"

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
    name: str = "Log Processor"

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


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break
            else:
                print("DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(f"{proc.name}: total {proc.total()} items processed, "
                  f"remaining {proc.remaining()} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    np = NumericProcessor()
    tp = TextProcessor()
    lp = LogProcessor()
    stream = DataStream()
    print("\nInitialize Data Stream...")
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    stream.register_processor(np)
    batch: list[Any] = [
        'Hello world', [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42, ['Hi', 'five']
    ]
    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    stream.register_processor(tp)
    stream.register_processor(lp)
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors: Numeric 3, "
          "Text 2, Log 1")
    for _ in range(3):
        np.output()
    for _ in range(2):
        tp.output()
    lp.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
