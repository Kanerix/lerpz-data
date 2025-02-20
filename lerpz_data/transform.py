from __future__ import annotations

import polars as pl

from functools import wraps
from typing import Protocol, Callable


class Transform:
    data: TransformData
    rules: list[TransformCallable]
    collector: CollectCallable

    def __init__(
        self,
        data: TransformData,
        rules: list[TransformCallable],
        collector: CollectCallable,
    ):
        self.data = data
        self.rules = rules
        self.collector = collector

    @staticmethod
    def builder(data: TransformData) -> TransformBuilder:
        return TransformBuilder(data)

    def collect(self) -> pl.DataFrame:
        for rule in self.rules:
            self.data = rule(self.data)
        df = self.collector(self.data)
        return df


class TransformBuilder:
    data: TransformData
    rules: list[TransformCallable]

    def __init__(self, data: TransformData):
        self.data = data
        self.rules = []

    def add_rule(self, rule: TransformCallable) -> TransformBuilder:
        self.rules.append(rule)
        return self

    def finish(self, func: CollectCallable) -> Transform:
        if not getattr(func, "_is_collect_function"):
            raise ValueError("The rule must be decorated with @collect.")
        return Transform(self.data, self.rules, func)


class TransformData(dict[str, pl.LazyFrame]):
    def collect(self) -> None:
        for key in self.keys():
            self[key] = self[key].collect().lazy()


class TransformCallable(Protocol):
    def __call__(self, data: TransformData) -> TransformData: ...


class CollectCallable(Protocol):
    def __call__(self, data: TransformData) -> pl.DataFrame: ...
