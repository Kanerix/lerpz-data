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

    def add_rule(self, rule: TransformFunction) -> TransformBuilder:
        if not rule._is_transform_function:
            raise ValueError("The rule must be decorated with @transform.")
        self.rules.append(rule)
        return self

    def finish(self, func: CollectFunction) -> Transform:
        if not getattr(func, "_is_collect_function"):
            raise ValueError("The rule must be decorated with @collect.")
        return Transform(self.data, self.rules, func)


class TransformData(dict[str, pl.LazyFrame]):
    def collect(self) -> None:
        for key in self.keys():
            self[key] = self[key].collect().lazy()


class TransformCallable(Protocol):
    def __call__(self, data: TransformData) -> TransformData: ...


class TransformFunction:
    def __init__(self, func: TransformCallable):
        self._func = func
        self._is_transform_function = True

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


def transform() -> Callable[[TransformCallable], TransformFunction]:
    def decorator(func: TransformCallable) -> TransformFunction:
        if not callable(func):
            raise ValueError("The decorator must be called with a callable.")

        @wraps(func)
        def wrapper(data: TransformData):
            if not isinstance(data, TransformData):
                raise ValueError("The first argument must be an InvoiceData object.")
            processed = func(data)
            processed.collect()
            return processed

        return TransformFunction(wrapper)

    return decorator


class CollectCallable(Protocol):
    def __call__(self, data: TransformData) -> pl.DataFrame: ...


class CollectFunction:
    def __init__(self, func: CollectCallable):
        self._func = func
        self._is_collect_function = True

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


def collect() -> Callable[[CollectCallable], CollectFunction]:
    def decorator(func: CollectCallable) -> CollectFunction:
        if not callable(func):
            raise ValueError("The decorator must be called with a callable.")

        @wraps(func)
        def wrapper(data: TransformData):
            if not isinstance(data, TransformData):
                raise ValueError("The first argument must be an InvoiceData object.")
            return func(data)

        return CollectFunction(wrapper)

    return decorator
