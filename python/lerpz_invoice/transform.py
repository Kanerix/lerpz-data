from __future__ import annotations

from functools import wraps
from typing import Callable, Protocol

import polars as pl

from lerpz_invoice.invoice import InvoiceData


class TransformBuilder:
    data: InvoiceData
    rules: list[TransformCallable]

    def __init__(self, data: InvoiceData):
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


class Transform:
    data: InvoiceData
    rules: list[TransformCallable]
    collector: CollectCallable

    def __init__(self, data: InvoiceData, rules: list[TransformCallable], collector: CollectCallable):
        self.data = data
        self.rules = rules
        self.collector = collector

    def transform(self) -> pl.DataFrame:
        for rule in self.rules:
            self.data = rule(self.data)
        df = self.collector(self.data)
        return df


class TransformCallable(Protocol):
    def __call__(self, data: InvoiceData) -> InvoiceData: ...


class TransformFunction:
    def __init__(self, func: TransformCallable):
        self._func = func
        self._is_transform_function = True

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)
    

class CollectCallable(Protocol):
    def __call__(self, data: InvoiceData) -> pl.DataFrame: ...


class CollectFunction:
    def __init__(self, func: CollectCallable):
        self._func = func
        self._is_collect_function = True

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


def transform() -> Callable[[TransformCallable], TransformFunction]:
    def decorator(func: TransformCallable) -> TransformFunction:
        if not callable(func):
            raise ValueError("The decorator must be called with a callable.")

        @wraps(func)
        def wrapper(*args, **kwargs):
            initial: InvoiceData = args[0]
            if not isinstance(initial, InvoiceData):
                raise ValueError("The first argument must be an InvoiceData object.")

            initial_keys = str(initial.keys())
            processed = func(*args, **kwargs)

            print(f"{initial_keys} --> {str(processed.keys())}")
            processed.collect()

            return processed

        return TransformFunction(wrapper)
    
    return decorator


def collect() -> Callable[[CollectCallable], CollectFunction]:
    def decorator(func: CollectCallable) -> CollectFunction:
        if not callable(func):
            raise ValueError("The decorator must be called with a callable.")

        @wraps(func)
        def wrapper(*args, **kwargs):
            initial: InvoiceData = args[0]
            if not isinstance(initial, InvoiceData):
                raise ValueError("The first argument must be an InvoiceData object.")
            
            initial_keys = str(initial.keys())
            processed = func(*args, **kwargs)

            print(f"{initial_keys} collected into schema {str(processed.columns)}")

            return processed

        return CollectFunction(wrapper)

    return decorator
