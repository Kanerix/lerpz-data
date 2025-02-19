from __future__ import annotations

from functools import wraps
from typing import Callable, Protocol

import polars as pl

from lerpz_data.transform import Transform

SourceList = list[str]


class Invoice:
    data: pl.DataFrame

    def __init__(self, data: pl.DataFrame):
        self.data = data

    @classmethod
    def from_transform(cls, transform: Transform) -> Invoice:
        return cls(transform.collect())


class InvoiceCallable(Protocol):
    def __call__(self, sources: SourceList) -> Invoice: ...


class InvoiceFunction:
    def __init__(self, func: InvoiceCallable):
        self._func = func
        self._is_invoice_function = True

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


def invoice(name: str) -> Callable[[InvoiceCallable], InvoiceFunction]:
    def decorator(func: InvoiceCallable):
        if not callable(func):
            raise ValueError("The decorator must be called with a callable.")

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return InvoiceFunction(wrapper)

    return decorator
