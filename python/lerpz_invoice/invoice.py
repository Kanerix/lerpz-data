from functools import wraps
from typing import Callable, Protocol

import polars as pl

SourceList = list[str]


class InvoiceData(dict[str, pl.LazyFrame]):
    def collect(self) -> None:
        for key in self.keys():
            self[key] = self[key].collect().lazy()


class InvoiceCallable(Protocol):
    def __call__(self, sources: SourceList) -> object: ...


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
