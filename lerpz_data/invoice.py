from __future__ import annotations

from datetime import date

import polars as pl

from lerpz_data.transform import Transform

SourceList = list[str]


class Invoice:
    _date: date
    _data: pl.DataFrame

    def __init__(self, data: pl.DataFrame):
        self._date = date.today()
        self._data = data

    @classmethod
    def from_transform(cls, transform: Transform) -> Invoice:
        return cls(transform.collect())

    def date(self) -> date:
        return self._date

    def data(self) -> pl.DataFrame:
        return self._data
