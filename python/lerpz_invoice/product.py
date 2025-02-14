from copy import deepcopy
from typing import Callable

from lerpz_invoice import BuildType
from lerpz_invoice.catalog import Catalog
from lerpz_invoice.invoice import InvoiceData
from lerpz_invoice.transform import Transform, TransformBuilder


class ProductBuilder:
    data: InvoiceData
    catalog: Catalog

    _it: Transform
    _ct: Transform
    _vt: Transform

    def __init__(self, data: InvoiceData):
        self.data = data

    def invoice(self, func: Callable[[TransformBuilder], Transform]):
        self._it = func(TransformBuilder(deepcopy(self.data)))
        return self

    def correction(self, func: Callable[[TransformBuilder], Transform]):
        self._ct = func(TransformBuilder(deepcopy(self.data)))
        return self

    def visualize(self, func: Callable[[TransformBuilder], Transform]):
        self._vt = func(TransformBuilder(deepcopy(self.data)))
        return self

    def get_catalog(self) -> Catalog:
        for rule in self._it.rules:
            if rule.__doc__ is None:
                raise ValueError("Missing docstring for rule.")
            self.catalog.changes[rule.__qualname__] = rule.__doc__

        collector = self._it.collector
        if collector.__doc__ is None:
            raise ValueError("Missing docstring for collector.")
        self.catalog.changes[collector.__qualname__] = collector.__doc__

        return self.catalog

    def build(self, type: BuildType):
        match type:
            case BuildType.INVOICE:
                if self._it is None:
                    raise ValueError("No invoice transform.")
                return self._it.transform()
            case BuildType.CORRECTION:
                if self._ct is None:
                    raise ValueError("No visualize transform.")
                return self._ct.transform()
            case BuildType.VISUALIZE:
                if self._vt is None:
                    raise ValueError("No visualize transform.")
                return self._ct.transform()
            case _:
                raise ValueError("Invalid build type.")
