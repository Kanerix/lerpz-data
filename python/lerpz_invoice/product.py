from copy import deepcopy
from typing import Callable

from lerpz_invoice import BuildType
from lerpz_invoice.invoice import InvoiceData
from lerpz_invoice.transform import Transform, TransformBuilder


class ProductBuilder:
    data: InvoiceData
    i_transform: Transform
    v_transform: Transform
    c_transform: Transform

    def __init__(self, data: InvoiceData):
        self.data = data

    def invoice(self, func: Callable[[TransformBuilder], Transform]):
        self.i_transform = func(TransformBuilder(deepcopy(self.data)))
        return self

    def visualize(self, func: Callable[[TransformBuilder], Transform]):
        self.v_transform = func(TransformBuilder(deepcopy(self.data)))
        return self

    def correction(self, func: Callable[[TransformBuilder], Transform]):
        self.c_transform = func(TransformBuilder(deepcopy(self.data)))
        return self

    def _validate(self, type: BuildType) -> bool:
        if type == BuildType.INVOICE and not self.i_transform:
            return False
        if type == BuildType.VISUALIZE and not self.v_transform:
            return False
        if type == BuildType.CORRECTION and not self.c_transform:
            return False
        return True

    def build(self, type: BuildType):
        if not self._validate(type):
            raise ValueError(f"Missing {type.name.lower()} transform.")
        if type == BuildType.INVOICE:
            print()
            print("Building invoice")
            print("----------------")
            return self.i_transform.transform()
        if type == BuildType.VISUALIZE:
            print()
            print("Building visualize")
            print("------------------")
            return self.v_transform.transform()
        if type == BuildType.CORRECTION:
            print()
            print("Building correction")
            print("-------------------")
            return self.c_transform.transform()
        raise ValueError("Invalid build type.")
