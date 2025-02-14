from enum import Enum, auto

from lerpz_invoice.invoice import InvoiceData, SourceList, invoice
from lerpz_invoice.product import ProductBuilder
from lerpz_invoice.transform import collect, transform, Transform

__all__ = [
    'InvoiceData',
    'SourceList',
    'invoice',
    'ProductBuilder',
    'transform',
    'collect',
    'Transform',
]


class BuildType(Enum):
    INVOICE = auto()
    VISUALIZE = auto()
    CORRECTION = auto()
