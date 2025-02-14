from collections import OrderedDict


class Catalog:
    changes: OrderedDict[str, str]

    def __init__(self):
        self.changes = OrderedDict()

    def size(self) -> int:
        return len(self.changes.keys())
    