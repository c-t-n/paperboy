from paperboy.stores.base import Store
from paperboy.stores.types import KT, VT


class MemoryStore(Store[KT, VT]):
    def __init__(self):
        self._data = {}

    def get_item(self, key):
        try:
            return self._data[key]
        except KeyError:
            self.on_missing_item(key)

    def set_item(self, key, value):
        self._data[key] = value

    def delete_item(self, key):
        if key in self._data:
            del self._data[key]

    def on_missing_item(self, key: KT):
        return None
