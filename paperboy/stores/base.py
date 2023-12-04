from abc import ABC, abstractmethod
from typing import Generic

from paperboy.stores.types import KT, VT


class Store(ABC, Generic[KT, VT]):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def get_item(self, key: KT):
        raise NotImplementedError

    @abstractmethod
    def set_item(self, key: KT, value: VT):
        raise NotImplementedError

    @abstractmethod
    def delete_item(self, key: KT):
        raise NotImplementedError

    @abstractmethod
    def on_missing_item(self, key: KT):
        raise NotImplementedError
