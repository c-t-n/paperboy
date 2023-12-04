from rocksdict import Options, Rdict

from paperboy.stores.base import Store
from paperboy.stores.types import KT, VT


class RocksDBStore(Store[KT, VT]):
    def __init__(self, path: str = "./db/test_db"):
        # TODO: Find a way to reduce log writes
        db_options = Options()
        db_options.create_if_missing(True)
        db_options.set_db_log_dir("/dev/null")
        self._data = Rdict(path)

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

    def __del__(self):
        self._data.flush()
        self._data.flush_wal()
        self._data.close()
