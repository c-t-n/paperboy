from paperboy.helpers import get_last_values_from_batch
from tests.helpers.fixtures import HelperBaseTestClass


class TestGetLatestValuesHelper(HelperBaseTestClass):
    def test_get_latest_values_helper(self):
        values, tombstones = get_last_values_from_batch(self.message_batch)

        listed_values = list(values)
        listed_tombstones = list(tombstones)

        assert len(listed_tombstones) == 2
        assert len(listed_values) == 7
        assert listed_values[0].offset != self.message_batch[0].offset
        assert listed_values[0].key == b"c"
