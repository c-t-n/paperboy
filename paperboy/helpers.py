from collections import deque
from itertools import filterfalse, groupby, tee
from typing import Iterable

from aiokafka import ConsumerRecord

Values = Iterable[ConsumerRecord]
Tombstones = Iterable[ConsumerRecord]


def partition(pred, iterable):
    """Partition entries into false entries and true entries.

    If *pred* is slow, consider wrapping it with functools.lru_cache().
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)


def get_last_values_from_batch(msgs: list[ConsumerRecord]) -> tuple[Values, Tombstones]:
    """
    Returns the last values from each key in a batch of messages, and returns two
    iterators: one for the values, and one for the tombstones.

    example:
    >>> pprint(msgs)
    [
        ConsumerRecord(offset=1, key="a", value=1, ...),
        ConsumerRecord(offset=2, key="a", value=2, ...),
        ConsumerRecord(offset=3, key="a", value=3, ...),
        ConsumerRecord(offset=4, key="b", value=1, ...),
        ConsumerRecord(offset=5, key="c", value=1, ...),
        ConsumerRecord(offset=6, key="c", value=10, ...),
        ConsumerRecord(offset=7, key="c", value=25, ...),
        ConsumerRecord(offset=8, key="c", value=None, ...),
        ConsumerRecord(offset=9, key="b", value=100, ...),
    ]
    >>> values, tombstones = get_last_values_from_batch(msgs)
    >>> list(values)
    [
        ConsumerRecord(offset=3, key="a", value=3, ...),
        ConsumerRecord(offset=9, key="b", value=100, ...),
    ]
    >>> list(tombstones)
    [
        ConsumerRecord(offset=8, key="c", value=None, ...),
    ]
    """
    v = map(
        lambda group: deque(group[1], maxlen=1).pop(),
        groupby(msgs, lambda msg: msg.key),
    )

    return partition(lambda record: record.value is None, v)


__all__ = ["get_last_values_from_batch"]
