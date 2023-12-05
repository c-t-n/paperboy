from itertools import filterfalse, groupby, tee
from collections import deque
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
    >>> msgs = [
    >>>  {"key": "a", "value": 1},
    >>>  {"key": "a", "value": 2},
    >>>  {"key": "a", "value": 3},
    >>>  {"key": "b", "value": 1},
    >>>  {"key": "c", "value": 1},
    >>>  {"key": "c", "value": 10},
    >>>  {"key": "c", "value": 25},
    >>>  {"key": "c", "value": None}
    >>>  {"key": "b", "value": 100},
    >>> ]
    >>> values, tombstones = get_last_values_from_batch(msgs)
    >>> list(values)
    [{"key": "a", "value": 3}, {"key": "b", "value": 100}]
    >>> list(tombstones)
    [{"key": "c", "value": None}]
    """
    v = map(
        lambda group: deque(group[1], maxlen=1).pop(),
        groupby(msgs, lambda msg: msg.key),
    )

    return partition(lambda record: record.value is None, v)


__all__ = ["get_last_values_from_batch"]
