from datetime import datetime

from aiokafka import ConsumerRecord


def generate_message(key: bytes | None = None, value: bytes | None = None, index=0) -> ConsumerRecord:
    return ConsumerRecord(
        topic="mock_topic",
        partition=0,
        offset=index,
        timestamp=datetime.now().timestamp(),
        timestamp_type=0,
        key=key,
        value=value,
        headers=[],
        checksum=0,
        serialized_key_size=0,
        serialized_value_size=0,
    )


class HelperBaseTestClass:
    def setup_method(self):
        messages = (
            (b"a", b"123"),
            (b"b", b"234"),
            (b"c", b"345"),
            (b"a", b"456"),
            (b"b", b"1231"),
            (b"b", b"113"),
            (b"b", b"jfodw3"),
            (b"f", b"12"),
            (b"e", None),
            (b"k", b"123"),
            (b"a", None),
            (b"k", b"123"),
            (b"r", b"123"),
            (b"v", b"123"),
            (b"d", b"123"),
        )

        self.message_batch = list(generate_message(k, v, i) for (i, (k, v)) in enumerate(messages))
