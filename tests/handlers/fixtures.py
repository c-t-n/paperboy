from aiokafka import ConsumerRecord

from paperboy.handler import BaseHandler


class HandlerBaseTestClass:
    def setup_method(self):
        self.mocked_message = ConsumerRecord(
            topic="mock_topic",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"mock_key",
            value=b"mock_value",
            headers=[],
            checksum=0,
            serialized_key_size=0,
            serialized_value_size=0,
        )

        self.mocked_message_tombstone = ConsumerRecord(
            topic="mock_topic",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"mock_key",
            value=None,
            headers=[],
            checksum=0,
            serialized_key_size=0,
            serialized_value_size=0,
        )


async def create_spies(handler: BaseHandler, mocker) -> dict:
    try:
        # BulkHandler Spies
        return {
            "define_context": mocker.spy(handler.__class__, "define_context"),
            "did_receive_message": mocker.spy(handler.__class__, "did_receive_message"),
            "deserialize": mocker.spy(handler.__class__, "deserialize"),
            "did_receive_bulk_messages": mocker.spy(handler.__class__, "did_receive_bulk_messages"),
            "on_bulk": mocker.spy(handler.__class__, "on_bulk"),
            "on_bulk_error": mocker.spy(handler.__class__, "on_bulk_error"),
            "on_message": mocker.spy(handler.__class__, "on_message"),
            "on_tombstone": mocker.spy(handler.__class__, "on_tombstone"),
            "on_finish_handling": mocker.spy(handler.__class__, "on_finish_handling"),
            "on_error": mocker.spy(handler.__class__, "on_error"),
        }
    except Exception:
        # Handler Spies
        return {
            "define_context": mocker.spy(handler.__class__, "define_context"),
            "did_receive_message": mocker.spy(handler.__class__, "did_receive_message"),
            "deserialize": mocker.spy(handler.__class__, "deserialize"),
            "on_message": mocker.spy(handler.__class__, "on_message"),
            "on_tombstone": mocker.spy(handler.__class__, "on_tombstone"),
            "on_finish_handling": mocker.spy(handler.__class__, "on_finish_handling"),
            "on_error": mocker.spy(handler.__class__, "on_error"),
        }
