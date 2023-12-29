from aiokafka import ConsumerRecord

import pytest
from paperboy import BulkHandler, Context
from tests.handlers.fixtures import HandlerBaseTestClass, create_spies


class MockedHandler(BulkHandler):
    ...


class RaiseErrorHandler(BulkHandler):
    @classmethod
    async def on_bulk(cls, msgs: list[ConsumerRecord], ctx: Context) -> None:
        raise Exception("error")

    @classmethod
    async def on_message(cls, e: Exception, msg: ConsumerRecord, ctx: Context) -> Exception | None:
        raise Exception("error")


@pytest.mark.asyncio
class TestBulkHandler(HandlerBaseTestClass):
    async def test_should_call_correct_methods_on_empty_batch(self, mocker):
        handler = MockedHandler()
        spies = await create_spies(handler, mocker)

        await handler.handle([])

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 0
        assert spies["did_receive_message"].call_count == 0
        assert spies["on_message"].call_count == 0
        assert spies["on_tombstone"].call_count == 0
        assert spies["on_error"].call_count == 0
        assert spies["on_finish_handling"].call_count == 1

    async def test_should_handle_one_message_properly(self, mocker):
        handler = MockedHandler()
        spies = await create_spies(handler, mocker)

        await handler.handle(self.mocked_message)

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 1
        assert spies["did_receive_message"].call_count == 1
        assert spies["on_message"].call_count == 1
        assert spies["on_tombstone"].call_count == 0
        assert spies["on_error"].call_count == 0
        assert spies["on_finish_handling"].call_count == 1

    async def test_should_handle_one_tombstone_properly(self, mocker):
        handler = MockedHandler()
        spies = await create_spies(handler, mocker)

        await handler.handle(self.mocked_message_tombstone)

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 1
        assert spies["did_receive_message"].call_count == 1
        assert spies["on_message"].call_count == 0
        assert spies["on_tombstone"].call_count == 1
        assert spies["on_error"].call_count == 0
        assert spies["on_finish_handling"].call_count == 1

    async def test_should_handler_exception_properly(self, mocker):
        handler = RaiseErrorHandler()
        spies = await create_spies(handler, mocker)

        with pytest.raises(Exception) as excinfo:
            await handler.handle(self.mocked_message)
            assert str(excinfo.value) == "error"

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 1
        assert spies["did_receive_message"].call_count == 1
        assert spies["on_message"].call_count == 1
        assert spies["on_tombstone"].call_count == 0
        assert spies["on_error"].call_count == 1
        assert spies["on_finish_handling"].call_count == 0


@pytest.mark.asyncio
class TestBulkHandlerBatches(HandlerBaseTestClass):
    async def test_should_handle_batches_in_once(self, mocker):
        handler = MockedHandler()
        spies = await create_spies(handler, mocker)

        await handler.handle(
            [
                self.mocked_message,
                self.mocked_message_tombstone,
            ]
        )

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 2
        assert spies["did_receive_message"].call_count == 0
        assert spies["did_receive_bulk_messages"].call_count == 1
        assert spies["on_bulk"].call_count == 1
        assert spies["on_bulk_error"].call_count == 0
        assert spies["on_message"].call_count == 0
        assert spies["on_tombstone"].call_count == 0
        assert spies["on_error"].call_count == 0
        assert spies["on_finish_handling"].call_count == 1

    async def test_should_fail_on_batch_exception(self, mocker):
        handler = RaiseErrorHandler()
        spies = await create_spies(handler, mocker)

        with pytest.raises(Exception) as excinfo:
            await handler.handle(
                [
                    self.mocked_message,
                    self.mocked_message_tombstone,
                ]
            )

        assert spies["define_context"].call_count == 1
        assert spies["deserialize"].call_count == 2
        assert spies["did_receive_message"].call_count == 0
        assert spies["did_receive_bulk_messages"].call_count == 1
        assert spies["on_bulk"].call_count == 1
        assert spies["on_bulk_error"].call_count == 1
        assert spies["on_message"].call_count == 0
        assert spies["on_tombstone"].call_count == 0
        assert spies["on_error"].call_count == 0
        assert spies["on_finish_handling"].call_count == 0
