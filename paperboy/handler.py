import logging
from typing import Generic, TypeVar, final

from aiokafka import ConsumerRecord

KT = TypeVar("KT")
VT = TypeVar("VT")

Context = dict | None


class BasePaperboyHandler(Generic[KT, VT]):
    """
    Base class for all topic handlers.

    Topic handlers are used to handle messages from a specific topic.
    This class is used to define the lifecycle hooks you want to use.
    """

    topic: str
    logger: logging.Logger

    @classmethod
    async def handle(
        cls,
        msgs: ConsumerRecord[KT, VT] | list[ConsumerRecord[KT, VT]],
        fail_on_exception: bool = False,
    ):
        cls.logger = logging.getLogger(cls.__name__)

    @classmethod
    async def define_context(cls) -> Context:
        """
        Lifecycle Hook called before handling a batch of messages.

        Use it to define a context that will be passed to all other hooks.
        """
        cls.logger.debug("define_context")
        return None

    @classmethod
    async def did_receive_message(cls, msg: ConsumerRecord, ctx: Context) -> ConsumerRecord[KT, VT]:
        """
        Lifecycle Hook called when a message of a batch is received.

        Use it to modify the message before handling it.
        """
        cls.logger.debug(f"did_receive_message {msg=} {ctx=}")
        return msg

    @classmethod
    async def on_tombstone(cls, msg: ConsumerRecord[KT, VT], ctx: Context):
        """
        Lifecycle Hook called when a tombstone is received.

        Tombstone events are used to delete a record from a topic.
        Use this callback to handle a deletion.
        """
        cls.logger.debug(f"on_tombstone: {msg=} {ctx=}")
        return

    @classmethod
    async def on_message(cls, msg: ConsumerRecord[KT, VT], ctx: Context):
        """
        Lifecycle Hook called when a message is received.

        Use it to handle the message.
        """
        cls.logger.debug(f"on_message: {msg=} {ctx=}")
        return

    @classmethod
    async def on_error(cls, e: Exception, msg: ConsumerRecord[KT, VT], ctx: Context) -> Exception | None:
        """
        Lifecycle Hook called when an error occurs while handling a message.

        Use it to handle errors.
        """
        cls.logger.debug(f"on_error: {e=} {msg=} {ctx=}")
        cls.logger.exception(e)

        return e

    @classmethod
    async def on_finish_handling(cls, msgs: list[ConsumerRecord[KT, VT]], ctx: Context | None = None):
        """
        Lifecycle Hook called after handling all messages of a batch

        Use it as a cleaner or to log the number of messages handled
        """
        cls.logger.debug(f"on_finish_handling: {ctx=}")
        return


class PaperboyHandler(BasePaperboyHandler[KT, VT]):
    """
    Topic handlers are used to handle messages from a specific topic.
    The handling is already operated in intern, you just have to define
    the lifecycle hooks you want to use.

    This handler will handle messages one by one, even in bulk mode.
    If you need to add a specific handling for bulk mode, use AIOKafkaTopicBulkHandler
    instead.

    Lifecycle Hooks:
    - `define_context`: Called before handling a batch of messages
    - `did_receive_message`: Called when a message of a batch is received
    - `on_tombstone`: Called when a tombstone is received
    - `on_message`: Called when a message is received
    - `on_error`: Called when an error occurs while handling a message
    - `on_finish_handling`: Called after handling all messages of a batch

    All Lifecycle Hooks are called with the same context, defined in define_context.

    For all lifecycle hooks, you can use the class attribute logger to log messages
    with `cls.logger`

    Example of declaration:
    ```python
    class UserHandler(AIOKafkaTopicHandler):
        topic: str = "example.users"

        @classmethod
        async def define_context(cls):
            ctx = { "user": get_user_repository() }
            return ctx

        @classmethod
        async def on_message(cls, msg, ctx):
            ctx["user"].create(msg.key, msg.value)

        @classmethod
        async def on_tombstone(cls, msg, ctx):
            ctx["user"].delete(msg.key)

        @classmethod
        async def on_finish_handling(cls, msgs, ctx):
            cls.logger.info(f"Finished handling {len(msgs)} messages")
    ```
    """

    @classmethod
    @final
    async def handle(
        cls,
        msgs: ConsumerRecord[KT, VT] | list[ConsumerRecord[KT, VT]],
        fail_on_exception: bool = False,
    ):
        cls.logger = logging.getLogger(cls.__name__)
        ctx = await cls.define_context()

        if not isinstance(msgs, list):
            msgs = [msgs]

        for msg in msgs:
            try:
                msg = await cls.did_receive_message(msg, ctx)

                if msg.value is None:
                    await cls.on_tombstone(msg, ctx)
                else:
                    await cls.on_message(msg, ctx)

            except Exception as e:
                exc = await cls.on_error(e, msg, ctx)
                if exc or fail_on_exception:
                    raise exc or e

        await cls.on_finish_handling(msgs, ctx)


class PaperboyBulkHandler(BasePaperboyHandler[KT, VT]):
    """
    Handler that have a different handling method in bulk mode.

    This could be interesting with bulk inserts in a database for example.

    Lifecycle Hooks:
    - `define_context`: Called before handling a batch of messages
    - `did_receive_message`: Called when a message of a batch is received
    - `on_tombstone`: Called when a tombstone is received
    - `on_message`: Called when a message is received
    - `on_error`: Called when an error occurs while handling a message
    - `on_bulk`: Called when a message batch is received.
    - `on_bulk_error`: Called when an error occurs while handling a message batch.
    - `on_finish_handling`: Called after handling all messages of a batch
    """

    @classmethod
    @final
    async def handle(
        cls,
        msgs: ConsumerRecord[KT, VT] | list[ConsumerRecord[KT, VT]],
        fail_on_exception: bool = False,
    ):
        cls.logger = logging.getLogger(cls.__name__)
        ctx = await cls.define_context()

        if isinstance(msgs, list):
            msgs = await cls.did_receive_bulk_messages(msgs, ctx)

            try:
                await cls.on_bulk(msgs, ctx)
            except Exception as e:
                exc = await cls.on_bulk_error(e, msgs, ctx)
                if exc or fail_on_exception:
                    raise exc or e

            await cls.on_finish_handling(msgs, ctx)
            return

        try:
            msg = await cls.did_receive_message(msgs, ctx)

            if msg.value is None:
                await cls.on_tombstone(msgs, ctx)
            else:
                await cls.on_message(msgs, ctx)

        except Exception as e:
            exc = await cls.on_error(e, msgs, ctx)
            if exc or fail_on_exception:
                raise exc or e

        await cls.on_finish_handling(msgs, ctx)

    @classmethod
    async def did_receive_bulk_messages(
        cls, msgs: list[ConsumerRecord[KT, VT]], ctx: Context
    ) -> list[ConsumerRecord[KT, VT]]:
        """
        Lifecycle Hook called before handling a message batch.

        Use it to handle the message.
        """
        cls.logger.debug(f"did_receive_bulk_messages: {msgs=} {ctx=}")
        return msgs

    @classmethod
    async def on_bulk(cls, msgs: list[ConsumerRecord[KT, VT]], ctx: Context):
        """
        Lifecycle Hook called when a message is received.

        Use it to handle the message.
        """
        cls.logger.debug(f"on_bulk: {msgs=} {ctx=}")
        return

    @classmethod
    async def on_bulk_error(cls, e: Exception, msgs: list[ConsumerRecord[KT, VT]], ctx: Context) -> Exception | None:
        """
        Lifecycle Hook called when an error occurs while handling a message.

        Use it to handle errors.
        """
        cls.logger.debug(f"on_bulk_error: {e=} {msgs=} {ctx=}")
        cls.logger.exception(e)

        return e

    @classmethod
    async def on_finish_handling(
        cls,
        msgs: list[ConsumerRecord[KT, VT]] | ConsumerRecord[KT, VT],
        ctx: Context | None = None,
    ):
        """
        Lifecycle Hook called after handling all messages of a batch

        Use it as a cleaner or to log the number of messages handled
        """
        cls.logger.debug(f"on_finish_handling: {ctx=}")
        return
