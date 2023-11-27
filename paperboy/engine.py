import asyncio
import logging
import signal
from typing import TYPE_CHECKING, final

from aiokafka import AIOKafkaConsumer, ConsumerStoppedError

if TYPE_CHECKING:
    from paperboy.handler import BaseHandler


class Engine:
    """
    Engine used to consume messages from Kafka, using AIOKafka Library.

    Engine has 2 modes:
    - Single Mode: Consume messages one by one
    - Bulk Mode: Consume messages in bulk, using AIOKafka's getmany method

    When you run the bulk mode, the engine will consume messages until :bulk_mode_threshdold: is reached
    on all topics. This is useful when you want to consume messages in bulk, but you don't want to wait
    for all topics to have messages to consume.

    If the bulk_mode_threshold is None, the engine will consume messages until the application is stopped
    or an error occurs.
    """

    handlers: list[type["BaseHandler"]] = []

    @final
    def __init__(
        self,
        with_commit: bool = True,
        fail_on_exception: bool = False,
        with_bulk_mode: bool = False,
        commit_interval_sec: int = 10,
        bulk_mode_timeout_ms: int = 10000,
        bulk_mode_max_records: int = 10000,
        bulk_mode_threshold: int | None = 1000,
    ):
        self.with_bulk_mode = with_bulk_mode
        self.bulk_mode_timeout_ms = bulk_mode_timeout_ms
        self.bulk_mode_max_records = bulk_mode_max_records
        self.bulk_mode_threshold = bulk_mode_threshold
        self.with_commit = with_commit
        self.fail_on_exception = fail_on_exception
        self.commit_interval_sec = commit_interval_sec

        self.log = logging.getLogger(self.__class__.__name__)
        self.topic_handlers = {handler.topic: handler for handler in self.handlers}
        self.log.debug(f"topic handlers: {self.topic_handlers}")

        self.log.info("KafkaConsumerEngine initialized")

    @final
    async def bulk_mode(self):
        self.log.info("Entering Bulk Mode")
        loop = asyncio.get_running_loop()

        in_bulk = True
        topics_in_bulk = {}

        while in_bulk:
            offsets = {}
            tasks = []
            messages = await self.consumer.getmany(
                timeout_ms=self.bulk_mode_timeout_ms,
                max_records=self.bulk_mode_max_records,
            )

            if not messages:
                break

            self.log.info(
                f"Received messages:{str({tp: len(msgs) for tp, msgs in messages.items()})}",
            )
            for tp, msgs in messages.items():
                if tp.topic not in topics_in_bulk.keys():
                    topics_in_bulk[tp.topic] = True

                handler = self.topic_handlers[tp.topic]
                tasks.append(loop.create_task(handler.handle(msgs)))

                if self.bulk_mode_threshold is not None:
                    highwater = self.consumer.highwater(tp)
                    if highwater <= msgs[-1].offset + self.bulk_mode_threshold:  # type: ignore
                        topics_in_bulk[tp.topic] = False

                offsets[tp] = msgs[-1].offset + 1  # type: ignore

            await asyncio.wait(tasks)

            if not self.consumer._enable_auto_commit and self.with_commit:
                self.log.info(f"Committing offsets: {offsets}")
                await self.consumer.commit(offsets)

            in_bulk = any(topics_in_bulk.values())

        self.log.info("Exiting Bulk Mode")

    @final
    async def __manual_commit(self):
        self.log.info(f"Starting manual commit loop, with a {self.commit_interval_sec}s interval")
        while True:
            await asyncio.sleep(self.commit_interval_sec)
            self.log.debug("Committing offsets...")
            await self.consumer.commit()

    @final
    async def __single_mode(self):
        self.log.info("Starting Single Mode")

        if self.with_commit and not self.consumer._enable_auto_commit:
            loop = asyncio.get_running_loop()
            loop.create_task(self.__manual_commit())

        async for msg in self.consumer:
            handler = self.topic_handlers[msg.topic]
            await handler.handle(msg)

    @final
    async def __consume(self):
        """
        Launches the consumer, and starts consuming messages from Kafka.
        """
        await self.consumer.start()

        self.consumer.subscribe([topic for topic in self.topic_handlers.keys()])

        if self.with_bulk_mode:
            await self.bulk_mode()

        await self.__single_mode()

    @final
    async def start(self):
        """
        Starts the consumer, and handles the shutdown of the application.

        Use this method with asyncio.run, or in an asyncio loop.
        """
        loop = asyncio.get_running_loop()
        try:
            self.consumer = self.configure_consumer()

            if self.consumer._enable_auto_commit:
                self.log.error(
                    "KafkaConsumerEngine handle commiting on its own, "
                    "please set enable_auto_commit=False in the AIOKafkaConsumer constructor. "
                    "If you don't want to commit, please set with_commit=False in the "
                    "Engine constructor."
                )
                await self.consumer.stop()
                return
            signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
            for s in signals:
                loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self.shutdown(s)))

            await loop.create_task(self.__consume())
        except (ConsumerStoppedError, asyncio.CancelledError):
            pass
        except Exception as e:
            self.log.exception(e)
            await self.shutdown(signal.SIGTERM)

    @final
    async def shutdown(self, signal: signal.Signals | None = None):
        """
        Stops the consumer, and cancels all async tasks.
        """
        if signal:
            self.log.info(f"Received exit signal {signal.name}...")
        self.log.info("Stopping consumer...")
        await self.consumer.stop()

        self.log.info("Cancelling async tasks...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]

        self.log.debug("Tasks to cancel: {tasks}")
        await asyncio.gather(*tasks, return_exceptions=True)

    def configure_consumer(self) -> AIOKafkaConsumer:
        """
        Returns an initiated AIOKafkaConsumer instance.

        Override this method to configure your AIOKafkaConsumer instance.
        """
        return AIOKafkaConsumer()
