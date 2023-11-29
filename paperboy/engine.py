import asyncio
import logging
import logging.config
import signal
from typing import TYPE_CHECKING, final

from aiokafka import AIOKafkaConsumer, ConsumerStoppedError, TopicPartition

from .logs import PaperboyFormatter

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
    steps: list[list[type["BaseHandler"]]] = []
    __loop: asyncio.AbstractEventLoop

    @final
    def __init__(
        self,
        bulk_mode_timeout_ms: int = 10000,
        bulk_mode_max_records: int = 10000,
        bulk_mode_threshold: int | None = 0,
        commit_interval_sec: int = 10,
        fail_on_exception: bool = False,
        logger_level: int = logging.INFO,
        with_commit: bool = True,
        with_bulk_mode: bool = False,
        with_aiokafka_logs: bool = True,
    ):
        self.with_bulk_mode = with_bulk_mode
        self.bulk_mode_timeout_ms = bulk_mode_timeout_ms
        self.bulk_mode_max_records = bulk_mode_max_records
        self.bulk_mode_threshold = bulk_mode_threshold
        self.with_commit = with_commit
        self.fail_on_exception = fail_on_exception
        self.commit_interval_sec = commit_interval_sec

        self.initialize_logger(
            log_level=logger_level,
            with_aiokafka_logs=with_aiokafka_logs,
        )

        self.offsets: dict[TopicPartition, int] = {}

        self.topic_handlers = {handler.topic: handler for handler in self.handlers}
        self.log.debug(f"topic handlers: {self.topic_handlers}")

        self.log.info("KafkaConsumerEngine initialized")

    @final
    def initialize_logger(
        self,
        log_level: int = logging.INFO,
        with_aiokafka_logs: bool = True,
    ):
        logging.config.dictConfig(
            {
                "version": 1,
                "handlers": {
                    "aiokafka": {
                        "class": "logging.StreamHandler",
                        "formatter": "aiokafka",
                        "stream": "ext://sys.stdout",
                    },
                },
                "formatters": {
                    "aiokafka": {
                        "format": "\x1b[33;20m%(levelname)s | %(name)s | %(message)s\x1b[0m",
                    },
                },
                "loggers": {
                    "aiokafka": {
                        "handlers": ["aiokafka"] if with_aiokafka_logs else [],
                        "level": log_level,
                        "propagate": True,
                    },
                    "paperboy": {
                        "handlers": [],
                        "level": log_level,
                        "propagate": True,
                    },
                },
            }
        )

        self.log = logging.getLogger(f"paperboy.{self.__class__.__name__}")
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(PaperboyFormatter())

        self.log.addHandler(ch)

    @final
    async def __bulk_mode(self, handlers: list[type["BaseHandler"]] | None = None):
        in_bulk = True
        topics_in_bulk = {handler.topic: True for handler in handlers or self.handlers}

        self.consumer.subscribe([topic for topic in topics_in_bulk.keys()])

        while in_bulk:
            tasks = []
            messages = await self.consumer.getmany(
                timeout_ms=self.bulk_mode_timeout_ms,
                max_records=self.bulk_mode_max_records,
            )

            if not messages:
                break

            self.log.info(f"Received messages:{str({tp: len(msgs) for tp, msgs in messages.items()})}")
            for tp, msgs in messages.items():
                if tp.topic not in topics_in_bulk.keys():
                    topics_in_bulk[tp.topic] = True

                handler = self.topic_handlers[tp.topic]
                tasks.append(self.__loop.create_task(handler.handle(msgs)))

                if self.bulk_mode_threshold is not None:
                    highwater = self.consumer.highwater(tp)
                    if highwater <= msgs[-1].offset + self.bulk_mode_threshold or 0:  # type: ignore
                        topics_in_bulk[tp.topic] = False

                self.offsets[tp] = msgs[-1].offset + 1  # type: ignore

            await asyncio.wait(tasks)

            if self.with_commit:
                self.log.info(f"Committing offsets: {self.offsets}")
                await self.consumer.commit(self.offsets)

            in_bulk = any(topics_in_bulk.values())

        self.consumer.unsubscribe()

    @final
    async def __manual_commit(self):
        self.log.info(f"Starting manual commit loop, with a {self.commit_interval_sec}s interval")

        if not self.with_commit:
            self.log.warning("with_commit is False, cannot start manual commit task")
            return

        # To not restart from the beginning when you move from bulk mode to single mode
        # when the with_commit option is false, we need to set the offsets to the last
        # read message of each topic.
        while True:
            await asyncio.sleep(self.commit_interval_sec)
            self.log.debug("Committing offsets...")
            await self.consumer.commit()

    @final
    async def __single_mode(self):
        self.log.info("Starting Single Mode")

        # Assign the same TP as the bulk mode does before, based on the previous subscription
        if not self.offsets:
            self.consumer.subscribe([topic for topic in self.topic_handlers.keys()])
        else:
            self.consumer.assign([tp for tp in self.offsets.keys()])

        # Create the manual commit task
        self.__loop.create_task(self.__manual_commit())

        # If the with_commit option is false, we need to set the offsets to the last
        if self.offsets and not self.with_commit:
            self.log.info("with_commit is False, setting offsets to the last read message")
            self.log.debug(f"Offsets: {self.offsets}")
            for tp, offset in self.offsets.items():
                self.log.info(f"Seeking topic {tp} to offset {offset}")
                self.consumer.seek(tp, offset)

        async for msg in self.consumer:
            handler = self.topic_handlers[msg.topic]
            await handler.handle(msg)

    @final
    async def __consume(self):
        """
        Launches the consumer, and starts consuming messages from Kafka.
        """
        await self.consumer.start()

        if len(self.steps) > 0:
            self.log.info(f"Entering Steps Mode with {len(self.steps)} steps")
            for step_index, step in enumerate(self.steps):
                self.log.info(
                    f"Step {step_index + 1}/{len(self.steps)} | "
                    f"Entering Bulk Mode with topics: {[handler.topic for handler in step]}"
                )
                await self.__bulk_mode(handlers=step)

        elif self.with_bulk_mode:
            self.log.info(f"Entering Bulk Mode with topics: {self.topic_handlers.keys()}")
            await self.__bulk_mode()

        await self.__single_mode()

    @final
    async def start(self):
        """
        Starts the consumer, and handles the shutdown of the application.

        Use this method with asyncio.run, or in an asyncio loop.
        """
        self.__loop = asyncio.get_running_loop()
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
                self.__loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self.shutdown(s)))

            await self.__loop.create_task(self.__consume())
        except (ConsumerStoppedError, asyncio.CancelledError):
            pass
        except Exception as e:
            self.log.exception(e)
            await self.shutdown()

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
