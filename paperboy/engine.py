import asyncio
import logging
import logging.config
import signal
from typing import final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, ConsumerStoppedError, TopicPartition
from aiokafka.helpers import create_ssl_context

from paperboy.handler import BaseHandler
from paperboy.table import Table

from .logs import PaperboyFormatter


class EngineConsumerRebalancer(ConsumerRebalanceListener):
    def __init__(self, lock):
        self.lock = lock

        self.log = logging.getLogger("paperboy.ConsumerRebalancer")
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(PaperboyFormatter())
        self.log.addHandler(ch)

    async def on_partitions_revoked(self, revoked):
        self.log.info(f"Revoking partitions {revoked}, waiting on lock")
        # wait until a single batch processing is done
        async with self.lock:
            pass
        self.log.info("Partitions Revoked")

    async def on_partitions_assigned(self, assigned):
        pass


class Engine:
    """
    Engine used to consume messages from Kafka, using AIOKafka Library.

    Engine has 3 modes:
    - Single Mode: Consume messages one by one
    - Bulk Mode: Consume messages in bulk, using AIOKafka's getmany method
    - Step Mode: Consume messages in bulk, but in steps. Each step will consume messages from a list of topics.

    When you run the bulk mode, the engine will consume messages until :bulk_mode_threshdold: is reached
    on all topics. This is useful when you want to consume messages in bulk, but you don't want to wait
    for all topics to have messages to consume.

    If the bulk_mode_threshold is None, the engine will consume messages until the application is stopped
    or an error occurs.
    """

    __loop: asyncio.AbstractEventLoop

    @final
    def __init__(
        self,
        *handlers: type["BaseHandler"],
        steps: list[list[type["BaseHandler"]]] | None = None,
        bootstrap_servers: str | list[str] = "localhost:9092",
        client_id: str = "paperboy",
        group_id: str = "paperboy",
        auto_offset_reset: str = "earliest",
        sasl_mechanism: str = "PLAIN",
        security_protocol: str = "PLAINTEXT",
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        consumer_bootstrap_servers: str | list[str] | None = None,
        consumer_client_id: str | None = None,
        consumer_auto_offset_reset: str | None = None,
        consumer_sasl_mechanism: str | None = None,
        consumer_security_protocol: str | None = None,
        consumer_sasl_plain_username: str | None = None,
        consumer_sasl_plain_password: str | None = None,
        producer_bootstrap_servers: str | list[str] | None = None,
        producer_client_id: str | None = None,
        producer_sasl_mechanism: str | None = None,
        producer_security_protocol: str | None = None,
        producer_sasl_plain_username: str | None = None,
        producer_sasl_plain_password: str | None = None,
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
        self.handlers = handlers
        self.steps = steps or []
        self.with_bulk_mode = with_bulk_mode
        self.bulk_mode_timeout_ms = bulk_mode_timeout_ms
        self.bulk_mode_max_records = bulk_mode_max_records
        self.bulk_mode_threshold = bulk_mode_threshold
        self.with_commit = with_commit
        self.fail_on_exception = fail_on_exception
        self.commit_interval_sec = commit_interval_sec

        self.__initialize_logger(
            log_level=logger_level,
            with_aiokafka_logs=with_aiokafka_logs,
        )

        self.consumer_options = {
            "bootstrap_servers": consumer_bootstrap_servers or bootstrap_servers,
            "client_id": consumer_client_id or client_id,
            "group_id": group_id,
            "enable_auto_commit": False,
            "auto_offset_reset": consumer_auto_offset_reset or auto_offset_reset,
            "sasl_mechanism": consumer_sasl_mechanism or sasl_mechanism,
            "security_protocol": consumer_security_protocol or security_protocol,
            "sasl_plain_username": consumer_sasl_plain_username or sasl_plain_username,
            "sasl_plain_password": consumer_sasl_plain_password or sasl_plain_password,
        }

        self.producer_options = {
            "bootstrap_servers": producer_bootstrap_servers or bootstrap_servers,
            "client_id": producer_client_id or client_id,
            "sasl_mechanism": producer_sasl_mechanism or sasl_mechanism,
            "security_protocol": producer_security_protocol or security_protocol,
            "sasl_plain_username": producer_sasl_plain_username or sasl_plain_username,
            "sasl_plain_password": producer_sasl_plain_password or sasl_plain_password,
        }

        self.offsets: dict[TopicPartition, int] = {}
        self.tables: set["Table"] = set()

        self.log.info("KafkaConsumerEngine initialized")

    @final
    def __configure_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            **self.consumer_options,
            ssl_context=create_ssl_context(),
        )

    @final
    def __configure_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            **self.producer_options,
            ssl_context=create_ssl_context(),
        )

    @final
    def __initialize_logger(
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
    async def __bulk_mode(self, handlers: list[type["BaseHandler"]] | list["Table"] | None = None):
        in_bulk = True
        topic_handlers = {handler.topic: handler for handler in handlers or self.handlers}
        topics_in_bulk = {handler.topic: True for handler in handlers or self.handlers}

        self.consumer.subscribe(
            list(topic_handlers.keys()),
            listener=EngineConsumerRebalancer(asyncio.Lock()),
        )

        while in_bulk:
            tasks: list[asyncio.Task] = []

            messages = await self.consumer.getmany(
                timeout_ms=self.bulk_mode_timeout_ms,
                max_records=self.bulk_mode_max_records,
            )

            if not messages:
                break

            consumed_msgs = {tp: len(msgs) for tp, msgs in messages.items()}
            self.log.info(f"Received messages:{consumed_msgs}")
            for tp, msgs in messages.items():
                if tp.topic not in topics_in_bulk.keys():
                    topics_in_bulk[tp.topic] = True

                handler = topic_handlers[tp.topic]
                tasks.append(self.__loop.create_task(handler.handle(msgs)))

                if self.bulk_mode_threshold is not None:
                    highwater = self.consumer.highwater(tp)
                    if highwater <= msgs[-1].offset + self.bulk_mode_threshold:  # type: ignore
                        topics_in_bulk[tp.topic] = False

                self.offsets[tp] = msgs[-1].offset + 1  # type: ignore

            results = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            task_exceptions = [task.exception() for task in results[0]]

            if any(task_exceptions):
                for task_exception in task_exceptions:
                    if task_exception:
                        self.log.exception(task_exception)

                if self.fail_on_exception:
                    self.log.error("An error occured, shutting down...")
                    return
                else:
                    self.log.warning("An error occured, continuing...")

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
            self.consumer.subscribe(
                list(self.topic_handlers.keys()),
                listener=EngineConsumerRebalancer(asyncio.Lock()),
            )
        else:
            self.consumer.assign(list(self.offsets.keys()))

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
            try:
                await self.topic_handlers[msg.topic].handle(msg)
            except Exception as e:
                self.log.exception(e)
                if self.fail_on_exception:
                    self.log.error("An error occured, shutting down...")
                    return

    @final
    async def __consume(self):
        """
        Launches the consumer, and starts consuming messages from Kafka.
        """
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
    async def __initialize_tables(self):
        """
        Initializes all tables registered to the engine.
        """
        if not self.tables:
            return

        self.log.info("Initializing tables...")
        for table in self.tables:
            table.initialize(self.consumer, self.producer)

        self.log.info("Recovering tables")
        await self.__bulk_mode(handlers=list(self.tables))

        # Resetting offsets
        self.offsets = {}

    async def __initialize_signal_handling(self):
        """"""
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            self.__loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self.shutdown(s)))

    @final
    def register_table(self, table: "Table"):
        """
        Registers a table to the engine.

        The table will be initialized when the engine starts.
        """
        if any((table.table_name == reg_table.table_name for reg_table in self.tables)):
            raise ValueError(f"Table {table.table_name} already registered")

        if any((table.changelog_topic_name == reg_table.changelog_topic_name for reg_table in self.tables)):
            raise ValueError(f"Changelog topic {table.changelog_topic_name} already registered")

        if any((table.changelog_topic_name == handler.topic for handler in self.handlers)):
            raise ValueError(f"Changelog topic {table.changelog_topic_name} already handled")

        self.tables.add(table)

    @final
    async def start(self):
        """
        Starts the consumer, and handles the shutdown of the application.

        Use this method with asyncio.run, or in an asyncio loop.
        """
        self.__loop = asyncio.get_running_loop()
        try:
            self.consumer = self.__configure_consumer()
            self.producer = self.__configure_producer()

            await self.consumer.start()
            if self.producer:
                await self.producer.start()

            if self.consumer._enable_auto_commit:
                self.log.error(
                    "KafkaConsumerEngine handle commiting on its own, "
                    "please set enable_auto_commit=False in the AIOKafkaConsumer constructor. "
                    "If you don't want to commit, please set with_commit=False in the "
                    "Engine constructor."
                )
                await self.shutdown()
                return

            # Initialize Signal Handling
            await self.__initialize_signal_handling()

            # Create Topic Handlers
            self.topic_handlers = {handler.topic: handler for handler in self.handlers}

            # Set producer to all handlers
            for handler in self.handlers:
                handler.set_producer(self.producer)

            # Initialize tables
            await self.__initialize_tables()

            # Consume messages
            await self.__loop.create_task(self.__consume())

        except (ConsumerStoppedError, asyncio.CancelledError):
            pass
        except Exception:
            await self.shutdown()
            return

    @final
    async def shutdown(self, signal: signal.Signals | None = None):
        """
        Stops the consumer, and cancels all async tasks.
        """
        if signal:
            self.log.info(f"Received exit signal {signal.name}...")
        self.log.info("Stopping consumer...")
        await self.consumer.stop()
        self.log.info("Stopping producer...")
        await self.producer.stop()

        self.log.info("Cancelling async tasks...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]

        self.log.info(f"Tasks to cancel: {tasks}")
        await asyncio.gather(*tasks, return_exceptions=True)
        self.log.info("All tasks cancelled, exiting...")

        return
