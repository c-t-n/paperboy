import asyncio
import logging
from typing import Any, Generic, Self, final

import msgpack
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from paperboy.logs import PaperboyFormatter
from paperboy.stores import Store
from paperboy.stores.memory import MemoryStore
from paperboy.stores.rocksdb import RocksDBStore
from paperboy.stores.types import KT, VT


class Table(Generic[KT, VT]):
    """
    Represents a Kafka table for storing key-value pairs.
    Changes in the table are written to a changelog topic in Kafka.

    Args:
        table_name (str):
            The name of the table.

        store (str, optional):
            The store type for the table. Defaults to "memory://".

        changelog_topic_name (str | None, optional):
            The name of the changelog topic. Defaults to None.

        changelog_topic_num_partitions (int, optional):
            The number of partitions for the changelog topic. Defaults to 1.

        changelog_topic_replication_factor (int, optional):
            The replication factor for the changelog topic. Defaults to 1.

        changelog_topic_segment_ms (int, optional):
            The segment duration in milliseconds for the changelog topic. Defaults to 43200000.

        changelog_topic_min_cleanable_dirty_ratio (float, optional):
            The minimum cleanable dirty ratio for the changelog topic. Defaults to 0.01.

        changelog_topic_max_compaction_lag_ms (int, optional):
            The maximum compaction lag in milliseconds for the changelog topic. Defaults to 86400000.


    Raises:
        ValueError: If an unknown store type is provided.

    """

    store: Store

    @final
    def __init__(
        self,
        table_name: str,
        store: str = "memory://",
        changelog_topic_name: str | None = None,
        changelog_topic_num_partitions: int = 1,
        changelog_topic_replication_factor: int = 1,
        changelog_topic_segment_ms: int = 43200000,
        changelog_topic_min_cleanable_dirty_ratio: float = 0.01,
        changelog_topic_max_compaction_lag_ms: int = 86400000,
    ):
        self.__initialize_logger(table_name)

        self.__consumer: AIOKafkaConsumer | None = None
        self.__producer: AIOKafkaProducer | None = None
        self.__loop: asyncio.AbstractEventLoop | None = None

        # Changelog Topic configuration
        self.__changelog_topic_name = changelog_topic_name or f"{table_name}_changelog"
        self.__topic_replication_factor = changelog_topic_replication_factor
        self.__topic_num_partitions = changelog_topic_num_partitions
        self.__topic_segment_ms: int = changelog_topic_segment_ms
        self.__topic_min_cleanable_dirty_ratio: float = changelog_topic_min_cleanable_dirty_ratio
        self.__topic_max_compaction_lag_ms: int = changelog_topic_max_compaction_lag_ms

        self.__table_name = table_name

        match store:
            case "memory://":
                self.store = MemoryStore[KT, VT]()
            case "rocksdb://":
                self.store = RocksDBStore[KT, VT](path=f"./db/{table_name}")
            case _ as unknown_store:
                raise ValueError(f"Unknown store: {unknown_store}")

    def __initialize_logger(self, table_name: str):
        self.log = logging.getLogger(f"paperboy.table.{table_name}")
        ch = logging.StreamHandler()
        ch.setFormatter(PaperboyFormatter())

        self.log.addHandler(ch)

    def initialize(self, consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> Self:
        self.__consumer = consumer
        self.__producer = producer
        self.__loop = asyncio.new_event_loop()
        self.__create_changelog_topic()

        return self

    def __create_changelog_topic(self):
        if not self.__producer:
            raise ValueError("Producer cannot be None")

        admin_client = KafkaAdminClient(
            bootstrap_servers=self.__producer.client._bootstrap_servers,
            client_id=self.__producer.client._client_id,
            sasl_mechanism=self.__producer.client._sasl_mechanism,
            sasl_plain_username=self.__producer.client._sasl_plain_username,
            sasl_plain_password=self.__producer.client._sasl_plain_password,
            security_protocol=self.__producer.client._security_protocol,
            ssl_context=self.__producer.client._ssl_context,
            api_version=self.__producer.client._api_version,
            request_timeout_ms=self.__producer.client._request_timeout_ms,
            metadata_max_age_ms=self.__producer.client._metadata_max_age_ms,
            connections_max_idle_ms=self.__producer.client._connections_max_idle_ms,
        )

        topic = NewTopic(
            self.changelog_topic_name,
            num_partitions=self.__topic_num_partitions,
            replication_factor=self.__topic_replication_factor,
            topic_configs={
                "cleanup.policy": "compact",
                "min.cleanable.dirty.ratio": self.__topic_min_cleanable_dirty_ratio,
                "max.compaction.lag.ms": self.__topic_max_compaction_lag_ms,
                "segment.ms": self.__topic_segment_ms,
            },
        )

        try:
            tst = admin_client.create_topics([topic])
            self.log.info(f"Created topic {self.changelog_topic_name}: {tst}")

        except TopicAlreadyExistsError:
            self.log.info(f"Topic {self.changelog_topic_name} already exists")

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def changelog_topic_name(self) -> str:
        return self.__changelog_topic_name

    @property
    def topic(self) -> str:
        return self.__changelog_topic_name

    @property
    def is_registered(self) -> bool:
        return self.__consumer is not None and self.__producer is not None

    @final
    def get(self, key: KT) -> VT:
        if not self.is_registered:
            raise RuntimeError("Table is not registered")
        return self.store.get_item(key)

    @final
    async def set(self, key: KT, value: VT) -> None:
        if not self.__producer or not self.__loop:
            raise RuntimeError("Table is not registered")

        self.store.set_item(key, value)

        await self.__producer.send_and_wait(
            self.__changelog_topic_name,
            key=self.to_bytes(key),
            value=self.to_bytes(value),
        )

    @final
    async def delete(self, key: KT) -> None:
        if not self.__producer or not self.__loop:
            raise RuntimeError("Table is not registered")

        self.store.delete_item(key)

        await self.__producer.send(
            self.__changelog_topic_name,
            key=self.to_bytes(key),
            value=None,
        )

    @final
    def __missing__(self, key: KT) -> None:
        if not self.is_registered:
            raise RuntimeError("Table is not registered")

    async def handle(self, items: list[ConsumerRecord]) -> None:
        for msg in items:
            self.store.set_item(
                self.from_bytes(msg.key),
                self.from_bytes(msg.value),
            )

    @final
    def to_bytes(self, value: Any) -> bytes | None:
        return None if value is None else msgpack.dumps(value)

    @final
    def from_bytes(self, value: bytes | None) -> Any:
        return None if value is None else msgpack.loads(value)
