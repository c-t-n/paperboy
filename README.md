# Paperboy

A wrapper around AIOKafka to make it easier to use, for message ingestion in your micro services

[[_TOC_]]

## Installation

```bash
pip install kafka-paperboy
```

## Quick Start

```python
import asyncio
from paperboy import Engine, Handler

# Create a topic handler
class ExampleTopicHandler(Handler):
    topic = "example-topic"

    @classmethod
    async def on_message(cls, msg, ctx):
        print(f"Received message: [{msg.key}] {msg.value}")

# Create an instance of the consumer engine and start it
c = Engine(
    ExampleTopicHandler,
    bootstrap_servers="localhost:9092",
    group_id="example-group",
    fail_on_exception=True
)
asyncio.run(c.start())
```

## The Engine

### Main idea

The engine here is a wrapper around the AIOKafkaConsumer, that will handle the consumption of messages from Kafka, and dispatch them to the right handler, based on the topic.

Rather than defining all the consumption logic in a single place, the engine will dispatch to handlers, that you can parameter by coding their lifecycle methods.

This engine is meant to be used in a micro service architecture, where you have multiple services, each one consuming messages from a set of topics, and doing some processing on them.

3 modes are available:

- **Single mode**: The engine will consume messages from Kafka, and dispatch them to the handlers, message per message. This is the default mode
- **Bulk mode**: The engine will consume messages from Kafka by batch (based on the number of records the engine can take, or a timeout), and dispatch them to the handlers, in a synchronous way. This mode is useful if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure.
- **Step mode**: The engine will consume certain topics from Kafka, and dispatch them to the handlers, in bulkd. This mode is useful if you want to consume certain topics, before others. After finishing the last steps, the engine will go back to single mode with all the topics handlers.

Single mode is the default mode, but the engine will switch to Bulk mode if the lag of the consumer is too high, to avoid overloading the Kafka cluster. Using Bulk mode is recommended if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure.
Once the lag of the consumer is back to normal, the engine will switch back to Single mode, to keep the reactivity of the service.

### Step mode

The engine can be run in Step mode, to consume certain topics before others. This is useful if you want to consume certain topics, before others. After finishing the last steps, the engine will go back to single mode with all the topics handlers.

To enable this mode, you need to define the `steps` attribute of the engine, as a list of lists of topics. Each list of topics will be consumed in bulk, and the engine will go back to single mode after finishing the last step.

```python
import asyncio
from paperboy import Engine

c = Engine(
    ExampleOneHandler,
    ExampleTwoHandler,
    ExampleThreeHandler,
    steps = [
        [ExampleOneHandler, ExampleTwoHandler,],
        [ExampleThreeHandler],
    ]
    bootstrap_servers="localhost:9092",
    group_id="example-group",
    fail_on_exception=True
)
asyncio.run(c.start())
```

## Handlers

Handlers are the classes that will handle the messages from a Kafka cluster.
Those handlers treats a series of messages, based on the topic they are subscribed to.
A handler exposes a series of class methods, used to handle the messages on certain points of their lifecycle, meant to be overriden by the user.

### Handler Quick Start

```python
import asyncio
from paperboy import Handler, Engine

class ExampleHandler(Handler):
    topic = "example-topic"

    @classmethod
    async def on_message(cls, msg, ctx):
        # Lifecycle method called when a message is received
        print(f"Received message: [{msg.key}] {msg.value}")

    @classmethod
    async def on_error(cls, e, ctx, exc) -> Exception | None:
        # Lifecycle method called when an error is raised in the handler
        print(f"Handler error: {exc}")
        return e

c = Engine(
    ExampleHandler,
    bootstrap_servers="localhost:9092",
    group_id="example-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)
asyncio.run(c.start())
```

### Handler class

You can define the following class attributes on a handler:
    - **topic**: The topic to consume
    - **key_serializer**: The serializer for the key of the message
    - **value_serializer**: The serializer for the value of the message

Serializers must be a callable, that takes a bytes object as input, and returns a deserialized object.

```python
class ExampleHandler(Handler):
    topic: str = "example-topic" # Topic to consume
    key_serializer: Callable = lambda x: json.loads(x.decode("utf-8")) # Serializer for the key of the message
    value_serializer: Callable = lambda x: json.loads(x.decode("utf-8")) # Serializer for the value of the message
```

This works alors with Avro schemas, using the `python-schema-registry-client` library

```python
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers.avro import AvroMessageSerializer

client = SchemaRegistryClient(url="http://localhost:8081")
serializer = AvroMessageSerializer(client)

class ExampleHandler(Handler):
    topic: str = "example-topic" # Topic to consume
    key_serializer: Callable = serializer.decode_message # Serializer for the key of the message
    value_serializer: Callable = serializer.decode_message # Serializer for the value of the message
```

### Handler lifecycle methods

#### on_message

```python
    @classmethod
    async def on_message(cls, msg: ConsumerRecord, ctx: Context):
        pass
```

This method is called when a message is received from Kafka. It takes 2 arguments:

- **msg**: The message received from Kafka, as a ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns nothing

#### on_tombstone

```python
    @classmethod
    async def on_tombstone(cls, msg: ConsumerRecord, ctx: Context):
        pass
```

This method is called when a tombstone message is received from Kafka.
Tombstone messages are messages with a null value, used to delete a key from a compacted topic.

It takes 2 arguments:

- **msg**: The message received from Kafka, as a ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns nothing

#### on_error

```python
    @classmethod
    async def on_error(cls, e: Exception, msg: ConsumerRecord, ctx: Context) -> Exception | None:
        pass
```

This method is called when an error is raised in the handler.

It takes 3 arguments:

- **e**: The exception raised in the handler
- **msg**: The message received from Kafka, as a ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns an Exception, or None. If an Exception is returned, it will be raised in the engine, and the engine will stop. If None is returned, the engine will log the exception, and continue the consumption of messages.

#### did_receive_message

```python
    @classmethod
    async def did_receive_message(cls, msg: ConsumerRecord, ctx: Context) -> ConsumerRecord:
        return msg
```

This method is called after the message reception.
This method is useful if you want to do some processing on the message, before it is handled by the on_message method.

It takes 2 arguments:

- **msg**: The message received from Kafka, as a ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns a ConsumerRecord.

#### on_finished_handling

```python
    @classmethod
    async def on_finished_handling(cls, msgs: ConsumerRecord | list[ConsumerRecord], ctx: Context):
        pass
```

This method is called after the overall message handling.
This method is useful if you want to do some processing on the message, after it has been handled by the on_message / on_tombstone method.

It takes 2 arguments:

- **msgs**: The message received from Kafka, as a ConsumerRecord (from aiokafka), or a list of ConsumerRecord if the engine is in bulk mode
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns nothing.

#### define_context

```python
    @classmethod
    async def define_context(cls) -> Context | None:
        return {}
```

This method is called before the message handling, and is used to define the context of the handling.
The context is a dictionary, that will be passed to the lifecycle methods, and can be used to store some data, that will be used in the lifecycle methods.

It takes no arguments.

Returns a dictionary, or None.

### Bulk Handlers

Bulk handlers are handlers that will handle a batch of messages, rather than a single message.
Those handlers are usefull if you want to apply a specific logic on a batch of messages, rather than a single message, via dedicated lifecycle methods.

You also need to specify normal lifecycle methods with the bulk ones, to handle the messages when the engine runs in Single Mode

```python
import asyncio
from paperboy import BulkHandler, Engine

class ExampleProbeHandler(BulkHandler):

    @classmethod
    async def on_bulk(cls, msgs: list[ConsumerRecord], ctx: Context):
        # Lifecycle method called when a batch of messages is received
        print(f"Received {len(msgs)} messages")

    @classmethod
    async def on_message(cls, msg: ConsumerRecord, ctx: Context):
        # Lifecycle method called when a message is received
        print(f"Received message: [{msg.key}] {msg.value}")


c = KafkaConsumerEngine(
    ExampleHandler,
    bootstrap_servers="localhost:9092",
    with_bulk_mode=True
    group_id="example-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)
asyncio.run(c.start())
```

In this example, the engine will run in Bulk Mode, and will call the on_bulk_message method, when a batch of messages is received, rather than applying the on_message method on each message. The on_message method is still needed, to handle the messages when the engine runs in Single Mode.

### BulkHandler parameters

#### on_bulk

```python
    @classmethod
    async def on_bulk(cls, msgs: list[ConsumerRecord], ctx: Context):
        pass
```

This method is called when a batch of messages is received from Kafka.

It takes 2 arguments:

- **msgs**: The messages received from Kafka, as a list of ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns nothing

#### on_bulk_error

```python
    @classmethod
    async def on_bulk_error(cls, e: Exception, msgs: list[ConsumerRecord], ctx: Context) -> Exception | None:
        pass
```

This method is called when an error is raised in the handler, when the engine is in Bulk Mode.

It takes 3 arguments:

- **e**: The exception raised in the handler
- **msgs**: The messages received from Kafka, as a list of ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns an Exception, or None. If an Exception is returned, it will be raised in the engine, and the engine will stop. If None is returned, the engine will log the exception, and continue the consumption of messages.

#### did_receive_bulk_messages

```python
    @classmethod
    async def did_receive_bulk_messages(cls, msgs: list[ConsumerRecord], ctx: Context) -> list[ConsumerRecord]:
        return msgs
```

This method is called after the batch of messages reception. This method is useful if you want to do some processing on the batch of messages, before it is handled by the on_bulk method.

It takes 2 arguments:

- **msgs**: The messages received from Kafka, as a list of ConsumerRecord (from aiokafka)
- **ctx**: The context of the message, as a Context object (defined in the define_context method)

Returns a list of ConsumerRecord.

## Tables

Tables are key/value stores, that can be used to store computed data from the messages, and can be used in the handlers.
To ensure Table consistency, each mutation of the table is stored in a compacted topic in Kafka, and the table is reloaded from the topic at the start of the engine.

### Table Quick Start

```python
import asyncio
from paperboy import Table, Engine

metrics_table = Table(
    "metrics",
    store="memory://"
)

class PageViewHandler(Handler):
    topic = "page-views"

    @classmethod
    async def on_message(cls, msg, ctx):
        # Access a specific key of the table (should return None if the key is not set)
        clicks = metrics_table.get("page_views")
        # Set a new value
        await metrics_table.set("page_views", (clicks or 0) + 1)

    @classmethod
    async def on_tombstone(cls, msg, ctx):
        # Delete a key
        await metrics_table.delete("page_views")

engine = Engine(
    PageViewHandler,
    bootstrap_servers="localhost:9092",
    group_id="example-group",
    fail_on_exception=True
)

# Register the table in the engine (the table will be reloaded from the topic at the start of the engine)
engine.register_table(metrics_table)

asyncio.run(engine.start())
```

### Table stores

Tables uses 2 types of backend to store the data:

- **InMemory**: The data is stored in memory, and is lost when the engine stops
- **RocksDB**: The data is stored in a RocksDB database, and is persisted in the worker

InMemory database is preferred in development, but RocksDB is recommended in production, to keep the data in case of failure.

```python
from paperboy import Table

# InMemory database
metrics_table = Table("metrics", store="memory://")

# RocksDB database
persisted_metrics_table = Table("prod_metrics", store="rocksdb://")
```

### Table methods

```python
from paperboy import Table

table = Table(
    # Table name
    "metrics",
    # Table store
    store="memory://" if DEBUG else "rocksdb://",
    # Changelog topic name in Kafka Cluster
    changelog_topic_name="metrics-changelog",
    # Changelog topic configuration
    changelog_topic_num_partitions = 1,
    changelog_topic_replication_factor = 1,
    changelog_topic_segment_ms = 43200000,
    changelog_topic_min_cleanable_dirty_ratio = 0.01,
    changelog_topic_max_compaction_lag_ms = 86400000,        
)

# Get a value from the table (returns None if the key is not set)
value = table.get("key") 

# Set a value in the table
await table.set("key", "value")

# Delete a value from the table
await table.delete("key")
```
