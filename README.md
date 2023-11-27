# Paperboy

A wrapper around AIOKafka to make it easier to use, for message ingestion in your micro services

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


# Create a consumer engine
class KafkaConsumerEngine(Engine):
    handlers = [ExampleTopicHandler]

    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

# Create an instance of the consumer engine and start it
c = KafkaConsumerEngine(fail_on_exception=True)
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

### On Commiting

To avoid the loss of messages in case of failure, the engine will commit the offsets to Kafka, after the processing of the messages. But to adapt it to Bulk and Single modes, the engine handles the offset commiting with its own logic. Kafka don't like to have the offsets committed too often, so we need to be careful about that.

In order to work, set the `enable_auto_commit` option of the AIOKafkaConsumer to False.

In Bulk mode, the engine will commit the offsets after the processing of the batch of messages. In Single mode, the engine will commit the offsets after a certain amount of time, in a background task. This is to avoid committing the offsets too often, and overload the Kafka cluster.

An Exception will be raised if the option `enable_auto_commit` is set as True in the consumer, and the engine will stop.

There's an option to disable the commiting of the offsets, if you don't want to commit them. This is useful if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure. To do so, set the `with_commit` option of the engine to False.

```python

class FailingEngine(Engine):
    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True, # This will raise an exception
        )

class OKEngine(Engine):
    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False, # This is OK

c_no_commit = OKEngine(with_commit=False) # This is OK but won't commit the offsets            
c_with_commit = OKEngine(with_commit=True) # This is OK and it commits the offsets
```

### Configuring the consumer

The engine will create an instance of the AIOKafkaConsumer, based on the `configure_consumer` method. This method must return an instance of the AIOKafkaConsumer, with the right configuration.

You can use this method to add a Schema Registry configuration, SASL authentication, etc.

```python
# Example with python-schema-registry-client
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers.message_serializer import AvroMessageSerializer

from paperboy import Engine

class KafkaConsumerEngine(Engine):
    handlers = []

    def configure_consumer(self):
        serializer = AvroMessageSerializer(
            SchemaRegistryClient("http://localhost:8081")
        )

        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=serializer.decode_message,
            value_deserializer=serializer.decode_message,
        )
```

### Engine parameters

The engine can be parametered by passing some arguments to the constructor:

- **with_commit**: If set to True, the engine will commit the offsets after the processing of the messages. If set to False, the engine won't commit the offsets. Default: True

- **fail_on_exception**: If set to True, the engine will stop if an exception is raised in a handler. If set to False, the engine will log the exception, and continue the consumption of messages. Default: True

- **with_bulk_mode**: If set to True, the engine will consume messages from Kafka by batch (based on the number of records the engine can take, or a timeout), and dispatch them to the handlers, in a synchronous way. If set to False, the engine will consume messages from Kafka, and dispatch them to the handlers, message per message. Default: False

- **bulk_mode_timeout_ms**: Timeout in milliseconds, before the next messages batch is consumed. Default: 10000

- **bulk_mode_max_records**: Maximum number of records to consume in a batch. Default: 10000

- **bulk_mode_threshold**: Lag threshold, in number of messages, before the engine switches to single mode. Can be set to None, to stay in bulk mode. Default: 1000

- **commit_interval_ms**: Interval in milliseconds, before the engine commits the offsets in Single Mode. Default: 10000

- **logger_level**: Logging level of the engine. Default: logging.INFO

- **with_aiokafka_logs**: If set to True, the engine will log the aiokafka logs as well. Default: True

### Step mode

The engine can be run in Step mode, to consume certain topics before others. This is useful if you want to consume certain topics, before others. After finishing the last steps, the engine will go back to single mode with all the topics handlers.

To enable this mode, you need to define the `steps` attribute of the engine, as a list of lists of topics. Each list of topics will be consumed in bulk, and the engine will go back to single mode after finishing the last step.

```python
class KafkaConsumerEngine(Engine):
    handlers = [
        ExampleOneHandler,
        ExampleTwoHandler,
        ExampleThreeHandler,
    ]

    steps = [
        [ExampleOneHandler, ExampleTwoHandler,],
        [ExampleThreeHandler],
    ]

    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
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

class KafkaConsumerEngine(Engine):
    handlers = [ExampleHandler]
    
    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

c = KafkaConsumerEngine()
asyncio.run(c.start())
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

class KafkaConsumerEngine(Engine):
    handlers = [ExampleHandler]
    
    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

c = KafkaConsumerEngine(with_bulk_mode=True)
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
