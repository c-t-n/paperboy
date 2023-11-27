# Paperboy

A wrapper around AIOKafka to make it easier to use, for message ingestion in your micro services

## Installation

```bash
pip install paperboy
```

## Quick Start

```python
import asyncio
from paperboy import PaperboyEngine, PaperboyHandler

# Create a topic handler
class ExampleTopicHandler(PaperboyHandler):
    topic = "example-topic"

    @classmethod
    async def on_message(cls, msg, ctx):
        print(f"Received message: [{msg.key}] {msg.value}")


# Create a consumer engine
class KafkaConsumerEngine(PaperboyEngine):
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

2 modes are available:

- **Single mode**: The engine will consume messages from Kafka, and dispatch them to the handlers, message per message. This is the default mode
- **Bulk mode**: The engine will consume messages from Kafka by batch (based on the number of records the engine can take, or a timeout), and dispatch them to the handlers, in a synchronous way. This mode is useful if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure.

Single mode is the default mode, but the engine will switch to Bulk mode if the lag of the consumer is too high, to avoid overloading the Kafka cluster. Using Bulk mode is recommended if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure.
Once the lag of the consumer is back to normal, the engine will switch back to Single mode, to keep the reactivity of the service.

### On Commiting

To avoid the loss of messages in case of failure, the engine will commit the offsets to Kafka, after the processing of the messages. But to adapt it to Bulk and Single modes, the engine handles the offset commiting with its own logic. Kafka don't like to have the offsets committed too often, so we need to be careful about that.

In order to work, set the `enable_auto_commit` option of the AIOKafkaConsumer to False.

In Bulk mode, the engine will commit the offsets after the processing of the batch of messages. In Single mode, the engine will commit the offsets after a certain amount of time, in a background task. This is to avoid committing the offsets too often, and overload the Kafka cluster.

An Exception will be raised if the option `enable_auto_commit` is set as True in the consumer, and the engine will stop.

There's an option to disable the commiting of the offsets, if you don't want to commit them. This is useful if you want to do some processing on the messages, and then commit the offsets, to avoid losing messages in case of failure. To do so, set the `with_commit` option of the engine to False.

```python

class FailingEnging(PaperboyEngine):
    def configure_consumer(self):
        return AIOKafkaConsumer(
            bootstrap_servers="localhost:9092",
            group_id="example-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True, # This will raise an exception
        )

class OKEngine(PaperboyEngine):
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

from paperboy import PaperboyEngine

class KafkaConsumerEngine(PaperboyEngine):
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

## Handlers

TBR
