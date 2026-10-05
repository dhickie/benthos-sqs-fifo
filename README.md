# benthos-sqs-fifo

This is a Benthos input plugin for working with SQS FIFO queues. 

The default SQS input plugin that comes with the current Redpanda Connect image can technically be pointed to SQS FIFO queues, but does not guarantee ordering of message processing. 

The only way, therefore, to guarantee ordering is to limit it to a single in-flight message at once, which severely limits throughput.

The input in this repository provides much higher throughput by grouping messages by message group ID and serving different groups concurrently.

## Running

A custom Benthos image that includes this input as well as all free connectors from the Redpanda Connect bundle is available on Dockerhub:

```shell
docker pull dominichickie/benthos-sqs-fifo:latest
```

The source for the input can be found under `src/input` and is available under the MIT licence.

## Testing

Tests are split into four categories:

- **Unit** - Tests smaller units of code using mocked interaction with SQS
- **Integration** - Tests the input component as a black box against emulated AWS resources using Floci
- **Stress** - Performs a stress test reading from a mocked queue of 5000 messages, including simulated SQS request latency
- **E2E** - Runs the built custom Benthos image against Floci, populates messages on the emulated queue, and checks messages sent to an output queue. The Benthos configuration used for the tests is under the `config` folder.

## Features

### High throughput

In its current version, assuming average SQS latency of 100ms this input is able to serve 300 messages per second with sufficiently fine-grained message group IDs.

This is currently bottlenecked by maximum message receive count and SQS latency, and will be improved in future versions.

### Guaranteed message ordering

The input is incapable of serving any subsequent messages from a message group until the message at the front of that group has been acknowledged by the output of the pipeline and deleted from the queue.

Therefore, message processing within a message group is guaranteed across the whole Benthos pipeline.

### Configurable retry behaviour

If processing of a message fails somewhere in the pipeline, the user has the option of either:

- Retrying the message up to some maximum number of attempts; or
- Immediately abandoning the message

If message processing is abandoned, any pending messages for that message group will also be abandoned.

This ensures that the messages can be picked up by a different application instance in case the error is instance specific, and preserves the message ordering guarantee.

It also means that message deadletter behaviour is configured purely at the queue level.

### Automatic message visibility management

In the event that a message has been received by a Benthos instance but is still pending processing some time later, the input automatically refreshes the visibility timeout of the message.

This ensures that it isn't processed multiple times by different instances.

## Configuration

The following configuration options must be set when using this input:

- `url` - The URL of the SQS FIFO queue that should be read from
- `region` - The AWS region the queue is in

The following configuration options are optional with sensible default values:

- `base_endpoint` - Default empty - The base AWS endpoint to use, when running with AWS emulators like Floci or LocalStack. Uses real AWS by default.
- `min_receive_batch_size` - Default 5, Min 1, Max 10 - The minimum amount of spare capacity that must be available in the buffer of in-flight messages before it will attempt to read more from the queue.
- `max_receive_batch_size` - Default 10, Min 1, Max 10 - The maximum number of messages to receive from SQS in one request.
- `max_in_flight_messages` - Default 30, Min 1 - The maximum number of messages that may have been received by the input, but not yet acknowledged and deleted from the queue.
- `max_processing_attempts` - Default 3, Min 1 - The maximum number of times a message can be attempted to be served to the rest of the pipeline, before it and the remainder of its message group is returned to the queue.
- `max_pending_acknowledgements` - Default 10, Min 1, Max 10 - The maximum number of pending acknowledgements from the pipeline to allow before processing them and deleting the messages from the queue.

### Configuration tuning tips

Below are some tips for tuning your configuration values to get the best throughput.

#### `min_receive_batch_size`

If your `max_in_flight_messages` value is small, then this value will also need to be relatively small.

If `min_receive_batch_size` is a significant percentage of `max_in_flight_messages`, it increases the likelihood that all remaining pending messages in the buffer will have been processed before the input has had a chance to re-populate it with new messages from SQS.

This would manifest as a sawtooth graph of message processing rate over time.

#### `max_receive_batch_size`

Most of the time, there isn't any reason to have this as anything other than 10.

One exception to this is if you have coarsely grained message group IDs, and a pipeline that takes a long time to process messages.

Each call to receive messages from an SQS FIFO queue will return as many messages as possible with the same message group ID. If you have coarsely grained message group IDs and these messages take a long time to process, that means your buffer will be full of messages that have to be processed sequentially and will severely limit your throughput.

In this scenario, it's better to have a smaller batch size so that subsequent calls will return messages from a different group, and give a broader diversity of message group IDs within the buffer.

#### `max_in_flight_messages`

If this value is too large, then it can increase your message waiting time. This is because messages are sat waiting in your buffer to be processed when they could have been being processed by a different Benthos instance.

Setting the value too small, on the other hand, means that the buffer can run dry while waiting on requests to SQS.

#### `max_processing_attempts`

If your pipeline is inherently flaky and requires regular retries to process messages successfully, then setting this value too low can significantly harm throughput.

This is because when a message is abandoned, it also abandons any other messages from the same message group, potentially removing a significant portion of your current message buffer.

High values can, however, also increase message processing time. If a Benthos instance has entered an unhealthy state, it could end up retrying messages a large number of times when that time would be better spent letting the message be picked up by a different instance.

#### `max_pending_acknowledgements`

Having a higher value promotes batching of the calls to delete messages from the queue, which is useful in high throughput scenarios where network resources may be under contention.

However, in lower throughput scenarios this delays the deletion of acknowledged messages from the queue. Given that subsequent messages from the same message group cannot be processed until the previous message has been deleted, this will harm through throughput.

In general, the higher the expected throughput, the higher this value should be. Any pending acknowledgements are processed once per second if this value isn't reached.