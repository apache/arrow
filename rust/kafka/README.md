# Kafka reader for Arrow

## Usage

`KafkaBatchReader` exposes an `Iterator` interface for reading Kafka 
messages into Arrow `RecordBatch`.

The following parameters can be configured on `KafkaReaderConfig`:

- **max_batch_size** (usize): The maximum number of Kafka messages to
  include in a `RecordBatch`. Default 10,000.
- **poll_timeout** (Option<Duration>): The `Duration` to wait for new
  messages to be available from the Consumer. Use `None` for no timeout.
  Default 10 seconds.

Each `RecordBatch` contains the following collumns:

- **key** (Binary, nullable): The key of a message, if present.
- **payload** (Binary, nullable): The payload bytes of a message, if present.
- **topic** (Utf8): The topic of the message.
- **partition** (Int32): The partition of the message.
- **offset** (Int64): The offset of the message.

```rust
// `rdkafka::config::ClientConfig`
let mut client_config = ClientConfig::new();
client_config.set("group.id", &group);
client_config.set("bootstrap.servers", &broker);
client_config.set("auto.offset.reset", "earliest");

let mut config = KafkaReaderConfig::new(client_config);

let reader = KafkaBatchReader::new(config, &[&topic]);

for batch in reader {
    println!("{:?}", batch);
}
```
