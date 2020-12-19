use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

pub use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::Message;

#[derive(Clone)]
pub struct KafkaReaderConfig {
    /// Maximum number of Kafka records in one Arrow Batch. Default 10,000.
    max_batch_size: usize,
    /// Maximum time to wait when polling consumer for new messages. Default 10 seconds.
    poll_timeout: Option<Duration>,
    /// Kafka consumer config. Re-export of `rdkafka::config::ClientConfig`.
    client_config: ClientConfig,
}

impl KafkaReaderConfig {
    pub fn new(client_config: ClientConfig) -> Self {
        KafkaReaderConfig {
            max_batch_size: 10_000,
            poll_timeout: Some(Duration::new(10, 0)),
            client_config,
        }
    }

    pub fn timeout(&mut self, poll_timeout: Option<Duration>) -> &mut Self {
        self.poll_timeout = poll_timeout;
        self
    }

    pub fn max_batch_size(&mut self, max_batch_size: usize) -> &mut Self {
        self.max_batch_size = max_batch_size;
        self
    }
}

pub struct KafkaBatchReader {
    config: KafkaReaderConfig,
    consumer: BaseConsumer,
}

impl KafkaBatchReader {
    pub fn new(mut config: KafkaReaderConfig, topics: &[&str]) -> Self {
        config.client_config.set("enable.partition.eof", "true");
        let consumer: BaseConsumer = config.client_config.create().unwrap();
        consumer.subscribe(topics).unwrap();
        KafkaBatchReader { config, consumer }
    }

    /// Obtain up to `max_batch_size` messages from Kafka.
    /// Waits up to `poll_timeout` for messages.
    ///
    /// Returns if Kafka consumer returns None, PartitionEOF is
    /// reached, or `max_batch_size` messages are reached.
    ///
    /// If `RecordBatch` returned by this method has zero rows,
    /// `Iterator` implementation will return `None` and terminate
    /// iteration.
    fn read_batch(&mut self) -> ArrowResult<RecordBatch> {
        let mut nread = 0;
        let max_batch_size = self.config.max_batch_size;
        let mut topics = StringBuilder::new(max_batch_size);
        let mut offsets = Int64Builder::new(max_batch_size);
        let mut partitions = Int32Builder::new(max_batch_size);
        let mut keys = BinaryBuilder::new(max_batch_size);
        let mut payloads = BinaryBuilder::new(max_batch_size);

        // TODO: Track PartitionEOF for each TopicPartition?

        loop {
            match self.consumer.poll(self.config.poll_timeout) {
                Some(Ok(message)) => {
                    nread += 1;
                    match message.key() {
                        Some(buf) => keys.append_value(buf)?,
                        None => keys.append_null()?,
                    }
                    match message.payload() {
                        Some(buf) => payloads.append_value(buf)?,
                        None => payloads.append_null()?,
                    }
                    topics.append_value(message.topic())?;
                    partitions.append_value(message.partition())?;
                    offsets.append_value(message.offset())?;
                    match self.consumer.commit_message(&message, CommitMode::Async) {
                        Ok(_) => (),
                        Err(e) => {
                            return Err(arrow::error::ArrowError::from_external_error(
                                Box::new(e),
                            ))
                        }
                    }
                    if nread == max_batch_size {
                        break;
                    }
                }
                Some(Err(KafkaError::PartitionEOF(_))) => {
                    break;
                }
                Some(Err(e)) => {
                    return Err(arrow::error::ArrowError::from_external_error(Box::new(
                        e,
                    )))
                }
                None => break,
            }
        }

        let fields: Vec<(Field, Arc<dyn Array + 'static>)> = vec![
            (
                Field::new("key", DataType::Binary, true),
                Arc::new(keys.finish()),
            ),
            (
                Field::new("payload", DataType::Binary, true),
                Arc::new(payloads.finish()),
            ),
            (
                Field::new("topic", DataType::Utf8, false),
                Arc::new(topics.finish()),
            ),
            (
                Field::new("partition", DataType::Int32, false),
                Arc::new(partitions.finish()),
            ),
            (
                Field::new("offset", DataType::Int64, false),
                Arc::new(offsets.finish()),
            ),
        ];

        let struct_array: StructArray = fields.into();
        Ok((&struct_array).into())
    }
}

impl Iterator for KafkaBatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_batch() {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    Some(Ok(batch))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}
