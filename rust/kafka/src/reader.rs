use std::time::Duration;

use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::CommitMode;
use rdkafka::error::KafkaError;

use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::array::StructArray;

use crate::batch::KafkaBatch;

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

    pub fn poll_timeout(&mut self, poll_timeout: Option<Duration>) -> &mut Self {
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
    /// Create new reader from `KafkaReaderConfig`.
    pub fn new(mut config: KafkaReaderConfig) -> Self {
        config.client_config.set("enable.partition.eof", "true");
        let consumer: BaseConsumer = config.client_config.create().unwrap();
        KafkaBatchReader { config, consumer }
    }

    /// Subscribe to a list of topics.
    pub fn subscribe(&self, topics: &[&str]) -> ArrowResult<()> {
        self.consumer.subscribe(topics).map_err(|e| {
            arrow::error::ArrowError::from_external_error(
                Box::new(e),
            )
        })
    }
    
    fn read_batch(&mut self) -> ArrowResult<RecordBatch> {
        let max_batch_size = self.config.max_batch_size;
        let mut batch = KafkaBatch::new(max_batch_size);

        // TODO: Track PartitionEOF for each TopicPartition?

        loop {
            match self.consumer.poll(self.config.poll_timeout) {
                Some(Ok(message)) => {
                    batch.process_message(&message)?;
                    match self.consumer.commit_message(&message, CommitMode::Async) {
                        Ok(_) => (),
                        Err(e) => {
                            return Err(arrow::error::ArrowError::from_external_error(
                                Box::new(e),
                            ));
                        }
                    }

                    if batch.len() == max_batch_size {
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

        let struct_array: StructArray = batch.into();
        Ok((&struct_array).into())
    }
}

impl Iterator for KafkaBatchReader {
    type Item = ArrowResult<RecordBatch>;

    /// Obtain up to `max_batch_size` messages from Kafka.
    /// Waits up to `poll_timeout` for messages.
    ///
    /// Returns if Kafka consumer returns None, PartitionEOF is
    /// reached, or `max_batch_size` messages are reached.
    ///
    /// If `RecordBatch` has zero rows, `Iterator` implementation
    /// will return `None` and terminate iteration.
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
