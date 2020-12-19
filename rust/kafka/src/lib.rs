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
use std::sync::Arc;

pub struct KafkaBatchReader {
    max_batch_size: usize,
    consumer: BaseConsumer,
}

impl KafkaBatchReader {
    pub fn new(mut config: ClientConfig, topics: &[&str], max_batch_size: usize) -> Self {
        config.set("enable.partition.eof", "true");
        let consumer: BaseConsumer = config.create().unwrap();
        consumer.subscribe(topics).unwrap();
        KafkaBatchReader {
            max_batch_size,
            consumer,
        }
    }
}

impl Iterator for KafkaBatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut nread = 0;
        let mut topics = StringBuilder::new(self.max_batch_size);
        let mut offsets = Int64Builder::new(self.max_batch_size);
        let mut partitions = Int32Builder::new(self.max_batch_size);
        let mut keys = BinaryBuilder::new(self.max_batch_size);
        let mut payloads = BinaryBuilder::new(self.max_batch_size);

        loop {
            println!("Loop...");
            match self.consumer.poll(None) {
                Some(Ok(message)) => {
                    nread += 1;
                    match message.key() {
                        Some(buf) => keys.append_value(buf).unwrap(),
                        None => keys.append_null().unwrap(),
                    }
                    match message.payload() {
                        Some(buf) => payloads.append_value(buf).unwrap(),
                        None => payloads.append_null().unwrap(),
                    }
                    topics.append_value(message.topic()).unwrap();
                    partitions.append_value(message.partition()).unwrap();
                    offsets.append_value(message.offset()).unwrap();
                    self.consumer
                        .commit_message(&message, CommitMode::Async)
                        .unwrap();
                    if nread == self.max_batch_size {
                        break;
                    }
                }
                Some(Err(KafkaError::PartitionEOF(_))) => {
                    break;
                }
                Some(Err(e)) => panic!("{:?}", e),
                None => (),
            }
        }

        println!("Read {} messages.", nread);
        if nread == 0 {
            None
        } else {
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
            Some(Ok((&struct_array).into()))
        }
    }
}
