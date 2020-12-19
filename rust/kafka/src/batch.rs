use std::sync::Arc;

use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use arrow::error::Result as ArrowResult;
use arrow::datatypes::{DataType, Field};
use arrow::array::{
    Array, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, StructArray,
};

pub (crate) struct KafkaBatch {
    nread: usize,
    topics: StringBuilder,
    offsets: Int64Builder,
    partitions: Int32Builder,
    keys: BinaryBuilder,
    payloads: BinaryBuilder,
}

impl KafkaBatch {
    pub (crate) fn new(max_batch_size: usize) -> Self {
        let topics = StringBuilder::new(max_batch_size);
        let offsets = Int64Builder::new(max_batch_size);
        let partitions = Int32Builder::new(max_batch_size);
        let keys = BinaryBuilder::new(max_batch_size);
        let payloads = BinaryBuilder::new(max_batch_size);
        KafkaBatch {
            nread: 0,
            topics,
            offsets,
            partitions,
            keys,
            payloads
        }
    }

    pub (crate) fn len(&self) -> usize {
        self.nread
    }

    pub (crate) fn process_message(&mut self,
        message: &BorrowedMessage,
    ) -> ArrowResult<()> {
        self.nread += 1;
        match message.key() {
            Some(buf) => self.keys.append_value(buf)?,
            None => self.keys.append_null()?,
        }
        match message.payload() {
            Some(buf) => self.payloads.append_value(buf)?,
            None => self.payloads.append_null()?,
        }
        self.topics.append_value(message.topic())?;
        self.partitions.append_value(message.partition())?;
        self.offsets.append_value(message.offset())?;
        Ok(())
    }
}

impl From<KafkaBatch> for StructArray {
    fn from(mut batch: KafkaBatch) -> StructArray {
        let fields: Vec<(Field, Arc<dyn Array + 'static>)> = vec![
            (
                Field::new("key", DataType::Binary, true),
                Arc::new(batch.keys.finish()),
            ),
            (
                Field::new("payload", DataType::Binary, true),
                Arc::new(batch.payloads.finish()),
            ),
            (
                Field::new("topic", DataType::Utf8, false),
                Arc::new(batch.topics.finish()),
            ),
            (
                Field::new("partition", DataType::Int32, false),
                Arc::new(batch.partitions.finish()),
            ),
            (
                Field::new("offset", DataType::Int64, false),
                Arc::new(batch.offsets.finish()),
            ),
        ];

        fields.into()
    }
}


