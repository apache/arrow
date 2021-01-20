// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines common code used in execution plans

use std::fs;
use std::fs::metadata;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};

use array::{
    ArrayData, BooleanArray, Date32Array, DecimalArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
    Time32MillisecondArray, Time32SecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::PrimitiveBuilder,
    datatypes::{
        ArrowPrimitiveType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
    error::Result as ArrowResult,
};
use arrow::{
    array::{self, ArrayRef},
    datatypes::Schema,
};
use arrow::{
    array::{
        build_empty_fixed_size_list_array, build_empty_list_array, Date64Array,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    buffer::Buffer,
    datatypes::{DataType, SchemaRef, TimeUnit},
};
use futures::{Stream, TryStreamExt};

/// Stream of record batches
pub struct SizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
    index: usize,
}

impl SizedRecordBatchStream {
    /// Create a new RecordBatchIterator
    pub fn new(schema: SchemaRef, batches: Vec<Arc<RecordBatch>>) -> Self {
        SizedRecordBatchStream {
            schema,
            index: 0,
            batches,
        }
    }
}

impl Stream for SizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.batches.len() {
            self.index += 1;
            Some(Ok(self.batches[self.index - 1].as_ref().clone()))
        } else {
            None
        })
    }
}

impl RecordBatchStream for SizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataFusionError::from)
}

/// Recursively build a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, filenames: &mut Vec<String>, ext: &str) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list(path_name, filenames, ext)?;
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
    }
    Ok(())
}

/// Creates an empty (0 row) record batch with the specified schema
pub fn create_batch_empty(schema: &Schema) -> ArrowResult<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| create_empty_array(f.data_type()))
        .collect::<Result<_>>()
        .map_err(DataFusionError::into_arrow_external_error)?;

    RecordBatch::try_new(Arc::new(schema.to_owned()), columns)
}

fn create_empty_array(data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Float32 => {
            Ok(Arc::new(Float32Array::from(vec![] as Vec<f32>)) as ArrayRef)
        }
        DataType::Float64 => {
            Ok(Arc::new(Float64Array::from(vec![] as Vec<f64>)) as ArrayRef)
        }
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![] as Vec<i64>)) as ArrayRef),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![] as Vec<i32>)) as ArrayRef),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(vec![] as Vec<i16>)) as ArrayRef),
        DataType::Int8 => Ok(Arc::new(Int8Array::from(vec![] as Vec<i8>)) as ArrayRef),
        DataType::UInt64 => {
            Ok(Arc::new(UInt64Array::from(vec![] as Vec<u64>)) as ArrayRef)
        }
        DataType::UInt32 => {
            Ok(Arc::new(UInt32Array::from(vec![] as Vec<u32>)) as ArrayRef)
        }
        DataType::UInt16 => {
            Ok(Arc::new(UInt16Array::from(vec![] as Vec<u16>)) as ArrayRef)
        }
        DataType::UInt8 => Ok(Arc::new(UInt8Array::from(vec![] as Vec<u8>)) as ArrayRef),
        DataType::Utf8 => {
            Ok(Arc::new(StringArray::from(vec![] as Vec<&str>)) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            Ok(Arc::new(LargeStringArray::from(vec![] as Vec<&str>)) as ArrayRef)
        }
        DataType::Boolean => {
            Ok(Arc::new(BooleanArray::from(vec![] as Vec<bool>)) as ArrayRef)
        }
        DataType::Decimal(scale, precision) => {
            let array_data = ArrayData::builder(DataType::Decimal(*scale, *precision))
                .len(0)
                .add_buffer(Buffer::from(&[]))
                .build();
            Ok(Arc::new(DecimalArray::from(array_data)) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Ok(Arc::new(
            TimestampNanosecondArray::from_vec(vec![] as Vec<i64>, tz.clone()),
        ) as ArrayRef),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Ok(Arc::new(
            TimestampMicrosecondArray::from_vec(vec![] as Vec<i64>, tz.clone()),
        ) as ArrayRef),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Ok(Arc::new(
            TimestampMillisecondArray::from_vec(vec![] as Vec<i64>, tz.clone()),
        ) as ArrayRef),
        DataType::Timestamp(TimeUnit::Second, tz) => Ok(Arc::new(
            TimestampSecondArray::from_vec(vec![] as Vec<i64>, tz.clone()),
        ) as ArrayRef),
        DataType::Date32(_) => {
            Ok(Arc::new(Date32Array::from(vec![] as Vec<i32>)) as ArrayRef)
        }
        DataType::Date64(_) => {
            Ok(Arc::new(Date64Array::from(vec![] as Vec<i64>)) as ArrayRef)
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                Ok(Arc::new(Time32SecondArray::from(vec![] as Vec<i32>)) as ArrayRef)
            }
            TimeUnit::Millisecond => {
                Ok(Arc::new(Time32MillisecondArray::from(vec![] as Vec<i32>))
                    as ArrayRef)
            }
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                Err(DataFusionError::NotImplemented(format!(
                    "Cannot convert datatype {:?} to array",
                    data_type
                )))
            }
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                Err(DataFusionError::NotImplemented(format!(
                    "Cannot convert datatype {:?} to array",
                    data_type
                )))
            }
            TimeUnit::Microsecond => {
                Ok(Arc::new(Time64MicrosecondArray::from(vec![] as Vec<i64>))
                    as ArrayRef)
            }
            TimeUnit::Nanosecond => {
                Ok(Arc::new(Time64NanosecondArray::from(vec![] as Vec<i64>)) as ArrayRef)
            }
        },
        DataType::List(nested_type) => Ok(build_empty_list_array::<i32>(
            nested_type.data_type().clone(),
        )?),
        DataType::LargeList(nested_type) => Ok(build_empty_list_array::<i64>(
            nested_type.data_type().clone(),
        )?),
        DataType::FixedSizeList(nested_type, _) => Ok(build_empty_fixed_size_list_array(
            nested_type.data_type().clone(),
        )?),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::UInt8 => build_empty_dictionary::<UInt8Type>(value_type),
            DataType::UInt16 => build_empty_dictionary::<UInt16Type>(value_type),
            DataType::UInt32 => build_empty_dictionary::<UInt32Type>(value_type),
            DataType::UInt64 => build_empty_dictionary::<UInt64Type>(value_type),
            DataType::Int8 => build_empty_dictionary::<Int8Type>(value_type),
            DataType::Int16 => build_empty_dictionary::<Int16Type>(value_type),
            DataType::Int32 => build_empty_dictionary::<Int32Type>(value_type),
            DataType::Int64 => build_empty_dictionary::<Int64Type>(value_type),
            _ => unreachable!(),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Creating empty array for type {:?} is not yet implemented",
            data_type
        ))),
    }
}

fn build_empty_dictionary<T: ArrowPrimitiveType>(
    value_type: &DataType,
) -> Result<ArrayRef> {
    let values: ArrayRef = create_empty_array(value_type)?;
    let mut keys_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(0);
    let dict_array = keys_builder.finish_dict(values);
    Ok(Arc::new(dict_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_create_batch_empty() {
        use DataType::*;

        let schema = Schema::new(vec![
            Field::new("c1", Utf8, false),
            Field::new("c2", UInt32, false),
            Field::new("c3", Int8, false),
            Field::new("c4", Int16, false),
            Field::new("c5", Int32, false),
            Field::new("c6", Int64, false),
            Field::new("c7", UInt8, false),
            Field::new("c8", UInt16, false),
            Field::new("c9", UInt32, false),
            Field::new("c10", UInt64, false),
            Field::new("c11", Float32, false),
            Field::new("c12", Float64, false),
            Field::new("c13", Utf8, false),
            Field::new("c14", Decimal(10, 10), false),
            Field::new("c15", Timestamp(TimeUnit::Second, None), false),
            Field::new("c16", Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("c17", Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("c18", Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("c19", Boolean, false),
            Field::new("20", Dictionary(Box::new(UInt8), Box::new(Utf8)), false),
            Field::new("21", Dictionary(Box::new(UInt16), Box::new(Utf8)), false),
            Field::new("22", Dictionary(Box::new(UInt32), Box::new(Utf8)), false),
            Field::new("23", Dictionary(Box::new(UInt64), Box::new(Utf8)), false),
            Field::new("24", Dictionary(Box::new(Int8), Box::new(Utf8)), false),
            Field::new("25", Dictionary(Box::new(Int16), Box::new(Utf8)), false),
            Field::new("26", Dictionary(Box::new(Int32), Box::new(Utf8)), false),
            Field::new("27", Dictionary(Box::new(Int64), Box::new(Utf8)), false),
            // try non string dictionary
            Field::new("28", Dictionary(Box::new(UInt8), Box::new(Int64)), false),
            Field::new(
                "29",
                Dictionary(Box::new(UInt8), Box::new(LargeUtf8)),
                false,
            ),
        ]);

        let batch = create_batch_empty(&schema).unwrap();
        assert_eq!(batch.columns().len(), 29);
        assert_eq!(batch.num_rows(), 0);

        for (i, array) in batch.columns().iter().enumerate() {
            assert_eq!(array.len(), 0, "Array[{}] was zero length", i);
        }
    }
}
