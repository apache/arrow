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

//! Contains reader which reads parquet data into arrow array.

use crate::arrow::array_reader::{build_array_reader, ArrayReader, StructArrayReader};
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::schema::parquet_to_arrow_schema_by_columns;
use crate::errors::{ParquetError, Result};
use crate::file::reader::FileReader;
use arrow::array::StructArray;
use arrow::datatypes::{DataType as ArrowType, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::rc::Rc;
use std::sync::Arc;

/// Arrow reader api.
/// With this api, user can get arrow schema from parquet file, and read parquet data
/// into arrow arrays.
pub trait ArrowReader {
    /// Read parquet schema and convert it into arrow schema.
    fn get_schema(&mut self) -> Result<Schema>;

    /// Read parquet schema and convert it into arrow schema.
    /// This schema only includes columns identified by `column_indices`.
    fn get_schema_by_columns<T>(&mut self, column_indices: T) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>;

    /// Returns record batch reader from whole parquet file.
    ///
    /// # Arguments
    ///
    /// `batch_size`: The size of each record batch returned from this reader. Only the
    /// last batch may contain records less than this size, otherwise record batches
    /// returned from this reader should contains exactly `batch_size` elements.
    fn get_record_reader(
        &mut self,
        batch_size: usize,
    ) -> Result<Box<dyn RecordBatchReader>>;

    /// Returns record batch reader whose record batch contains columns identified by
    /// `column_indices`.
    ///
    /// # Arguments
    ///
    /// `column_indices`: The columns that should be included in record batches.
    /// `batch_size`: Please refer to `get_record_reader`.
    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<Box<dyn RecordBatchReader>>
    where
        T: IntoIterator<Item = usize>;
}

pub struct ParquetFileArrowReader {
    file_reader: Rc<dyn FileReader>,
}

impl ArrowReader for ParquetFileArrowReader {
    fn get_schema(&mut self) -> Result<Schema> {
        parquet_to_arrow_schema(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
        )
    }

    fn get_schema_by_columns<T>(&mut self, column_indices: T) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>,
    {
        parquet_to_arrow_schema_by_columns(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            column_indices,
        )
    }

    fn get_record_reader(
        &mut self,
        batch_size: usize,
    ) -> Result<Box<dyn RecordBatchReader>> {
        let column_indices = 0..self
            .file_reader
            .metadata()
            .file_metadata()
            .schema_descr_ptr()
            .num_columns();

        self.get_record_reader_by_columns(column_indices, batch_size)
    }

    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<Box<dyn RecordBatchReader>>
    where
        T: IntoIterator<Item = usize>,
    {
        let array_reader = build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            column_indices,
            self.file_reader.clone(),
        )?;

        Ok(Box::new(ParquetRecordBatchReader::try_new(
            batch_size,
            array_reader,
        )?))
    }
}

impl ParquetFileArrowReader {
    pub fn new(file_reader: Rc<dyn FileReader>) -> Self {
        Self { file_reader }
    }
}

struct ParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    schema: SchemaRef,
}

impl RecordBatchReader for ParquetRecordBatchReader {
    fn schema(&mut self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        self.array_reader
            .next_batch(self.batch_size)
            .map_err(|err| err.into())
            .and_then(|array| {
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        general_err!("Struct array reader should return struct array")
                            .into()
                    })
                    .and_then(|struct_array| {
                        RecordBatch::try_new(
                            self.schema.clone(),
                            struct_array.columns_ref(),
                        )
                    })
            })
            .map(|record_batch| {
                if record_batch.num_rows() > 0 {
                    Some(record_batch)
                } else {
                    None
                }
            })
    }
}

impl ParquetRecordBatchReader {
    pub fn try_new(
        batch_size: usize,
        array_reader: Box<dyn ArrayReader>,
    ) -> Result<Self> {
        // Check that array reader is struct array reader
        array_reader
            .as_any()
            .downcast_ref::<StructArrayReader>()
            .ok_or_else(|| general_err!("The input must be struct array reader!"))?;

        let schema = match array_reader.get_data_type() {
            &ArrowType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Ok(Self {
            batch_size,
            array_reader,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::arrow_reader::{ArrowReader, ParquetFileArrowReader};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use arrow::array::Array;
    use arrow::array::StructArray;
    use serde_json::Value::Array as JArray;
    use std::cmp::min;
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;
    use std::rc::Rc;

    #[test]
    fn test_arrow_reader() {
        let json_values = match serde_json::from_reader(get_test_file(
            "parquet/generated_simple_numerics/blogs.json",
        ))
        .expect("Failed to read json value from file!")
        {
            JArray(values) => values,
            _ => panic!("Input should be json array!"),
        };

        let parquet_file_reader =
            get_test_reader("parquet/generated_simple_numerics/blogs.parquet");

        let max_len = parquet_file_reader.metadata().file_metadata().num_rows() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);

        let mut record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        for i in 0..20 {
            let array: Option<StructArray> = record_batch_reader
                .next_batch()
                .expect("Failed to read record batch!")
                .map(|r| r.into());

            let (start, end) = (i * 60 as usize, (i + 1) * 60 as usize);

            if start < max_len {
                assert!(array.is_some());
                assert_ne!(0, array.as_ref().unwrap().len());
                let end = min(end, max_len);
                let json = JArray(Vec::from(&json_values[start..end]));
                assert_eq!(array.unwrap(), json)
            } else {
                assert!(array.is_none());
            }
        }
    }

    fn get_test_reader(file_name: &str) -> Rc<dyn FileReader> {
        let file = get_test_file(file_name);

        let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");

        Rc::new(reader)
    }

    fn get_test_file(file_name: &str) -> File {
        let mut path = PathBuf::new();
        path.push(env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined!"));
        path.push(file_name);

        File::open(path.as_path()).expect("File not found!")
    }
}
