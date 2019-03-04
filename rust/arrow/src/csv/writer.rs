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

//! CSV Writer
//!
//! This CSV writer allows Arrow data (in record batches) to be written as CSV files.
//! The writer does not support writing `ListArray` and `StructArray`.
//!
//! Example:
//!
//! ```
//! use arrow::array::*;
//! use arrow::csv;
//! use arrow::datatypes::*;
//! use arrow::record_batch::RecordBatch;
//! use arrow::util::test_util::get_temp_file;
//! use std::fs::File;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("c1", DataType::Utf8, false),
//!     Field::new("c2", DataType::Float64, true),
//!     Field::new("c3", DataType::UInt32, false),
//!     Field::new("c3", DataType::Boolean, true),
//! ]);
//! let c1 = BinaryArray::from(vec![
//!     "Lorem ipsum dolor sit amet",
//!     "consectetur adipiscing elit",
//!     "sed do eiusmod tempor",
//! ]);
//! let c2 = PrimitiveArray::<Float64Type>::from(vec![
//!     Some(123.564532),
//!     None,
//!     Some(-556132.25),
//! ]);
//! let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
//! let c4 = PrimitiveArray::<BooleanType>::from(vec![Some(true), Some(false), None]);
//!
//! let batch = RecordBatch::new(
//!     Arc::new(schema),
//!     vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
//! );
//!
//! let file = get_temp_file("out.csv", &[]);
//!
//! let writer = csv::Writer::new(file);
//! writer.write(vec![&batch, &batch]).unwrap();
//! ```

use csv as csv_crate;

use std::fs::File;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

fn write_primitive_value<T>(array: &ArrayRef, i: usize) -> String
where
    T: ArrowNumericType,
    T::Native: ::std::string::ToString,
{
    let c = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    c.value(i).to_string()
}

/// A CSV writer
pub struct Writer {
    /// The file to write to
    file: File,
    /// Column delimiter. Defaults to `b','`
    delimiter: u8,
    /// Whether file should be written with headers. Defaults to `true`
    has_headers: bool,
}

impl Writer {
    /// Create a new CsvWriter from a file, with default options
    pub fn new(file: File) -> Self {
        Writer {
            file,
            delimiter: b',',
            has_headers: true,
        }
    }

    /// Write a vector of record batches to a file
    pub fn write(&self, batches: Vec<&RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(ArrowError::CsvError(
                "No record batches supplied to the CSV writer".to_string(),
            ));
        }
        let mut builder = csv_crate::WriterBuilder::new();
        let mut wtr = builder.delimiter(self.delimiter).from_writer(&self.file);
        let num_columns = batches[0].num_columns();
        if self.has_headers {
            let mut headers: Vec<String> = Vec::with_capacity(num_columns);
            &batches[0]
                .schema()
                .fields()
                .iter()
                .for_each(|field| headers.push(field.name().to_string()));
            wtr.write_record(&headers[..])?;
        }

        for batch in batches {
            for row_index in 0..batch.num_rows() {
                // TODO: it'd be more efficient if we could create `record: Vec<&[u8]>
                let mut record: Vec<String> = Vec::with_capacity(batch.num_columns());
                for col_index in 0..batch.num_columns() {
                    let col = batch.column(col_index);
                    if col.is_null(row_index) {
                        // write an empty value
                        record.push(String::from(""));
                        continue;
                    }
                    let string = match col.data_type() {
                        DataType::Float64 => {
                            write_primitive_value::<Float64Type>(col, row_index)
                        }
                        DataType::Float32 => {
                            write_primitive_value::<Float32Type>(col, row_index)
                        }
                        DataType::Int8 => {
                            write_primitive_value::<Int8Type>(col, row_index)
                        }
                        DataType::Int16 => {
                            write_primitive_value::<Int16Type>(col, row_index)
                        }
                        DataType::Int32 => {
                            write_primitive_value::<Int32Type>(col, row_index)
                        }
                        DataType::Int64 => {
                            write_primitive_value::<Int64Type>(col, row_index)
                        }
                        DataType::UInt8 => {
                            write_primitive_value::<UInt8Type>(col, row_index)
                        }
                        DataType::UInt16 => {
                            write_primitive_value::<UInt16Type>(col, row_index)
                        }
                        DataType::UInt32 => {
                            write_primitive_value::<UInt32Type>(col, row_index)
                        }
                        DataType::UInt64 => {
                            write_primitive_value::<UInt64Type>(col, row_index)
                        }
                        DataType::Boolean => {
                            let c = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                            c.value(row_index).to_string()
                        }
                        DataType::Utf8 => {
                            let c = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                            String::from_utf8(c.value(row_index).to_vec())?
                        }
                        t => {
                            // List and Struct arrays not supported by the writer, any
                            // other type needs to be implemented
                            return Err(ArrowError::CsvError(format!(
                                "CSV Writer does not support {:?} data type",
                                t
                            )));
                        }
                    };

                    record.push(string);
                }
                wtr.write_record(&record[..])?;
            }
            wtr.flush()?;
        }

        Ok(())
    }
}

/// A CSV writer builder
pub struct WriterBuilder {
    /// Optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Whether to write column names as file headers. Defaults to `true`
    has_headers: bool,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self {
            has_headers: true,
            delimiter: None,
        }
    }
}

impl WriterBuilder {
    /// Create a new builder for configuring CSV writing options.
    ///
    /// To convert a builder into a writer, call `WriterBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate arrow;
    ///
    /// use arrow::csv;
    /// use std::fs::File;
    ///
    /// fn example() -> csv::Writer {
    ///     let file = File::create("target/out.csv").unwrap();
    ///
    ///     // create a builder that doesn't write headers
    ///     let builder = csv::WriterBuilder::new().has_headers(false);
    ///     let writer = builder.build(file);
    ///
    ///     writer
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to write headers
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Create a new `Writer`
    pub fn build(self, file: File) -> Writer {
        Writer {
            file,
            delimiter: self.delimiter.unwrap_or(b','),
            has_headers: self.has_headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datatypes::{Field, Schema};
    use crate::util::test_util::get_temp_file;
    use std::io::Read;
    use std::sync::Arc;

    #[test]
    fn test_write_csv() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c3", DataType::Boolean, true),
        ]);

        let c1 = BinaryArray::from(vec![
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = PrimitiveArray::<Float64Type>::from(vec![
            Some(123.564532),
            None,
            Some(-556132.25),
        ]);
        let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
        let c4 = PrimitiveArray::<BooleanType>::from(vec![Some(true), Some(false), None]);

        let batch = RecordBatch::new(
            Arc::new(schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
        );

        let file = get_temp_file("columns.csv", &[]);

        let writer = Writer::new(file);
        writer.write(vec![&batch, &batch]).unwrap();

        // check that file was written successfully
        let mut file = File::open("target/debug/testdata/columns.csv").unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "c1,c2,c3,c3\nLorem ipsum dolor sit amet,123.564532,3,true\nconsectetur adipiscing elit,,2,false\nsed do eiusmod tempor,-556132.25,1,\nLorem ipsum dolor sit amet,123.564532,3,true\nconsectetur adipiscing elit,,2,false\nsed do eiusmod tempor,-556132.25,1,\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }

    #[test]
    fn test_write_csv_custom_options() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c3", DataType::Boolean, true),
        ]);

        let c1 = BinaryArray::from(vec![
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
        ]);
        let c2 = PrimitiveArray::<Float64Type>::from(vec![
            Some(123.564532),
            None,
            Some(-556132.25),
        ]);
        let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
        let c4 = PrimitiveArray::<BooleanType>::from(vec![Some(true), Some(false), None]);

        let batch = RecordBatch::new(
            Arc::new(schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
        );

        let file = get_temp_file("custom_options.csv", &[]);

        let builder = WriterBuilder::new().has_headers(false).with_delimiter(b'|');

        let writer = builder.build(file);
        writer.write(vec![&batch]).unwrap();

        // check that file was written successfully
        let mut file = File::open("target/debug/testdata/custom_options.csv").unwrap();
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).unwrap();

        assert_eq!(
            "Lorem ipsum dolor sit amet|123.564532|3|true\nconsectetur adipiscing elit||2|false\nsed do eiusmod tempor|-556132.25|1|\n"
            .to_string(),
            String::from_utf8(buffer).unwrap()
        );
    }
}
