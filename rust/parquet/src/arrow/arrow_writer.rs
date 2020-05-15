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

//! Contains writer which writes arrow data into parquet data.

use std::fs::File;
use std::rc::Rc;

use arrow::array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::column::writer::ColumnWriter;
use crate::errors::Result;
use crate::file::properties::WriterProperties;
use crate::file::writer::{FileWriter, SerializedFileWriter};

struct ArrowWriter {
    writer: SerializedFileWriter<File>,
    rows: i64,
}

impl ArrowWriter {
    pub fn try_new(file: File, arrow_schema: &Schema) -> Result<Self> {
        let schema = crate::arrow::arrow_to_parquet_schema(arrow_schema)?;
        let props = Rc::new(WriterProperties::builder().build());
        let file_writer = SerializedFileWriter::new(
            file.try_clone()?,
            schema.root_schema_ptr(),
            props,
        )?;

        Ok(Self {
            writer: file_writer,
            rows: 0,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut row_group_writer = self.writer.next_row_group()?;
        for i in 0..batch.schema().fields().len() {
            let col_writer = row_group_writer.next_column()?;
            if let Some(mut writer) = col_writer {
                match writer {
                    ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                        let array = batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<array::Int32Array>()
                            .expect("Unable to downcast to Int32Array");
                        self.rows += typed.write_batch(
                            array.value_slice(0, array.len()),
                            None,
                            None,
                        )? as i64;
                    }
                    //TODO add other types
                    _ => {
                        unimplemented!();
                    }
                }
                row_group_writer.close_column(writer)?;
            }
        }
        self.writer.close_row_group(row_group_writer)
    }

    pub fn close(&mut self) -> Result<()> {
        self.writer.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn arrow_writer() {
        // define schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let file = File::create("test.parquet").unwrap();
        let mut writer = ArrowWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
