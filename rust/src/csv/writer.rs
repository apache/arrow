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

use std::fs::File;
use std::io::{BufWriter, Write};

use array::{BinaryArray, PrimitiveArray};
use datatypes::{DataType, Schema};
use record_batch::RecordBatch;

pub struct Writer {
    w: BufWriter<File>,
}

macro_rules! write_primitive_array {
    ($WRITER:expr, $BATCH:expr, $ROW_INDEX:expr, $COL_INDEX:expr, $TY:ty) => {{
        let array = $BATCH
            .column($COL_INDEX)
            .as_any()
            .downcast_ref::<PrimitiveArray<$TY>>()
            .unwrap();
        $WRITER
            .write(format!("{}", array.value($ROW_INDEX)).as_bytes())
            .unwrap();
    }};
}

impl Writer {
    pub fn new(file: File) -> Self {
        Writer {
            w: BufWriter::new(file),
        }
    }

    pub fn write(&mut self, batch: &RecordBatch) {
        for row_index in 0..batch.num_rows() {
            for col_index in 0..batch.num_columns() {
                if col_index > 0 {
                    self.w.write(",".as_bytes()).unwrap();
                }
                match batch.schema().field(col_index).data_type() {
                    &DataType::Boolean => {
                        write_primitive_array!(self.w, batch, row_index, col_index, bool)
                    }
                    &DataType::Int8 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, i8)
                    }
                    &DataType::Int16 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, i16)
                    }
                    &DataType::Int32 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, i32)
                    }
                    &DataType::Int64 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, i64)
                    }
                    &DataType::UInt8 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, u8)
                    }
                    &DataType::UInt16 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, u16)
                    }
                    &DataType::UInt32 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, u32)
                    }
                    &DataType::UInt64 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, u64)
                    }
                    &DataType::Float32 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, f32)
                    }
                    &DataType::Float64 => {
                        write_primitive_array!(self.w, batch, row_index, col_index, f64)
                    }
                    &DataType::Utf8 => {
                        let array = batch
                            .column(col_index)
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap();
                        self.w.write("\"".as_bytes()).unwrap();
                        self.w.write(array.get_value(row_index)).unwrap();
                        self.w.write("\"".as_bytes()).unwrap();
                    }
                    other => panic!("unsupported type {:?}", other),
                }
            }
            self.w.write("\n".as_bytes()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_write_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let cities = BinaryArray::from(vec![
            "Elgin, Scotland, the UK",
            "Stoke-on-Trent, Staffordshire, the UK",
            "Solihull, Birmingham, UK",
        ]);
        let lat = PrimitiveArray::from(vec![57.653484, 53.002666, 52.412811]);
        let lng = PrimitiveArray::from(vec![-3.335724, -2.179404, -1.778197]);

        let batch = RecordBatch::new(
            Arc::new(schema),
            vec![Arc::new(cities), Arc::new(lat), Arc::new(lng)],
        );

        let file = File::create("/tmp/uk_cities.csv").unwrap();

        let mut writer = Writer::new(file);
        writer.write(&batch);
    }
}
