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

use array::PrimitiveArray;
use datatypes::{Schema, DataType};
use record_batch::RecordBatch;

pub struct Writer {
    w: BufWriter<File>
}

impl Writer {

    pub fn new(file: File) -> Self {
        Writer { w: BufWriter::new(file) }
    }

    pub fn write(&mut self, batch: &RecordBatch) {
        for row_index in 0..batch.num_rows() {
            for col_index in 0..batch.num_columns() {
                match batch.schema().field(col_index).data_type() {
                    &DataType::Int32 => {
                        let array = batch.column(col_index)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<i32>>()
                            .unwrap();
                        self.w.write(format!("{}", array.value(row_index)).as_bytes());
                    },
                    _ => panic!("unsupported type")
                }
            }
            self.w.write("\n".as_bytes());
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use builder::PrimitiveArrayBuilder;
    use datatypes::Field;

    #[test]
    fn test_write_csv() {
        let schema = Schema::new(vec![
//            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
//            Field::new("lng", DataType::Float64, false),
        ]);

        // "Elgin, Scotland, the UK", "Stoke-on-Trent, Staffordshire, the UK", "Solihull, Birmingham, UK"
        let lat = PrimitiveArray::from(vec![57.653484, 53.002666, 52.412811]);
        let lng = PrimitiveArray::from(vec![-3.335724, -2.179404, -1.778197]);

        let batch = RecordBatch::new(Arc::new(schema), vec![Arc::new(lat), Arc::new(lng)]);

        let file = File::create("/tmp/uk_cities.csv").unwrap();

        let mut writer = Writer::new(file);
        writer.write(&batch);
    }
}
