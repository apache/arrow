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

//! Parquet data source

use std::string::String;
use std::sync::Arc;

use arrow::datatypes::*;

use crate::datasource::TableProvider;
use crate::error::Result;
use crate::physical_plan::parquet::ParquetExec;
use crate::physical_plan::ExecutionPlan;

/// Table-based representation of a `ParquetFile`.
pub struct ParquetTable {
    path: String,
    schema: SchemaRef,
}

impl ParquetTable {
    /// Attempt to initialize a new `ParquetTable` from a file path.
    pub fn try_new(path: &str) -> Result<Self> {
        let parquet_exec = ParquetExec::try_new(path, None, 0)?;
        let schema = parquet_exec.schema();
        Ok(Self {
            path: path.to_string(),
            schema,
        })
    }
}

impl TableProvider for ParquetTable {
    /// Get the schema for this parquet file.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Scan the file(s), using the provided projection, and return one BatchIterator per
    /// partition.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ParquetExec::try_new(
            &self.path,
            projection.clone(),
            batch_size,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampNanosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use std::env;

    #[test]
    fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = None;
        let exec = table.scan(&projection, 2)?;
        let it = exec.execute(0)?;
        let mut it = it.lock().unwrap();

        let mut count = 0;
        while let Some(batch) = it.next_batch()? {
            assert_eq!(11, batch.num_columns());
            assert_eq!(2, batch.num_rows());
            count += 1;
        }

        // we should have seen 4 batches of 2 rows
        assert_eq!(4, count);

        Ok(())
    }

    #[test]
    fn read_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;

        let x: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Nanosecond, None)",
            y
        );

        let projection = None;
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        Ok(())
    }

    #[test]
    fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![1]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[test]
    fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![0]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[test]
    fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![10]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));

        Ok(())
    }

    #[test]
    fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![6]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[test]
    fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![7]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[test]
    fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![9]);
        let batch = get_first_batch(table, &projection)?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batch.num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    fn load_table(name: &str) -> Result<Box<dyn TableProvider>> {
        let testdata =
            env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::try_new(&filename)?;
        Ok(Box::new(table))
    }

    fn get_first_batch(
        table: Box<dyn TableProvider>,
        projection: &Option<Vec<usize>>,
    ) -> Result<RecordBatch> {
        let exec = table.scan(projection, 1024)?;
        let it = exec.execute(0)?;
        let mut it = it.lock().expect("failed to lock mutex");
        Ok(it
            .next_batch()?
            .expect("should have received at least one batch"))
    }
}
