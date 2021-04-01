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

use std::any::Any;
use std::string::String;
use std::sync::Arc;

use arrow::datatypes::*;

use crate::datasource::datasource::Statistics;
use crate::datasource::TableProvider;
use crate::error::Result;
use crate::logical_plan::{combine_filters, Expr};
use crate::physical_plan::parquet::ParquetExec;
use crate::physical_plan::ExecutionPlan;

use super::datasource::TableProviderFilterPushDown;

/// Table-based representation of a `ParquetFile`.
pub struct ParquetTable {
    path: String,
    schema: SchemaRef,
    statistics: Statistics,
    max_concurrency: usize,
}

impl ParquetTable {
    /// Attempt to initialize a new `ParquetTable` from a file path.
    pub fn try_new(path: &str, max_concurrency: usize) -> Result<Self> {
        let parquet_exec = ParquetExec::try_from_path(path, None, None, 0, 1, None)?;
        let schema = parquet_exec.schema();
        Ok(Self {
            path: path.to_string(),
            schema,
            statistics: parquet_exec.statistics().to_owned(),
            max_concurrency,
        })
    }

    /// Get the path for the Parquet file(s) represented by this ParquetTable instance
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl TableProvider for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this parquet file.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    /// Scan the file(s), using the provided projection, and return one BatchIterator per
    /// partition.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = combine_filters(filters);
        Ok(Arc::new(ParquetExec::try_from_path(
            &self.path,
            projection.clone(),
            predicate,
            limit
                .map(|l| std::cmp::min(l, batch_size))
                .unwrap_or(batch_size),
            self.max_concurrency,
            limit,
        )?))
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
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
    use futures::StreamExt;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = None;
        let exec = table.scan(&projection, 2, &[], None)?;
        let stream = exec.execute(0).await?;

        let _ = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        // test metadata
        assert_eq!(table.statistics().num_rows, Some(8));
        assert_eq!(table.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
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
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![1]);
        let batch = get_first_batch(table, &projection).await?;

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

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![0]);
        let batch = get_first_batch(table, &projection).await?;

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

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![10]);
        let batch = get_first_batch(table, &projection).await?;

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

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![6]);
        let batch = get_first_batch(table, &projection).await?;

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

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![7]);
        let batch = get_first_batch(table, &projection).await?;

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

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = Some(vec![9]);
        let batch = get_first_batch(table, &projection).await?;

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

    fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = arrow::util::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::try_new(&filename, 2)?;
        Ok(Arc::new(table))
    }

    async fn get_first_batch(
        table: Arc<dyn TableProvider>,
        projection: &Option<Vec<usize>>,
    ) -> Result<RecordBatch> {
        let exec = table.scan(projection, 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        it.next()
            .await
            .expect("should have received at least one batch")
            .map_err(|e| e.into())
    }

    #[test]
    fn combine_zero_filters() {
        let result = combine_filters(&[]);
        assert_eq!(result, None);
    }

    #[test]
    fn combine_one_filter() {
        use crate::logical_plan::{binary_expr, col, lit, Operator};
        let filter = binary_expr(col("c1"), Operator::Lt, lit(1));
        let result = combine_filters(&[filter.clone()]);
        assert_eq!(result, Some(filter));
    }

    #[test]
    fn combine_multiple_filters() {
        use crate::logical_plan::{and, binary_expr, col, lit, Operator};
        let filter1 = binary_expr(col("c1"), Operator::Lt, lit(1));
        let filter2 = binary_expr(col("c2"), Operator::Lt, lit(2));
        let filter3 = binary_expr(col("c3"), Operator::Lt, lit(3));
        let result =
            combine_filters(&[filter1.clone(), filter2.clone(), filter3.clone()]);
        assert_eq!(result, Some(and(and(filter1, filter2), filter3)));
    }
}
