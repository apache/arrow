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

//! Table API for building a logical query plan. This is similar to the Table API in Ibis and
//! the DataFrame API in Apache Spark

use crate::error::Result;
use crate::logicalplan::LogicalPlan;
use std::sync::Arc;

/// Table is an abstraction of a logical query plan
pub trait Table {
    /// Select columns by name
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<Table>>;

    /// limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<Table>>;

    /// Return the logical plan
    fn to_logical_plan(&self) -> Arc<LogicalPlan>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::ExecutionContext;
    use arrow::datatypes::*;

    #[test]
    fn demonstrate_api_usage() {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx);

        let t = ctx.table("aggregate_test_100").unwrap();

        let example = t
            .select_columns(vec!["c1", "c2", "c11"])
            .unwrap()
            .limit(10)
            .unwrap();

        let plan = example.to_logical_plan();

        assert_eq!("Limit: UInt32(10)\n  Projection: #0, #1, #10\n    TableScan: aggregate_test_100 projection=None", format!("{:?}", plan));
    }

    fn register_aggregate_csv(ctx: &mut ExecutionContext) {
        let schema = aggr_test_schema();
        ctx.register_csv(
            "aggregate_test_100",
            "../../testing/data/csv/aggregate_test_100.csv",
            &schema,
            true,
        );
    }

    fn aggr_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c4", DataType::Int16, false),
            Field::new("c5", DataType::Int32, false),
            Field::new("c6", DataType::Int64, false),
            Field::new("c7", DataType::UInt8, false),
            Field::new("c8", DataType::UInt16, false),
            Field::new("c9", DataType::UInt32, false),
            Field::new("c10", DataType::UInt64, false),
            Field::new("c11", DataType::Float32, false),
            Field::new("c12", DataType::Float64, false),
            Field::new("c13", DataType::Utf8, false),
        ]))
    }

}
