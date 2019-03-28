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

use std::cell::RefCell;
use std::env;
use std::rc::Rc;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};

use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::relation::Relation;

const DEFAULT_BATCH_SIZE: usize = 1024 * 1024;

#[test]
fn parquet_query() {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "alltypes_plain",
        load_parquet_table("alltypes_plain.parquet"),
    );
    let sql = "SELECT id, string_col FROM alltypes_plain";
    let actual = execute(&mut ctx, sql);
    let expected = "4\t\"0\"\n5\t\"1\"\n6\t\"0\"\n7\t\"1\"\n2\t\"0\"\n3\t\"1\"\n0\t\"0\"\n1\t\"1\"\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_with_predicate() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT c1, c12 FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute(&mut ctx, sql);
    let expected = "\"e\"\t0.39144436569161134\n\"d\"\t0.38870280983958583\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_int_min_max() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c2";
    let actual = execute(&mut ctx, sql);
    let expected = "4\t0.02182578039211991\t0.9237877978193884\n2\t0.16301110515739792\t0.991517828651004\n5\t0.01479305307777301\t0.9723580396501548\n3\t0.047343434291126085\t0.9293883502480845\n1\t0.05636955101974106\t0.9965400387585364\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_string_min_max() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute(&mut ctx, sql);
    let expected =
        "\"d\"\t0.061029375346466685\t0.9748360509016578\n\"c\"\t0.0494924465469434\t0.991517828651004\n\"b\"\t0.04893135681998029\t0.9185813970744787\n\"a\"\t0.02182578039211991\t0.9800193410444061\n\"e\"\t0.01479305307777301\t0.9965400387585364\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_cast() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute(&mut ctx, sql);
    let expected = "0.39144436569161134\n0.38870280983958583\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_cast_literal() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute(&mut ctx, sql);
    let expected = "0.9294097332465232\t1.0\n0.3114712539863804\t1.0\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_limit() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT 0 FROM aggregate_test_100 LIMIT 2";
    let actual = execute(&mut ctx, sql);
    let expected = "0\n0\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_limit_bigger_than_nbr_of_rows() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 200";
    let actual = execute(&mut ctx, sql);
    let expected = "2\n5\n1\n1\n5\n4\n3\n3\n1\n4\n1\n4\n3\n2\n1\n1\n2\n1\n3\n2\n4\n1\n5\n4\n2\n1\n4\n5\n2\n3\n4\n2\n1\n5\n3\n1\n2\n3\n3\n3\n2\n4\n1\n3\n2\n5\n2\n1\n4\n1\n4\n2\n5\n4\n2\n3\n4\n4\n4\n5\n4\n2\n1\n2\n4\n2\n3\n5\n1\n1\n4\n2\n1\n2\n1\n1\n5\n4\n5\n2\n3\n2\n4\n1\n3\n4\n3\n2\n5\n3\n3\n2\n5\n5\n4\n1\n3\n3\n4\n4\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_limit_with_same_nbr_of_rows() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 100";
    let actual = execute(&mut ctx, sql);
    let expected = "2\n5\n1\n1\n5\n4\n3\n3\n1\n4\n1\n4\n3\n2\n1\n1\n2\n1\n3\n2\n4\n1\n5\n4\n2\n1\n4\n5\n2\n3\n4\n2\n1\n5\n3\n1\n2\n3\n3\n3\n2\n4\n1\n3\n2\n5\n2\n1\n4\n1\n4\n2\n5\n4\n2\n3\n4\n4\n4\n5\n4\n2\n1\n2\n4\n2\n3\n5\n1\n1\n4\n2\n1\n2\n1\n1\n5\n4\n5\n2\n3\n2\n4\n1\n3\n4\n3\n2\n5\n3\n3\n2\n5\n5\n4\n1\n3\n3\n4\n4\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_limit_zero() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT 0 FROM aggregate_test_100 LIMIT 0";
    let actual = execute(&mut ctx, sql);
    let expected = "".to_string();
    assert_eq!(expected, actual);
}

//TODO Uncomment the following test when ORDER BY is implemented to be able to test ORDER BY + LIMIT
/*
#[test]
fn csv_query_limit_with_order_by() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT c7 FROM aggregate_test_100 ORDER BY c7 ASC LIMIT 2";
    let actual = execute(&mut ctx, sql);
    let expected = "0\n2\n".to_string();
    assert_eq!(expected, actual);
}
*/

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

fn register_aggregate_csv(ctx: &mut ExecutionContext) {
    let schema = aggr_test_schema();
    register_csv(
        ctx,
        "aggregate_test_100",
        "../../testing/data/csv/aggregate_test_100.csv",
        &schema,
    );
}

fn register_csv(
    ctx: &mut ExecutionContext,
    name: &str,
    filename: &str,
    schema: &Arc<Schema>,
) {
    ctx.register_csv(name, filename, &schema, true);
}

fn load_parquet_table(name: &str) -> Rc<TableProvider> {
    let testdata = env::var("PARQUET_TEST_DATA").unwrap();
    let filename = format!("{}/{}", testdata, name);
    let table = ParquetTable::try_new(&filename).unwrap();
    Rc::new(table)
}

/// Execute query and return result set as tab delimited string
fn execute(ctx: &mut ExecutionContext, sql: &str) -> String {
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let results = ctx.execute(&plan, DEFAULT_BATCH_SIZE).unwrap();
    result_str(&results)
}

fn result_str(results: &Rc<RefCell<Relation>>) -> String {
    let mut relation = results.borrow_mut();
    let mut str = String::new();
    while let Some(batch) = relation.next().unwrap() {
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    str.push_str("\t");
                }
                let column = batch.column(column_index);

                match column.data_type() {
                    DataType::Int8 => {
                        let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int16 => {
                        let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Int64 => {
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt8 => {
                        let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt16 => {
                        let array =
                            column.as_any().downcast_ref::<UInt16Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt32 => {
                        let array =
                            column.as_any().downcast_ref::<UInt32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::UInt64 => {
                        let array =
                            column.as_any().downcast_ref::<UInt64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float32 => {
                        let array =
                            column.as_any().downcast_ref::<Float32Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Float64 => {
                        let array =
                            column.as_any().downcast_ref::<Float64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Utf8 => {
                        let array =
                            column.as_any().downcast_ref::<BinaryArray>().unwrap();
                        let s =
                            String::from_utf8(array.value(row_index).to_vec()).unwrap();

                        str.push_str(&format!("{:?}", s));
                    }
                    _ => str.push_str("???"),
                }
            }
            str.push_str("\n");
        }
    }
    str
}
