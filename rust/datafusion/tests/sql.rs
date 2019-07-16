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
use datafusion::logicalplan::LogicalPlan;

const DEFAULT_BATCH_SIZE: usize = 1024 * 1024;

#[test]
fn nyc() {
    // schema for nyxtaxi csv files
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]);

    let mut ctx = ExecutionContext::new();
    ctx.register_csv("tripdata", "file.csv", &schema, true);

    let optimized_plan = ctx
        .create_logical_plan(
            "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
             FROM tripdata GROUP BY passenger_count",
        )
        .unwrap();

    println!("Logical plan: {:?}", optimized_plan);

    match optimized_plan.as_ref() {
        LogicalPlan::Aggregate { input, .. } => match input.as_ref() {
            LogicalPlan::TableScan {
                ref projected_schema,
                ..
            } => {
                assert_eq!(2, projected_schema.fields().len());
                assert_eq!(projected_schema.field(0).name(), "passenger_count");
                assert_eq!(projected_schema.field(1).name(), "fare_amount");
            }
            _ => assert!(false),
        },
        _ => assert!(false),
    }
}

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
fn csv_count_star() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    let sql = "SELECT COUNT(*), COUNT(1), COUNT(c1) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql);
    let expected = "100\t100\t100\n".to_string();
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
    let expected = "4\t0.02182578039211991\t0.9237877978193884\n5\t0.01479305307777301\t0.9723580396501548\n2\t0.16301110515739792\t0.991517828651004\n3\t0.047343434291126085\t0.9293883502480845\n1\t0.05636955101974106\t0.9965400387585364\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_avg() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql);
    let expected = "0.5089725099127211\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_avg() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute(&mut ctx, sql);
    let expected = "\"a\"\t0.48754517466109415\n\"e\"\t0.48600669271341534\n\"d\"\t0.48855379387549824\n\"c\"\t0.6600456536439784\n\"b\"\t0.41040709263815384\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_avg_multi_batch() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let results = ctx.execute(&plan, 4).unwrap();
    let mut relation = results.borrow_mut();
    let batch = relation.next().unwrap().unwrap();
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
}

//#[test]
//fn csv_query_group_by_avg_multi_batch() {
//    let mut ctx = ExecutionContext::new();
//    register_aggregate_csv(&mut ctx);
//    //TODO add ORDER BY once supported, to make this test determistic
//    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY c1";
//    let plan = ctx.create_logical_plan(&sql).unwrap();
//    let results = ctx.execute(&plan, 4).unwrap();
//    let mut relation = results.borrow_mut();
//    let mut actual_vec = Vec::new();
//    while let Some(batch) = relation.next().unwrap() {
//        let column = batch.column(1);
//        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
//
//        for row_index in 0..batch.num_rows() {
//            actual_vec.push(array.value(row_index));
//        }
//    }
//
//    let expect_vec = vec![0.41040709, 0.48754517, 0.48855379, 0.66004565];
//
//    actual_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
//
//    println!("actual  : {:?}", actual_vec);
//    println!("expected: {:?}", expect_vec);
//
//    actual_vec
//        .iter()
//        .zip(expect_vec.iter())
//        .for_each(|(actual, expect)| {
//            // Due to float number's accuracy, different batch size will lead to
// different answers.            assert!((expect - actual).abs() < 0.01);
//        });
//}

#[test]
fn csv_query_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql);
    let expected = "100\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_int_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT count(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute(&mut ctx, sql);
    let expected = "\"a\"\t21\n\"e\"\t21\n\"d\"\t18\n\"c\"\t21\n\"b\"\t19\n".to_string();
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
        "\"a\"\t0.02182578039211991\t0.9800193410444061\n\"e\"\t0.01479305307777301\t0.9965400387585364\n\"d\"\t0.061029375346466685\t0.9748360509016578\n\"c\"\t0.0494924465469434\t0.991517828651004\n\"b\"\t0.04893135681998029\t0.9185813970744787\n".to_string();
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

#[test]
fn csv_query_create_external_table() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, 10, c11, c12, c13 FROM aggregate_test_100 LIMIT 1";
    let actual = execute(&mut ctx, sql);
    let expected = "\"c\"\t2\t1\t18109\t2033001162\t-6513304855495910254\t25\t43062\t1491205016\t10\t0.110830784\t0.9294097332465232\t\"6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW\"\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_external_table_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql);
    let expected = "100\n".to_string();
    assert_eq!(expected, actual);
}

//TODO Uncomment the following test when ORDER BY is implemented to be able to test ORDER
// BY + LIMIT
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

fn register_aggregate_csv_by_sql(ctx: &mut ExecutionContext) {
    let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");

    // TODO: The following c9 should be migrated to UInt32 and c10 should be UInt64 once
    // unsigned is supported.
    ctx.sql(
        &format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  INT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INT NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  BIGINT NOT NULL,
        c10 VARCHAR NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{}/csv/aggregate_test_100.csv'
    ",
            testdata
        ),
        1024,
    )
    .unwrap();
}

fn register_aggregate_csv(ctx: &mut ExecutionContext) {
    let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
    let schema = aggr_test_schema();
    register_csv(
        ctx,
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
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

fn load_parquet_table(name: &str) -> Rc<dyn TableProvider> {
    let testdata = env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
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

fn result_str(results: &Rc<RefCell<dyn Relation>>) -> String {
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
