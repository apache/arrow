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

use std::env;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::csv::CsvReadOptions;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::physical_plan::udf::ScalarFunction;
use datafusion::logicalplan::LogicalPlan;

const DEFAULT_BATCH_SIZE: usize = 1024 * 1024;

#[test]
fn nyc() -> Result<()> {
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
    ctx.register_csv(
        "tripdata",
        "file.csv",
        CsvReadOptions::new().schema(&schema),
    )?;

    let logical_plan = ctx.create_logical_plan(
        "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
    )?;

    let optimized_plan = ctx.optimize(&logical_plan)?;

    match &optimized_plan {
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

    Ok(())
}

#[test]
fn parquet_query() {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx);
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected =
        "4\t\"0\"\n5\t\"1\"\n6\t\"0\"\n7\t\"1\"\n2\t\"0\"\n3\t\"1\"\n0\t\"0\"\n1\t\"1\""
            .to_string();
    assert_eq!(expected, actual);
}

#[test]
fn parquet_single_nan_schema() {
    let mut ctx = ExecutionContext::new();
    let testdata = env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
    ctx.register_parquet("single_nan", &format!("{}/single_nan.parquet", testdata))
        .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan, DEFAULT_BATCH_SIZE).unwrap();
    let results = ctx.collect(plan.as_ref()).unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[test]
fn csv_count_star() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT COUNT(*), COUNT(1), COUNT(c1) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "100\t100\t100".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_with_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c1, c12 FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "\"e\"\t0.39144436569161134\n\"d\"\t0.38870280983958583".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_group_by_int_min_max() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c2";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "1\t0.05636955101974106\t0.9965400387585364\n2\t0.16301110515739792\t0.991517828651004\n3\t0.047343434291126085\t0.9293883502480845\n4\t0.02182578039211991\t0.9237877978193884\n5\t0.01479305307777301\t0.9723580396501548".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_avg_sqrt() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT avg(custom_sqrt(c12)) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "0.6706002946036462".to_string();
    assert_eq!(actual.join("\n"), expected);
    Ok(())
}

fn create_ctx() -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();

    // register a custom UDF
    ctx.register_udf(ScalarFunction::new(
        "custom_sqrt",
        vec![Field::new("n", DataType::Float64, true)],
        DataType::Float64,
        Arc::new(custom_sqrt),
    ));

    Ok(ctx)
}

fn custom_sqrt(args: &[ArrayRef]) -> Result<ArrayRef> {
    let input = &args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast failed");

    let mut builder = Float64Builder::new(input.len());
    for i in 0..input.len() {
        if input.is_null(i) {
            builder.append_null()?;
        } else {
            builder.append_value(input.value(i).sqrt())?;
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[test]
fn csv_query_avg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "0.5089725099127211".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_group_by_avg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY c1";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "\"a\"\t0.48754517466109415\n\"b\"\t0.41040709263815384\n\"c\"\t0.6600456536439784\n\"d\"\t0.48855379387549824\n\"e\"\t0.48600669271341534".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_group_by_avg_with_projection() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT avg(c12), c1 FROM aggregate_test_100 GROUP BY c1";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "0.41040709263815384\t\"b\"\n0.48600669271341534\t\"e\"\n0.48754517466109415\t\"a\"\n0.48855379387549824\t\"d\"\n0.6600456536439784\t\"c\"".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_avg_multi_batch() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan, DEFAULT_BATCH_SIZE).unwrap();
    let results = ctx.collect(plan.as_ref()).unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[test]
fn csv_query_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "100".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_group_by_int_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c1, count(c12) FROM aggregate_test_100 GROUP BY c1";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected = "\"a\"\t21\n\"b\"\t19\n\"c\"\t21\n\"d\"\t18\n\"e\"\t21".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_group_by_string_min_max() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c1";
    let mut actual = execute(&mut ctx, sql);
    actual.sort();
    let expected =
        "\"a\"\t0.02182578039211991\t0.9800193410444061\n\"b\"\t0.04893135681998029\t0.9185813970744787\n\"c\"\t0.0494924465469434\t0.991517828651004\n\"d\"\t0.061029375346466685\t0.9748360509016578\n\"e\"\t0.01479305307777301\t0.9965400387585364".to_string();
    assert_eq!(expected, actual.join("\n"));
    Ok(())
}

#[test]
fn csv_query_cast() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "0.39144436569161134\n0.38870280983958583".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_cast_literal() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "0.9294097332465232\t1.0\n0.3114712539863804\t1.0".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_limit() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 2";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "\"c\"\n\"d\"".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_limit_bigger_than_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 200";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "2\n5\n1\n1\n5\n4\n3\n3\n1\n4\n1\n4\n3\n2\n1\n1\n2\n1\n3\n2\n4\n1\n5\n4\n2\n1\n4\n5\n2\n3\n4\n2\n1\n5\n3\n1\n2\n3\n3\n3\n2\n4\n1\n3\n2\n5\n2\n1\n4\n1\n4\n2\n5\n4\n2\n3\n4\n4\n4\n5\n4\n2\n1\n2\n4\n2\n3\n5\n1\n1\n4\n2\n1\n2\n1\n1\n5\n4\n5\n2\n3\n2\n4\n1\n3\n4\n3\n2\n5\n3\n3\n2\n5\n5\n4\n1\n3\n3\n4\n4".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_limit_with_same_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "2\n5\n1\n1\n5\n4\n3\n3\n1\n4\n1\n4\n3\n2\n1\n1\n2\n1\n3\n2\n4\n1\n5\n4\n2\n1\n4\n5\n2\n3\n4\n2\n1\n5\n3\n1\n2\n3\n3\n3\n2\n4\n1\n3\n2\n5\n2\n1\n4\n1\n4\n2\n5\n4\n2\n3\n4\n4\n4\n5\n4\n2\n1\n2\n4\n2\n3\n5\n1\n1\n4\n2\n1\n2\n1\n1\n5\n4\n5\n2\n3\n2\n4\n1\n3\n4\n3\n2\n5\n3\n3\n2\n5\n5\n4\n1\n3\n3\n4\n4".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_limit_zero() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx)?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 0";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "".to_string();
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn csv_query_create_external_table() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, 10, c11, c12, c13 FROM aggregate_test_100 LIMIT 1";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "\"c\"\t2\t1\t18109\t2033001162\t-6513304855495910254\t25\t43062\t1491205016\t10\t0.110830784\t0.9294097332465232\t\"6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW\"".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_external_table_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "100".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_count_star() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT COUNT(*) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "100".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_count_one() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx);
    let sql = "SELECT COUNT(1) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).join("\n");
    let expected = "100".to_string();
    assert_eq!(expected, actual);
}

fn aggr_test_schema() -> SchemaRef {
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

fn register_aggregate_csv(ctx: &mut ExecutionContext) -> Result<()> {
    let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
    let schema = aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new().schema(&schema),
    )?;
    Ok(())
}

fn register_alltypes_parquet(ctx: &mut ExecutionContext) {
    let testdata = env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
    )
    .unwrap();
}

/// Execute query and return result set as tab delimited string
fn execute(ctx: &mut ExecutionContext, sql: &str) -> Vec<String> {
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan, DEFAULT_BATCH_SIZE).unwrap();
    let results = ctx.collect(plan.as_ref()).unwrap();
    result_str(&results)
}

fn result_str(results: &[RecordBatch]) -> Vec<String> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let mut str = String::new();
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
                            column.as_any().downcast_ref::<StringArray>().unwrap();
                        let s = array.value(row_index);

                        str.push_str(&format!("{:?}", s));
                    }
                    _ => str.push_str("???"),
                }
            }
            result.push(str);
        }
    }
    result
}
