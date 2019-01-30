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
use std::rc::Rc;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};

use datafusion::execution::context::ExecutionContext;
use datafusion::execution::datasource::CsvDataSource;
use datafusion::execution::relation::Relation;

#[test]
fn csv_query_with_predicate() {
    let mut ctx = ExecutionContext::new();
    register_cities_csv(&mut ctx);
    let sql =
        "SELECT city, lat, lng, lat + lng FROM cities WHERE lat > 51.0 AND lat < 53";
    let actual = execute(&mut ctx, sql);
    let expected= "\"Solihull, Birmingham, UK\"\t52.412811\t-1.778197\t50.634614\n\"Cardiff, Cardiff county, UK\"\t51.481583\t-3.17909\t48.302493\n\"Oxford, Oxfordshire, UK\"\t51.752022\t-1.257677\t50.494344999999996\n\"London, UK\"\t51.509865\t-0.118092\t51.391773\n\"Swindon, Swindon, UK\"\t51.568535\t-1.772232\t49.796302999999995\n\"Gravesend, Kent, UK\"\t51.441883\t0.370759\t51.812642\n\"Northampton, Northamptonshire, UK\"\t52.240479\t-0.902656\t51.337823\n\"Rugby, Warwickshire, UK\"\t52.370876\t-1.265032\t51.105844000000005\n\"Sutton Coldfield, West Midlands, UK\"\t52.570385\t-1.824042\t50.746343\n\"Harlow, Essex, UK\"\t51.772938\t0.10231\t51.875248000000006\n\"Swansea, Swansea, UK\"\t51.621441\t-3.943646\t47.677794999999996\n\"Salisbury, Wiltshire, UK\"\t51.068787\t-1.794472\t49.274315\n\"Wolverhampton, West Midlands, UK\"\t52.59137\t-2.110748\t50.480622\n\"Bedford, UK\"\t52.136436\t-0.460739\t51.67569700000001\n\"Basildon, Essex, UK\"\t51.572376\t0.470009\t52.042384999999996\n\"Chippenham, Wiltshire, UK\"\t51.458057\t-2.116074\t49.341983\n\"Haverhill, Suffolk, UK\"\t52.080875\t0.444517\t52.525392\n\"Frankton, Warwickshire, UK\"\t52.328415\t-1.377561\t50.950854\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_int_min_max() {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Float64, false),
    ]));
    register_csv(&mut ctx, "t1", "test/data/aggregate_test_1.csv", &schema);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT a, MIN(b), MAX(b) FROM t1 GROUP BY a";
    let actual = execute(&mut ctx, sql);
    let expected = "2\t3.3\t5.5\n3\t1.0\t2.0\n1\t1.1\t2.2\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_group_by_string_min_max() {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Float64, false),
    ]));
    register_csv(&mut ctx, "t1", "test/data/aggregate_test_2.csv", &schema);
    //TODO add ORDER BY once supported, to make this test determistic
    let sql = "SELECT a, MIN(b), MAX(b) FROM t1 GROUP BY a";
    let actual = execute(&mut ctx, sql);
    let expected =
        "\"three\"\t1.0\t2.0\n\"two\"\t3.3\t5.5\n\"one\"\t1.1\t2.2\n".to_string();
    assert_eq!(expected, actual);
}

#[test]
fn csv_query_cast() {
    let mut ctx = ExecutionContext::new();
    register_cities_csv(&mut ctx);
    let sql = "SELECT CAST(lat AS int) FROM cities";
    let actual = execute(&mut ctx, sql);
    let expected= "53\n52\n51\n50\n51\n51\n51\n51\n52\n52\n52\n51\n57\n51\n53\n55\n51\n50\n52\n53\n50\n53\n55\n50\n52\n51\n51\n54\n50\n50\n53\n54\n50\n52\n52\n57\n".to_string();
    assert_eq!(expected, actual);
}

fn register_cities_csv(ctx: &mut ExecutionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]));

    register_csv(ctx, "cities", "test/data/uk_cities.csv", &schema);
}

fn register_csv(
    ctx: &mut ExecutionContext,
    name: &str,
    filename: &str,
    schema: &Arc<Schema>,
) {
    let csv_datasource = CsvDataSource::new(filename, schema.clone(), 1024);
    ctx.register_datasource(name, Rc::new(RefCell::new(csv_datasource)));
}

/// Execute query and return result set as tab delimited string
fn execute(ctx: &mut ExecutionContext, sql: &str) -> String {
    let results = ctx.sql(&sql).unwrap();
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
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
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
