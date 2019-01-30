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

use arrow::array::{BinaryArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};

use datafusion::execution::context::ExecutionContext;
use datafusion::execution::datasource::CsvDataSource;

/// This example demonstrates executing a simple query against an Arrow data source and fetching results
fn main() {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]));

    // register csv file with the execution context
    let csv_datasource =
        CsvDataSource::new("test/data/uk_cities.csv", schema.clone(), 1024);
    ctx.register_datasource("cities", Rc::new(RefCell::new(csv_datasource)));

    // simple projection and selection
    let sql = "SELECT city, lat, lng FROM cities WHERE lat > 51.0 AND lat < 53";

    // execute the query
    let relation = ctx.sql(&sql).unwrap();

    // display the relation
    let mut results = relation.borrow_mut();

    while let Some(batch) = results.next().unwrap() {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let lng = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let city_name: String = String::from_utf8(city.value(i).to_vec()).unwrap();

            println!(
                "City: {}, Latitude: {}, Longitude: {}",
                city_name,
                lat.value(i),
                lng.value(i),
            );
        }
    }
}
