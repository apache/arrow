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

extern crate arrow;

use arrow::array::{BinaryArray, Float64Array};
use arrow::csv;
use std::fs::File;

fn main() {
    let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    let builder = csv::ReaderBuilder::new()
        .has_headers(true)
        .infer_schema(Some(100));
    let mut csv = builder.build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();

    println!(
        "Loaded {} rows containing {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    println!("Inferred schema: {:?}", batch.schema());

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
            lng.value(i)
        );
    }
}
