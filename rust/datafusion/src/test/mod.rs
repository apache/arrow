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

//! Common unit test utility methods

use crate::error::Result;
use arrow::datatypes::{DataType, Field, Schema};
use std::env;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

/// Get the value of the ARROW_TEST_DATA environment variable
pub fn arrow_testdata_path() -> String {
    env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined")
}

/// Generated partitioned copy of a CSV file
pub fn create_partitioned_csv(filename: &str, partitions: usize) -> Result<String> {
    let testdata = arrow_testdata_path();
    let path = format!("{}/csv/{}", testdata, filename);

    let mut dir = env::temp_dir();
    dir.push(&format!("{}-{}", filename, partitions));

    if Path::new(&dir).exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir(dir.clone()).unwrap();

    let mut writers = vec![];
    for i in 0..partitions {
        let mut filename = dir.clone();
        filename.push(format!("part{}.csv", i));
        let writer = BufWriter::new(File::create(&filename).unwrap());
        writers.push(writer);
    }

    let f = File::open(&path)?;
    let f = BufReader::new(f);
    let mut i = 0;
    for line in f.lines() {
        let line = line.unwrap();

        if i == 0 {
            // write header to all partitions
            for w in writers.iter_mut() {
                w.write(line.as_bytes()).unwrap();
                w.write(b"\n").unwrap();
            }
        } else {
            // write data line to single partition
            let partition = i % partitions;
            writers[partition].write(line.as_bytes()).unwrap();
            writers[partition].write(b"\n").unwrap();
        }

        i += 1;
    }
    for w in writers.iter_mut() {
        w.flush().unwrap();
    }

    Ok(dir.as_os_str().to_str().unwrap().to_string())
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> Arc<Schema> {
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
