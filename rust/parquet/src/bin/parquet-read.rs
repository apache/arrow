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

//! Binary file to read data from a Parquet file.
//!
//! # Install
//!
//! `parquet-read` can be installed using `cargo`:
//! ```
//! cargo install parquet
//! ```
//! After this `parquet-read` should be globally available:
//! ```
//! parquet-read XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --bin parquet-read XYZ.parquet
//! ```
//!
//! # Usage
//! ```
//!  parquet-read <file-path> [num-records]
//! ```
//!
//! ## Flags
//!     -h, --help       Prints help information
//!     -j, --json       Print parquet file in JSON lines Format
//!     -V, --version    Prints version information
//!
//! ## Args
//!     <file-path>      Path to a parquet file
//!     <num-records>    Number of records to read. When not provided, all records are read.
//!
//! Note that `parquet-read` reads full file schema, no projection or filtering is
//! applied.

extern crate parquet;

use std::{env, fs::File, path::Path};

use clap::{App, Arg, crate_version, crate_authors};

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;

fn main() {
    #[cfg(not(feature = "json_output"))]
    let app = App::new("parquet-read")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Read data from parquet file")
        .arg(
            Arg::with_name("file_path")
                .value_name("file-path")
                .required(true)
                .index(1)
                .help("Path to a parquet file")
        )
        .arg(
            Arg::with_name("num_records")
                .value_name("num-records")
                .index(2)
                .help("Number of records to read. When not provided, all records are read.")
        );

    #[cfg(feature = "json_output")]
    let app = App::new("parquet-read")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Read data from parquet file")
        .arg(
            Arg::with_name("file_path")
                .value_name("file-path")
                .required(true)
                .index(1)
                .help("Path to a parquet file")
        )
        .arg(
            Arg::with_name("num_records")
                .value_name("num-records")
                .index(2)
                .help("Number of records to read. When not provided, all records are read.")
        ).arg(
        Arg::with_name("json")
            .short("j")
            .long("json")
            .takes_value(false)
            .help("Print parquet file in JSON lines Format")
        );


    let matches = app.get_matches();
    let filename = matches.value_of("file_path").unwrap();
    let num_records: Option<usize> = if matches.is_present("num_records") {
        match matches.value_of("num_records").unwrap().parse() {
            Ok(value) => Some(value),
            Err(e) => panic!("Error when reading value for [num-records], {}", e),
        }
    } else {
        None
    };

    let mut json: Option<bool> = None;
    if cfg!(feature = "json_output") {
        json = Some(matches.is_present("json"));
    }


    let path = Path::new(&filename);
    let file = File::open(&path).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();

    // Use full schema as projected schema
    let mut iter = parquet_reader.get_row_iter(None).unwrap();

    let mut start = 0;
    let end = num_records.unwrap_or(0);
    let all_records = num_records.is_none();

    while all_records || start < end {
        match iter.next() {
            Some(row) => print_row(&row, json),
            None => break,
        }
        start += 1;
    }
}

#[cfg(feature = "json_output")]
fn print_row(row: &Row, json: Option<bool>) {
    if let Some(j) = json {
        if j {
            println!("{}", row.to_json_value())
        } else {
            println!("{}", row.to_string());
        }
    }
}

#[cfg(not(feature = "json_output"))]
fn print_row(row: &Row, _json: Option<bool>) {
    println!("{}", row.to_string());
}
