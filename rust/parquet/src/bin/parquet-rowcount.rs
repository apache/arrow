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

//! Binary file to return the number of rows found from Parquet file(s).
//!
//! # Install
//!
//! `parquet-rowcount` can be installed using `cargo`:
//! ```
//! cargo install parquet
//! ```
//! After this `parquet-rowcount` should be globally available:
//! ```
//! parquet-rowcount XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --bin parquet-rowcount XYZ.parquet ABC.parquet ZXC.parquet
//! ```
//!
//! # Usage
//! ```
//! parquet-rowcount <file-paths>...
//! ```
//!
//! ## Flags
//!     -h, --help       Prints help information
//!     -V, --version    Prints version information
//!
//! ## Args
//!     <file-paths>...    List of Parquet files to read from
//!
//! Note that `parquet-rowcount` reads full file schema, no projection or filtering is
//! applied.

extern crate parquet;

use std::{env, fs::File, path::Path};

use clap::{crate_authors, crate_version, App, Arg};

use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
    let matches = App::new("parquet-rowcount")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Return number of rows in Parquet file")
        .arg(
            Arg::with_name("file_paths")
                .value_name("file-paths")
                .required(true)
                .multiple(true)
                .help("List of Parquet files to read from separated by space"),
        )
        .get_matches();

    let filenames: Vec<&str> = matches.values_of("file_paths").unwrap().collect();
    for filename in &filenames {
        let path = Path::new(filename);
        let file = File::open(path).unwrap();
        let parquet_reader = SerializedFileReader::new(file).unwrap();
        let row_group_metadata = parquet_reader.metadata().row_groups();
        let mut total_num_rows = 0;

        for group_metadata in row_group_metadata {
            total_num_rows += group_metadata.num_rows();
        }

        eprintln!("File {}: rowcount={}", filename, total_num_rows);
    }
}
