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

//! Binary file to print the schema and metadata of a Parquet file.
//!
//! # Install
//!
//! `parquet-schema` can be installed using `cargo`:
//! ```
//! cargo install parquet
//! ```
//! After this `parquet-schema` should be globally available:
//! ```
//! parquet-schema XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --bin parquet-schema XYZ.parquet
//! ```
//!
//! # Usage
//! ```
//! parquet-schema [FLAGS] <file-path>
//! ```
//!
//! ## Flags
//!     -h, --help       Prints help information
//!     -V, --version    Prints version information
//!     -v, --verbose    Enable printing full file metadata
//!
//! ## Args
//!     <file-path>    Path to a Parquet file
//!
//! Note that `verbose` is an optional boolean flag that allows to print schema only,
//! when not provided or print full file metadata when provided.

extern crate parquet;

use std::{env, fs::File, path::Path};

use clap::{crate_authors, crate_version, App, Arg};

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    schema::printer::{print_file_metadata, print_parquet_metadata},
};

fn main() {
    let matches = App::new("parquet-schema")
        .version(crate_version!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("file_path")
                .value_name("file-path")
                .required(true)
                .index(1)
                .help("Path to a Parquet file"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .help("Enable printing full file metadata"),
        )
        .get_matches();

    let filename = matches.value_of("file_path").unwrap();
    let path = Path::new(&filename);
    let file = match File::open(&path) {
        Err(e) => panic!("Error when opening file {}: {}", path.display(), e),
        Ok(f) => f,
    };
    let verbose = matches.is_present("verbose");

    match SerializedFileReader::new(file) {
        Err(e) => panic!("Error when parsing Parquet file: {}", e),
        Ok(parquet_reader) => {
            let metadata = parquet_reader.metadata();
            println!("Metadata for file: {}", &filename);
            println!();
            if verbose {
                print_parquet_metadata(&mut std::io::stdout(), &metadata);
            } else {
                print_file_metadata(&mut std::io::stdout(), &metadata.file_metadata());
            }
        }
    }
}
