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

//! Defines `ArrowError` for representing failures in various Arrow operations
use std::error::Error;

use csv as csv_crate;

/// Many different operations in the `arrow` crate return this error type
#[derive(Debug, Clone, PartialEq)]
pub enum ArrowError {
    MemoryError(String),
    ParseError(String),
    ComputeError(String),
    DivideByZero,
    CsvError(String),
    IoError(String),
}

impl From<::std::io::Error> for ArrowError {
    fn from(error: ::std::io::Error) -> Self {
        ArrowError::IoError(error.description().to_string())
    }
}

impl From<csv_crate::Error> for ArrowError {
    fn from(error: csv_crate::Error) -> Self {
        match error.kind() {
            csv_crate::ErrorKind::Io(error) => {
                ArrowError::CsvError(error.description().to_string())
            }
            csv_crate::ErrorKind::Utf8 { pos: _, err } => ArrowError::CsvError(format!(
                "Encountered UTF-8 error while reading CSV file: {:?}",
                err.description()
            )),
            csv_crate::ErrorKind::UnequalLengths {
                pos: _,
                expected_len,
                len,
            } => ArrowError::CsvError(format!(
                "Encountered unequal lengths between records on CSV file. Expected {} \
                 records, found {} records",
                len, expected_len
            )),
            _ => ArrowError::CsvError("Error reading CSV file".to_string()),
        }
    }
}

pub type Result<T> = ::std::result::Result<T, ArrowError>;
