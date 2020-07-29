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

//! DataFusion error types

use std::error;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::result;

use arrow::error::ArrowError;
use parquet::errors::ParquetError;

use sqlparser::sqlparser::ParserError;

/// Result type for operations that could result in an `ExecutionError`
pub type Result<T> = result::Result<T, ExecutionError>;

/// DataFusion error
#[derive(Debug)]
#[allow(missing_docs)]
pub enum ExecutionError {
    /// Wraps an error from the Arrow crate
    ArrowError(ArrowError),
    /// Wraps an error from the Parquet crate
    ParquetError(ParquetError),
    /// I/O error
    IoError(Error),
    /// SQL parser error
    ParserError(ParserError),
    /// General error
    General(String),
    /// Invalid column error
    InvalidColumn(String),
    /// Missing functionality
    NotImplemented(String),
    /// Internal error
    InternalError(String),
    /// Query engine execution error
    ExecutionError(String),
}

impl ExecutionError {
    /// Wraps this `ExecutionError` in arrow's `ExternalError` variant.
    pub fn into_arrow_external_error(self) -> ArrowError {
        ArrowError::from_external_error(Box::new(self))
    }
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

impl From<String> for ExecutionError {
    fn from(e: String) -> Self {
        ExecutionError::General(e)
    }
}

impl From<&'static str> for ExecutionError {
    fn from(e: &'static str) -> Self {
        ExecutionError::General(e.to_string())
    }
}

impl From<ArrowError> for ExecutionError {
    fn from(e: ArrowError) -> Self {
        ExecutionError::ArrowError(e)
    }
}

impl From<ParquetError> for ExecutionError {
    fn from(e: ParquetError) -> Self {
        ExecutionError::ParquetError(e)
    }
}

impl From<ParserError> for ExecutionError {
    fn from(e: ParserError) -> Self {
        ExecutionError::ParserError(e)
    }
}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            ExecutionError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            ExecutionError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {}", desc)
            }
            ExecutionError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            ExecutionError::ParserError(ref desc) => {
                write!(f, "Parser error: {:?}", desc)
            }
            ExecutionError::General(ref desc) => write!(f, "General error: {}", desc),
            ExecutionError::InvalidColumn(ref desc) => {
                write!(f, "Invalid column error: {}", desc)
            }
            ExecutionError::NotImplemented(ref desc) => {
                write!(f, "NotImplemented: {}", desc)
            }
            ExecutionError::InternalError(ref desc) => {
                write!(f, "Internal error: {}", desc)
            }
            ExecutionError::ExecutionError(ref desc) => {
                write!(f, "Execution error: {}", desc)
            }
        }
    }
}

impl error::Error for ExecutionError {}
