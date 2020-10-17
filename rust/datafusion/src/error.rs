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
use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T> = result::Result<T, DataFusionError>;

/// DataFusion error
#[derive(Debug)]
#[allow(missing_docs)]
pub enum DataFusionError {
    /// Error returned by arrow.
    ArrowError(ArrowError),
    /// Wraps an error from the Parquet crate
    ParquetError(ParquetError),
    /// I/O error
    IoError(Error),
    /// Error returned when SQL is syntatically incorrect.
    SQL(ParserError),
    /// General error that does not fit in any of the other errors.
    General(String),
    /// Error returned on a code branch that we know it is possible
    /// but to which we still have no implementation of.
    /// Often, these errors are tracked in our issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in DataFusion.
    /// This error should not happen in normal usage of DataFusion.
    // We use this error when an invariant that we were unable
    // to make the compiler assert for us is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid. Examples include
    /// impossible casts, schema inference not possible and non-unique column names.
    Plan(String),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
}

impl DataFusionError {
    /// Wraps this [DataFusionError] as an [Arrow::error::ArrowError].
    pub fn into_arrow_external_error(self) -> ArrowError {
        ArrowError::from_external_error(Box::new(self))
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        DataFusionError::IoError(e)
    }
}

impl From<String> for DataFusionError {
    fn from(e: String) -> Self {
        DataFusionError::General(e)
    }
}

impl From<&'static str> for DataFusionError {
    fn from(e: &'static str) -> Self {
        DataFusionError::General(e.to_string())
    }
}

impl From<ArrowError> for DataFusionError {
    fn from(e: ArrowError) -> Self {
        DataFusionError::ArrowError(e)
    }
}

impl From<ParquetError> for DataFusionError {
    fn from(e: ParquetError) -> Self {
        DataFusionError::ParquetError(e)
    }
}

impl From<ParserError> for DataFusionError {
    fn from(e: ParserError) -> Self {
        DataFusionError::SQL(e)
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            DataFusionError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            DataFusionError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {}", desc)
            }
            DataFusionError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            DataFusionError::SQL(ref desc) => {
                write!(f, "SQL error: {:?}", desc)
            }
            DataFusionError::General(ref desc) => write!(f, "General error: {}", desc),
            DataFusionError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            DataFusionError::Internal(ref desc) => {
                write!(f, "Internal error: {}. This was likely caused by a bug in DataFusion's \
                    code and we would welcome that you file an bug report in our issue tracker", desc)
            }
            DataFusionError::Plan(ref desc) => {
                write!(f, "Error during planning: {}", desc)
            }
            DataFusionError::Execution(ref desc) => {
                write!(f, "Execution error: {}", desc)
            }
        }
    }
}

impl error::Error for DataFusionError {}
