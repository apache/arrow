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

//! Common Parquet errors and macros.

use std::{cell, convert, io, result, str};

#[cfg(any(feature = "arrow", test))]
use arrow::error::ArrowError;

#[derive(Debug, PartialEq)]
pub enum ParquetError {
    /// General Parquet error.
    /// Returned when code violates normal workflow of working with Parquet files.
    General(String),
    /// "Not yet implemented" Parquet error.
    /// Returned when functionality is not yet available.
    NYI(String),
    /// "End of file" Parquet error.
    /// Returned when IO related failures occur, e.g. when there are not enough bytes to
    /// decode.
    EOF(String),
    #[cfg(any(feature = "arrow", test))]
    /// Arrow error.
    /// Returned when reading into arrow or writing from arrow.
    ArrowError(String),
    IndexOutOfBound(usize, usize),
}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ParquetError::General(ref message) => {
                write!(fmt, "Parquet error: {}", message)
            }
            ParquetError::NYI(ref message) => write!(fmt, "NYI: {}", message),
            ParquetError::EOF(ref message) => write!(fmt, "EOF: {}", message),
            #[cfg(any(feature = "arrow", test))]
            ParquetError::ArrowError(ref message) => write!(fmt, "Arrow: {}", message),
            ParquetError::IndexOutOfBound(ref index, ref bound) => {
                write!(fmt, "Index {} out of bound: {}", index, bound)
            }
        }
    }
}

impl std::error::Error for ParquetError {
    fn cause(&self) -> Option<&::std::error::Error> {
        None
    }
}

impl From<io::Error> for ParquetError {
    fn from(e: io::Error) -> ParquetError {
        ParquetError::General(format!("underlying IO error: {}", e))
    }
}

#[cfg(any(feature = "snap", test))]
impl From<snap::Error> for ParquetError {
    fn from(e: snap::Error) -> ParquetError {
        ParquetError::General(format!("underlying snap error: {}", e))
    }
}

impl From<thrift::Error> for ParquetError {
    fn from(e: thrift::Error) -> ParquetError {
        ParquetError::General(format!("underlying Thrift error: {}", e))
    }
}

impl From<cell::BorrowMutError> for ParquetError {
    fn from(e: cell::BorrowMutError) -> ParquetError {
        ParquetError::General(format!("underlying borrow error: {}", e))
    }
}

impl From<str::Utf8Error> for ParquetError {
    fn from(e: str::Utf8Error) -> ParquetError {
        ParquetError::General(format!("underlying utf8 error: {}", e))
    }
}

#[cfg(any(feature = "arrow", test))]
impl From<ArrowError> for ParquetError {
    fn from(e: ArrowError) -> ParquetError {
        ParquetError::ArrowError(format!("underlying Arrow error: {}", e))
    }
}

/// A specialized `Result` for Parquet errors.
pub type Result<T> = result::Result<T, ParquetError>;

// ----------------------------------------------------------------------
// Conversion from `ParquetError` to other types of `Error`s

impl convert::From<ParquetError> for io::Error {
    fn from(e: ParquetError) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

// ----------------------------------------------------------------------
// Convenient macros for different errors

macro_rules! general_err {
    ($fmt:expr) => (ParquetError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (ParquetError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        ParquetError::General(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
    ($fmt:expr) => (ParquetError::NYI($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::NYI(format!($fmt, $($args),*)));
}

macro_rules! eof_err {
    ($fmt:expr) => (ParquetError::EOF($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::EOF(format!($fmt, $($args),*)));
}

// ----------------------------------------------------------------------
// Convert parquet error into other errors

#[cfg(any(feature = "arrow", test))]
impl Into<ArrowError> for ParquetError {
    fn into(self) -> ArrowError {
        ArrowError::ParquetError(format!("{}", self))
    }
}
