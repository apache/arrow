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

//! A native Rust implementation of [Apache Arrow](https://arrow.apache.org), a cross-language
//! development platform for in-memory data.
//!
//! ### DataType
//!
//! Every [`Array`](array::Array) in this crate has an associated [`DataType`](datatypes::DataType),
//! that specifies how its data is layed in memory and represented.
//! Thus, a central enum of this crate is [`DataType`](datatypes::DataType), that contains the set of valid
//! DataTypes in the specification. For example, [`DataType::Utf8`](datatypes::DataType::Utf8).
//!
//! ## Array
//!
//! The central trait of this package is the dynamically-typed [`Array`](array::Array) that
//! represents a fixed-sized, immutable, Send + Sync Array of nullable elements. An example of such an array is [`UInt32Array`](array::UInt32Array).
//! One way to think about an arrow [`Array`](array::Array) is a `Arc<[Option<T>; len]>` where T can be anything ranging from an integer to a string, or even
//! another [`Array`](array::Array).
//!
//! [`Arrays`](array::Array) have [`len()`](array::Array::len), [`data_type()`](array::Array::data_type), and the nullability of each of its elements,
//! can be obtained via [`is_null(index)`](array::Array::is_null). To downcast an [`Array`](array::Array) to a specific implementation, you can use
//!
//! ```rust
//! use arrow::array::{Array, UInt32Array};
//! let array = UInt32Array::from(vec![Some(1), None, Some(3)]);
//! assert_eq!(array.len(), 3);
//! assert_eq!(array.value(0), 1);
//! assert_eq!(array.is_null(1), true);
//! ```
//!
//! To make the array dynamically typed, we wrap it in an [`Arc`](std::sync::Arc):
//!
//! ```rust
//! # use std::sync::Arc;
//! use arrow::datatypes::DataType;
//! use arrow::array::{UInt32Array, ArrayRef};
//! # let array = UInt32Array::from(vec![Some(1), None, Some(3)]);
//! let array: ArrayRef = Arc::new(array);
//! assert_eq!(array.len(), 3);
//! // array.value() is not available in the dynamically-typed version
//! assert_eq!(array.is_null(1), true);
//! assert_eq!(array.data_type(), &DataType::UInt32);
//! ```
//!
//! to downcast, use `as_any()`:
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow::array::{UInt32Array, ArrayRef};
//! # let array = UInt32Array::from(vec![Some(1), None, Some(3)]);
//! # let array: ArrayRef = Arc::new(array);
//! let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
//! assert_eq!(array.value(0), 1);
//! ```
//!
//! ## Memory and Buffers
//!
//! Data in [`Array`](array::Array) is stored in [`ArrayData`](array::data::ArrayData), that in turn
//! is a collection of other [`ArrayData`](array::data::ArrayData) and [`Buffers`](buffer::Buffer).
//! [`Buffers`](buffer::Buffer) is the central struct that array implementations use keep allocated memory and pointers.
//! The [`MutableBuffer`](buffer::MutableBuffer) is the mutable counter-part of[`Buffer`](buffer::Buffer).
//! These are the lowest abstractions of this crate, and are used throughout the crate to
//! efficiently allocate, write, read and deallocate memory.
//!
//! ## Field, Schema and RecordBatch
//!
//! [`Field`](datatypes::Field) is a struct that contains an array's metadata (datatype and whether its values
//! can be null), and a name. [`Schema`](datatypes::Schema) is a vector of fields with optional metadata.
//! Together, they form the basis of a schematic representation of a group of [`Arrays`](array::Array).
//!
//! In fact, [`RecordBatch`](record_batch::RecordBatch) is a struct with a [`Schema`](datatypes::Schema) and a vector of
//! [`Array`](array::Array)s, all with the same `len`. A record batch is the highest order struct that this crate currently offers
//! and is broadly used to represent a table where each column in an `Array`.
//!
//! ## Compute
//!
//! This crate offers many operations (called kernels) to operate on `Array`s, that you can find at [compute::kernels].
//! It has both vertial and horizontal operations, and some of them have an SIMD implementation.
//!
//! ## Status
//!
//! This crate has most of the implementation of the arrow specification. Specifically, it supports the following types:
//!
//! * All arrow primitive types, such as [`Int32Array`](array::UInt8Array), [`BooleanArray`](array::BooleanArray) and [`Float64Array`](array::Float64Array).
//! * All arrow variable length types, such as [`StringArray`](array::StringArray) and [`BinaryArray`](array::BinaryArray)
//! * All composite types such as [`StructArray`](array::StructArray) and [`ListArray`](array::ListArray)
//! * Dictionary types  [`DictionaryArray`](array::DictionaryArray)

//!
//! This crate also implements many common vertical operations:
//! * all mathematical binary operators, such as [`subtract`](compute::kernels::arithmetic::subtract)
//! * all boolean binary operators such as [`equality`](compute::kernels::comparison::eq)
//! * [`cast`](compute::kernels::cast::cast)
//! * [`filter`](compute::kernels::filter::filter)
//! * [`take`](compute::kernels::take::take) and [`limit`](compute::kernels::limit::limit)
//! * [`sort`](compute::kernels::sort::sort)
//! * some string operators such as [`substring`](compute::kernels::substring::substring) and [`length`](compute::kernels::length::length)
//!
//! as well as some horizontal operations, such as
//!
//! * [`min`](compute::kernels::aggregate::min) and [`max`](compute::kernels::aggregate::max)
//! * [`sum`](compute::kernels::aggregate::sum)
//!
//! Finally, this crate implements some readers and writers to different formats:
//!
//! * json: [reader](json::reader::Reader)
//! * csv: [reader](csv::reader::Reader) and [writer](csv::writer::Writer)
//! * ipc: [reader](ipc::reader::StreamReader) and [writer](ipc::writer::FileWriter)
//!
//! The parquet implementation is on a [separate crate](https://crates.io/crates/parquet)

#![cfg_attr(feature = "avx512", feature(stdsimd))]
#![cfg_attr(feature = "avx512", feature(repr_simd))]
#![cfg_attr(feature = "avx512", feature(avx512_target_feature))]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(bare_trait_objects)]
#![warn(missing_debug_implementations)]
#![deny(clippy::redundant_clone)]
// introduced to ignore lint errors when upgrading from 2020-04-22 to 2020-11-14
#![allow(clippy::float_equality_without_abs, clippy::type_complexity)]

mod arch;
pub mod array;
pub mod bitmap;
pub mod buffer;
pub mod bytes;
pub mod compute;
pub mod csv;
pub mod datatypes;
pub mod error;
pub mod ffi;
pub mod ipc;
pub mod json;
pub mod memory;
pub mod record_batch;
pub mod tensor;
pub mod util;
mod zz_memory_check;
