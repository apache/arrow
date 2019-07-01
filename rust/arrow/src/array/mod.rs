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

//! Defines public types representing Apache Arrow arrays. Arrow's specification defines
//! an array as "a sequence of values with known length all having the same type." For
//! example, the type `Int16Array` represents an Apache Arrow array of 16-bit integers.
//!
//! ```
//! extern crate arrow;
//!
//! use arrow::array::Int16Array;
//!
//! // Create a new builder with a capacity of 100
//! let mut builder = Int16Array::builder(100);
//!
//! // Append a single primitive value
//! builder.append_value(1).unwrap();
//!
//! // Append a null value
//! builder.append_null().unwrap();
//!
//! // Append a slice of primitive values
//! builder.append_slice(&[2, 3, 4]).unwrap();
//!
//! // Build the array
//! let array = builder.finish();
//!
//! assert_eq!(
//!     5,
//!     array.len(),
//!     "The array has 5 values, counting the null value"
//! );
//!
//! assert_eq!(2, array.value(2), "Get the value with index 2");
//!
//! assert_eq!(
//!     array.value_slice(3, 2),
//!     &[3, 4],
//!     "Get slice of len 2 starting at idx 3"
//! )
//! ```

mod array;
mod builder;
mod data;
mod equal;

use crate::datatypes::*;

// --------------------- Array & ArrayData ---------------------

pub use self::array::Array;
pub use self::array::ArrayRef;
pub use self::data::ArrayData;
pub use self::data::ArrayDataBuilder;
pub use self::data::ArrayDataRef;

pub use self::array::BinaryArray;
pub use self::array::ListArray;
pub use self::array::PrimitiveArray;
pub use self::array::StructArray;

pub(crate) use self::array::make_array;

pub type BooleanArray = PrimitiveArray<BooleanType>;
pub type Int8Array = PrimitiveArray<Int8Type>;
pub type Int16Array = PrimitiveArray<Int16Type>;
pub type Int32Array = PrimitiveArray<Int32Type>;
pub type Int64Array = PrimitiveArray<Int64Type>;
pub type UInt8Array = PrimitiveArray<UInt8Type>;
pub type UInt16Array = PrimitiveArray<UInt16Type>;
pub type UInt32Array = PrimitiveArray<UInt32Type>;
pub type UInt64Array = PrimitiveArray<UInt64Type>;
pub type Float32Array = PrimitiveArray<Float32Type>;
pub type Float64Array = PrimitiveArray<Float64Type>;

pub type TimestampSecondArray = PrimitiveArray<TimestampSecondType>;
pub type TimestampMillisecondArray = PrimitiveArray<TimestampMillisecondType>;
pub type TimestampMicrosecondArray = PrimitiveArray<TimestampMicrosecondType>;
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecondType>;
pub type Date32Array = PrimitiveArray<Date32Type>;
pub type Date64Array = PrimitiveArray<Date64Type>;
pub type Time32SecondArray = PrimitiveArray<Time32SecondType>;
pub type Time32MillisecondArray = PrimitiveArray<Time32MillisecondType>;
pub type Time64MicrosecondArray = PrimitiveArray<Time64MicrosecondType>;
pub type Time64NanosecondArray = PrimitiveArray<Time64NanosecondType>;
// TODO add interval

pub use self::array::ListArrayOps;
pub use self::array::PrimitiveArrayOps;

// --------------------- Array Builder ---------------------

pub use self::builder::BufferBuilder;
pub use self::builder::BufferBuilderTrait;

pub type BooleanBufferBuilder = BufferBuilder<BooleanType>;
pub type Int8BufferBuilder = BufferBuilder<Int8Type>;
pub type Int16BufferBuilder = BufferBuilder<Int16Type>;
pub type Int32BufferBuilder = BufferBuilder<Int32Type>;
pub type Int64BufferBuilder = BufferBuilder<Int64Type>;
pub type UInt8BufferBuilder = BufferBuilder<UInt8Type>;
pub type UInt16BufferBuilder = BufferBuilder<UInt16Type>;
pub type UInt32BufferBuilder = BufferBuilder<UInt32Type>;
pub type UInt64BufferBuilder = BufferBuilder<UInt64Type>;
pub type Float32BufferBuilder = BufferBuilder<Float32Type>;
pub type Float64BufferBuilder = BufferBuilder<Float64Type>;

pub type TimestampSecondBufferBuilder = BufferBuilder<TimestampSecondType>;
pub type TimestampMillisecondBufferBuilder = BufferBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBufferBuilder = BufferBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBufferBuilder = BufferBuilder<TimestampNanosecondType>;
pub type Date32BufferBuilder = BufferBuilder<Date32Type>;
pub type Date64BufferBuilder = BufferBuilder<Date64Type>;
pub type Time32SecondBufferBuilder = BufferBuilder<Time32SecondType>;
pub type Time32MillisecondBufferBuilder = BufferBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBufferBuilder = BufferBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBufferBuilder = BufferBuilder<Time64NanosecondType>;

pub use self::builder::ArrayBuilder;
pub use self::builder::BinaryBuilder;
pub use self::builder::ListBuilder;
pub use self::builder::PrimitiveBuilder;
pub use self::builder::StructBuilder;

pub type BooleanBuilder = PrimitiveBuilder<BooleanType>;
pub type Int8Builder = PrimitiveBuilder<Int8Type>;
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBuilder = PrimitiveBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBuilder = PrimitiveBuilder<TimestampNanosecondType>;
pub type Date32Builder = PrimitiveBuilder<Date32Type>;
pub type Date64Builder = PrimitiveBuilder<Date64Type>;
pub type Time32SecondBuilder = PrimitiveBuilder<Time32SecondType>;
pub type Time32MillisecondBuilder = PrimitiveBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBuilder = PrimitiveBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBuilder = PrimitiveBuilder<Time64NanosecondType>;

// --------------------- Array Equality ---------------------

pub use self::equal::ArrayEqual;
