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


//! The central type in Apache Arrow are arrays, represented
//! by the [`Array` trait](crate::array::Array).
//! An array represents a known-length sequence of values all
//! having the same type.
//! 
//! Internally, those values are represented by one or several
//! [buffers](crate::buffer::Buffer), the number and meaning 
//! of which depend on the array’s data type, as documented in 
//! [the Arrow data layout specification](https://arrow.apache.org/docs/format/Columnar.html).
//! For example, the type `Int16Array` represents an Apache 
//! Arrow array of 16-bit integers.
//!
//! Those buffers consist of the value data itself and an
//! optional [bitmap buffer](crate::bitmap::Bitmap) that
//! indicates which array entries are null values.
//! The bitmap buffer can be entirely omitted if the array is 
//! known to have zero null values.
//!
//! There are concrete implementations of this trait for each
//! data type, that help you access individual values of the 
//! array.
//! 
//! # Building an Array
//! 
//! Arrow's `Arrays` are immutable, but there is the trait
//! [`ArrayBuilder`](crate::array::ArrayBuilder)
//! that helps you with constructing new `Arrays`. As with the
//! `Array` trait, there are builder implementations for all
//! concrete array types.
//! 
//! # Example
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
pub use self::array::DictionaryArray;
pub use self::array::FixedSizeBinaryArray;
pub use self::array::FixedSizeListArray;
pub use self::array::ListArray;
pub use self::array::PrimitiveArray;
pub use self::array::StringArray;
pub use self::array::StructArray;

pub(crate) use self::array::make_array;

#[allow(missing_docs)]
pub type BooleanArray = PrimitiveArray<BooleanType>;
#[allow(missing_docs)]
pub type Int8Array = PrimitiveArray<Int8Type>;
#[allow(missing_docs)]
pub type Int16Array = PrimitiveArray<Int16Type>;
#[allow(missing_docs)]
pub type Int32Array = PrimitiveArray<Int32Type>;
#[allow(missing_docs)]
pub type Int64Array = PrimitiveArray<Int64Type>;
#[allow(missing_docs)]
pub type UInt8Array = PrimitiveArray<UInt8Type>;
#[allow(missing_docs)]
pub type UInt16Array = PrimitiveArray<UInt16Type>;
#[allow(missing_docs)]
pub type UInt32Array = PrimitiveArray<UInt32Type>;
#[allow(missing_docs)]
pub type UInt64Array = PrimitiveArray<UInt64Type>;
#[allow(missing_docs)]
pub type Float32Array = PrimitiveArray<Float32Type>;
#[allow(missing_docs)]
pub type Float64Array = PrimitiveArray<Float64Type>;

#[allow(missing_docs)]
pub type Int8DictionaryArray = DictionaryArray<Int8Type>;
#[allow(missing_docs)]
pub type Int16DictionaryArray = DictionaryArray<Int16Type>;
#[allow(missing_docs)]
pub type Int32DictionaryArray = DictionaryArray<Int32Type>;
#[allow(missing_docs)]
pub type Int64DictionaryArray = DictionaryArray<Int64Type>;
#[allow(missing_docs)]
pub type UInt8DictionaryArray = DictionaryArray<UInt8Type>;
#[allow(missing_docs)]
pub type UInt16DictionaryArray = DictionaryArray<UInt16Type>;
#[allow(missing_docs)]
pub type UInt32DictionaryArray = DictionaryArray<UInt32Type>;
#[allow(missing_docs)]
pub type UInt64DictionaryArray = DictionaryArray<UInt64Type>;

#[allow(missing_docs)]
pub type TimestampSecondArray = PrimitiveArray<TimestampSecondType>;
#[allow(missing_docs)]
pub type TimestampMillisecondArray = PrimitiveArray<TimestampMillisecondType>;
#[allow(missing_docs)]
pub type TimestampMicrosecondArray = PrimitiveArray<TimestampMicrosecondType>;
#[allow(missing_docs)]
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecondType>;
#[allow(missing_docs)]
pub type Date32Array = PrimitiveArray<Date32Type>;
#[allow(missing_docs)]
pub type Date64Array = PrimitiveArray<Date64Type>;
#[allow(missing_docs)]
pub type Time32SecondArray = PrimitiveArray<Time32SecondType>;
#[allow(missing_docs)]
pub type Time32MillisecondArray = PrimitiveArray<Time32MillisecondType>;
#[allow(missing_docs)]
pub type Time64MicrosecondArray = PrimitiveArray<Time64MicrosecondType>;
#[allow(missing_docs)]
pub type Time64NanosecondArray = PrimitiveArray<Time64NanosecondType>;
#[allow(missing_docs)]
pub type IntervalYearMonthArray = PrimitiveArray<IntervalYearMonthType>;
#[allow(missing_docs)]
pub type IntervalDayTimeArray = PrimitiveArray<IntervalDayTimeType>;
#[allow(missing_docs)]
pub type DurationSecondArray = PrimitiveArray<DurationSecondType>;
#[allow(missing_docs)]
pub type DurationMillisecondArray = PrimitiveArray<DurationMillisecondType>;
#[allow(missing_docs)]
pub type DurationMicrosecondArray = PrimitiveArray<DurationMicrosecondType>;
#[allow(missing_docs)]
pub type DurationNanosecondArray = PrimitiveArray<DurationNanosecondType>;

pub use self::array::ListArrayOps;
pub use self::array::PrimitiveArrayOps;

// --------------------- Array Builder ---------------------

pub use self::builder::BufferBuilder;
pub use self::builder::BufferBuilderTrait;

#[allow(missing_docs)]
pub type BooleanBufferBuilder = BufferBuilder<BooleanType>;
#[allow(missing_docs)]
pub type Int8BufferBuilder = BufferBuilder<Int8Type>;
#[allow(missing_docs)]
pub type Int16BufferBuilder = BufferBuilder<Int16Type>;
#[allow(missing_docs)]
pub type Int32BufferBuilder = BufferBuilder<Int32Type>;
#[allow(missing_docs)]
pub type Int64BufferBuilder = BufferBuilder<Int64Type>;
#[allow(missing_docs)]
pub type UInt8BufferBuilder = BufferBuilder<UInt8Type>;
#[allow(missing_docs)]
pub type UInt16BufferBuilder = BufferBuilder<UInt16Type>;
#[allow(missing_docs)]
pub type UInt32BufferBuilder = BufferBuilder<UInt32Type>;
#[allow(missing_docs)]
pub type UInt64BufferBuilder = BufferBuilder<UInt64Type>;
#[allow(missing_docs)]
pub type Float32BufferBuilder = BufferBuilder<Float32Type>;
#[allow(missing_docs)]
pub type Float64BufferBuilder = BufferBuilder<Float64Type>;

#[allow(missing_docs)]
pub type TimestampSecondBufferBuilder = BufferBuilder<TimestampSecondType>;
#[allow(missing_docs)]
pub type TimestampMillisecondBufferBuilder = BufferBuilder<TimestampMillisecondType>;
#[allow(missing_docs)]
pub type TimestampMicrosecondBufferBuilder = BufferBuilder<TimestampMicrosecondType>;
#[allow(missing_docs)]
pub type TimestampNanosecondBufferBuilder = BufferBuilder<TimestampNanosecondType>;
#[allow(missing_docs)]
pub type Date32BufferBuilder = BufferBuilder<Date32Type>;
#[allow(missing_docs)]
pub type Date64BufferBuilder = BufferBuilder<Date64Type>;
#[allow(missing_docs)]
pub type Time32SecondBufferBuilder = BufferBuilder<Time32SecondType>;
#[allow(missing_docs)]
pub type Time32MillisecondBufferBuilder = BufferBuilder<Time32MillisecondType>;
#[allow(missing_docs)]
pub type Time64MicrosecondBufferBuilder = BufferBuilder<Time64MicrosecondType>;
#[allow(missing_docs)]
pub type Time64NanosecondBufferBuilder = BufferBuilder<Time64NanosecondType>;
#[allow(missing_docs)]
pub type IntervalYearMonthBufferBuilder = BufferBuilder<IntervalYearMonthType>;
#[allow(missing_docs)]
pub type IntervalDayTimeBufferBuilder = BufferBuilder<IntervalDayTimeType>;
#[allow(missing_docs)]
pub type DurationSecondBufferBuilder = BufferBuilder<DurationSecondType>;
#[allow(missing_docs)]
pub type DurationMillisecondBufferBuilder = BufferBuilder<DurationMillisecondType>;
#[allow(missing_docs)]
pub type DurationMicrosecondBufferBuilder = BufferBuilder<DurationMicrosecondType>;
#[allow(missing_docs)]
pub type DurationNanosecondBufferBuilder = BufferBuilder<DurationNanosecondType>;

pub use self::builder::ArrayBuilder;
pub use self::builder::BinaryBuilder;
pub use self::builder::FixedSizeBinaryBuilder;
pub use self::builder::FixedSizeListBuilder;
pub use self::builder::ListBuilder;
pub use self::builder::PrimitiveBuilder;
pub use self::builder::PrimitiveDictionaryBuilder;
pub use self::builder::StringBuilder;
pub use self::builder::StructBuilder;

#[allow(missing_docs)]
pub type BooleanBuilder = PrimitiveBuilder<BooleanType>;
#[allow(missing_docs)]
pub type Int8Builder = PrimitiveBuilder<Int8Type>;
#[allow(missing_docs)]
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
#[allow(missing_docs)]
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
#[allow(missing_docs)]
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
#[allow(missing_docs)]
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
#[allow(missing_docs)]
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
#[allow(missing_docs)]
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
#[allow(missing_docs)]
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
#[allow(missing_docs)]
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
#[allow(missing_docs)]
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

#[allow(missing_docs)]
pub type TimestampSecondBuilder = PrimitiveBuilder<TimestampSecondType>;
#[allow(missing_docs)]
pub type TimestampMillisecondBuilder = PrimitiveBuilder<TimestampMillisecondType>;
#[allow(missing_docs)]
pub type TimestampMicrosecondBuilder = PrimitiveBuilder<TimestampMicrosecondType>;
#[allow(missing_docs)]
pub type TimestampNanosecondBuilder = PrimitiveBuilder<TimestampNanosecondType>;
#[allow(missing_docs)]
pub type Date32Builder = PrimitiveBuilder<Date32Type>;
#[allow(missing_docs)]
pub type Date64Builder = PrimitiveBuilder<Date64Type>;
#[allow(missing_docs)]
pub type Time32SecondBuilder = PrimitiveBuilder<Time32SecondType>;
#[allow(missing_docs)]
pub type Time32MillisecondBuilder = PrimitiveBuilder<Time32MillisecondType>;
#[allow(missing_docs)]
pub type Time64MicrosecondBuilder = PrimitiveBuilder<Time64MicrosecondType>;
#[allow(missing_docs)]
pub type Time64NanosecondBuilder = PrimitiveBuilder<Time64NanosecondType>;
#[allow(missing_docs)]
pub type IntervalYearMonthBuilder = PrimitiveBuilder<IntervalYearMonthType>;
#[allow(missing_docs)]
pub type IntervalDayTimeBuilder = PrimitiveBuilder<IntervalDayTimeType>;
#[allow(missing_docs)]
pub type DurationSecondBuilder = PrimitiveBuilder<DurationSecondType>;
#[allow(missing_docs)]
pub type DurationMillisecondBuilder = PrimitiveBuilder<DurationMillisecondType>;
#[allow(missing_docs)]
pub type DurationMicrosecondBuilder = PrimitiveBuilder<DurationMicrosecondType>;
#[allow(missing_docs)]
pub type DurationNanosecondBuilder = PrimitiveBuilder<DurationNanosecondType>;

// --------------------- Array Equality ---------------------

pub use self::equal::ArrayEqual;
pub use self::equal::JsonEqual;
