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

//! Defines the logical data types of Arrow arrays.
//!
//! The most important things you might be looking for are:
//!  * [`Schema`](crate::datatypes::Schema) to describe a schema.
//!  * [`Field`](crate::datatypes::Field) to describe one field within a schema.
//!  * [`DataType`](crate::datatypes::DataType) to describe the type of a field.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::mem::size_of;
use std::ops::Neg;
#[cfg(feature = "simd")]
use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Div, Mul, Not, Sub};
use std::slice::from_raw_parts;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "simd")]
use packed_simd::*;
use serde_derive::{Deserialize, Serialize};
use serde_json::{
    json, Number, Value, Value::Number as VNumber, Value::String as VString,
};

use crate::error::{ArrowError, Result};

/// The set of datatypes that are supported by this implementation of Apache Arrow.
///
/// The Arrow specification on data types includes some more types.
/// See also [`Schema.fbs`](https://github.com/apache/arrow/blob/master/format/Schema.fbs)
/// for Arrow's specification.
///
/// The variants of this enum include primitive fixed size types as well as parametric or
/// nested types.
/// Currently the Rust implementation supports the following  nested types:
///  - `List<T>`
///  - `Struct<T, U, V, ...>`
///
/// Nested types can themselves be nested within other arrays.
/// For more information on these types please see
/// [the physical memory layout of Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    Timestamp(TimeUnit, Option<String>),
    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32(DateUnit),
    /// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits).
    Date64(DateUnit),
    /// A 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time32(TimeUnit),
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during day light savings time transitions).
    Interval(IntervalUnit),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// A list of some logical data type with variable length.
    List(Box<Field>),
    /// A list of some logical data type with fixed length.
    FixedSizeList(Box<Field>, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    LargeList(Box<Field>),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Vec<Field>),
    /// A nested datatype that can represent slots of differing types.
    Union(Vec<Field>),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// Decimal value with precision and scale
    Decimal(usize, usize),
}

/// Date is either a 32-bit or 64-bit type representing elapsed time since UNIX
/// epoch (1970-01-01) in days or milliseconds.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DateUnit {
    /// Days since the UNIX epoch.
    Day,
    /// Milliseconds indicating UNIX time elapsed since the epoch (no
    /// leap seconds), where the values are evenly divisible by 86400000.
    Millisecond,
}

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

/// YEAR_MONTH or DAY_TIME interval in SQL style.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (8-bytes in total).
    DayTime,
}

/// Contains the meta-data for a single relative type.
///
/// The `Schema` object is an ordered collection of `Field` objects.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<BTreeMap<String, String>>,
}

/// Trait declaring any type that is serializable to JSON. This includes all primitive types (bool, i32, etc.).
pub trait JsonSerializable: 'static {
    fn into_json_value(self) -> Option<Value>;
}

/// Trait expressing a Rust type that has the same in-memory representation
/// as Arrow. This includes `i16`, `f32`, but excludes `bool` (which in arrow is represented in bits).
/// In little endian machines, types that implement [`ArrowNativeType`] can be memcopied to arrow buffers
/// as is.
pub trait ArrowNativeType:
    fmt::Debug + Send + Sync + Copy + PartialOrd + FromStr + Default + JsonSerializable
{
    /// Convert native type from usize.
    fn from_usize(_: usize) -> Option<Self> {
        None
    }

    /// Convert native type to usize.
    fn to_usize(&self) -> Option<usize> {
        None
    }

    /// Convert native type from i32.
    fn from_i32(_: i32) -> Option<Self> {
        None
    }

    /// Convert native type from i64.
    fn from_i64(_: i64) -> Option<Self> {
        None
    }
}

/// Trait bridging the dynamic-typed nature of Arrow (via [`DataType`]) with the
/// static-typed nature of rust types ([`ArrowNativeType`]) for all types that implement [`ArrowNativeType`].
pub trait ArrowPrimitiveType: 'static {
    /// Corresponding Rust native type for the primitive type.
    type Native: ArrowNativeType;

    /// the corresponding Arrow data type of this primitive type.
    const DATA_TYPE: DataType;

    /// Returns the byte width of this primitive type.
    fn get_byte_width() -> usize {
        size_of::<Self::Native>()
    }

    /// Returns a default value of this primitive type.
    ///
    /// This is useful for aggregate array ops like `sum()`, `mean()`.
    fn default_value() -> Self::Native {
        Default::default()
    }
}

impl JsonSerializable for bool {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for i8 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for i8 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for i16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for i16 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for i32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for i32 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    /// Convert native type from i32.
    fn from_i32(val: i32) -> Option<Self> {
        Some(val)
    }
}

impl JsonSerializable for i64 {
    fn into_json_value(self) -> Option<Value> {
        Some(VNumber(Number::from(self)))
    }
}

impl ArrowNativeType for i64 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    /// Convert native type from i64.
    fn from_i64(val: i64) -> Option<Self> {
        Some(val)
    }
}

impl JsonSerializable for u8 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u8 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for u16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u16 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for u32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u32 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for u64 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u64 {
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }
}

impl JsonSerializable for f32 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(self as f64 * 1000.0) / 1000.0).map(VNumber)
    }
}

impl JsonSerializable for f64 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(self).map(VNumber)
    }
}

impl ArrowNativeType for f32 {}
impl ArrowNativeType for f64 {}

// BooleanType is special: its bit-width is not the size of the primitive type, and its `index`
// operation assumes bit-packing.
#[derive(Debug)]
pub struct BooleanType {}

impl BooleanType {
    pub const DATA_TYPE: DataType = DataType::Boolean;
}

macro_rules! make_type {
    ($name:ident, $native_ty:ty, $data_ty:expr) => {
        #[derive(Debug)]
        pub struct $name {}

        impl ArrowPrimitiveType for $name {
            type Native = $native_ty;
            const DATA_TYPE: DataType = $data_ty;
        }
    };
}

make_type!(Int8Type, i8, DataType::Int8);
make_type!(Int16Type, i16, DataType::Int16);
make_type!(Int32Type, i32, DataType::Int32);
make_type!(Int64Type, i64, DataType::Int64);
make_type!(UInt8Type, u8, DataType::UInt8);
make_type!(UInt16Type, u16, DataType::UInt16);
make_type!(UInt32Type, u32, DataType::UInt32);
make_type!(UInt64Type, u64, DataType::UInt64);
make_type!(Float32Type, f32, DataType::Float32);
make_type!(Float64Type, f64, DataType::Float64);
make_type!(
    TimestampSecondType,
    i64,
    DataType::Timestamp(TimeUnit::Second, None)
);
make_type!(
    TimestampMillisecondType,
    i64,
    DataType::Timestamp(TimeUnit::Millisecond, None)
);
make_type!(
    TimestampMicrosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Microsecond, None)
);
make_type!(
    TimestampNanosecondType,
    i64,
    DataType::Timestamp(TimeUnit::Nanosecond, None)
);
make_type!(Date32Type, i32, DataType::Date32(DateUnit::Day));
make_type!(Date64Type, i64, DataType::Date64(DateUnit::Millisecond));
make_type!(Time32SecondType, i32, DataType::Time32(TimeUnit::Second));
make_type!(
    Time32MillisecondType,
    i32,
    DataType::Time32(TimeUnit::Millisecond)
);
make_type!(
    Time64MicrosecondType,
    i64,
    DataType::Time64(TimeUnit::Microsecond)
);
make_type!(
    Time64NanosecondType,
    i64,
    DataType::Time64(TimeUnit::Nanosecond)
);
make_type!(
    IntervalYearMonthType,
    i32,
    DataType::Interval(IntervalUnit::YearMonth)
);
make_type!(
    IntervalDayTimeType,
    i64,
    DataType::Interval(IntervalUnit::DayTime)
);
make_type!(
    DurationSecondType,
    i64,
    DataType::Duration(TimeUnit::Second)
);
make_type!(
    DurationMillisecondType,
    i64,
    DataType::Duration(TimeUnit::Millisecond)
);
make_type!(
    DurationMicrosecondType,
    i64,
    DataType::Duration(TimeUnit::Microsecond)
);
make_type!(
    DurationNanosecondType,
    i64,
    DataType::Duration(TimeUnit::Nanosecond)
);

/// A subtype of primitive type that represents legal dictionary keys.
/// See <https://arrow.apache.org/docs/format/Columnar.html>
pub trait ArrowDictionaryKeyType: ArrowPrimitiveType {}

impl ArrowDictionaryKeyType for Int8Type {}

impl ArrowDictionaryKeyType for Int16Type {}

impl ArrowDictionaryKeyType for Int32Type {}

impl ArrowDictionaryKeyType for Int64Type {}

impl ArrowDictionaryKeyType for UInt8Type {}

impl ArrowDictionaryKeyType for UInt16Type {}

impl ArrowDictionaryKeyType for UInt32Type {}

impl ArrowDictionaryKeyType for UInt64Type {}

/// A subtype of primitive type that represents numeric values.
///
/// SIMD operations are defined in this trait if available on the target system.
#[cfg(simd)]
pub trait ArrowNumericType: ArrowPrimitiveType
where
    Self::Simd: Add<Output = Self::Simd>
        + Sub<Output = Self::Simd>
        + Mul<Output = Self::Simd>
        + Div<Output = Self::Simd>
        + Copy,
    Self::SimdMask: BitAnd<Output = Self::SimdMask>
        + BitOr<Output = Self::SimdMask>
        + BitAndAssign
        + BitOrAssign
        + Not<Output = Self::SimdMask>
        + Copy,
{
    /// Defines the SIMD type that should be used for this numeric type
    type Simd;

    /// Defines the SIMD Mask type that should be used for this numeric type
    type SimdMask;

    /// The number of SIMD lanes available
    fn lanes() -> usize;

    /// Initializes a SIMD register to a constant value
    fn init(value: Self::Native) -> Self::Simd;

    /// Loads a slice into a SIMD register
    fn load(slice: &[Self::Native]) -> Self::Simd;

    /// Creates a new SIMD mask for this SIMD type filling it with `value`
    fn mask_init(value: bool) -> Self::SimdMask;

    /// Creates a new SIMD mask for this SIMD type from the lower-most bits of the given `mask`.
    /// The number of bits used corresponds to the number of lanes of this type
    fn mask_from_u64(mask: u64) -> Self::SimdMask;

    /// Creates a bitmask from the given SIMD mask.
    /// Each bit corresponds to one vector lane, starting with the least-significant bit.
    fn mask_to_u64(mask: &Self::SimdMask) -> u64;

    /// Gets the value of a single lane in a SIMD mask
    fn mask_get(mask: &Self::SimdMask, idx: usize) -> bool;

    /// Sets the value of a single lane of a SIMD mask
    fn mask_set(mask: Self::SimdMask, idx: usize, value: bool) -> Self::SimdMask;

    /// Selects elements of `a` and `b` using `mask`
    fn mask_select(mask: Self::SimdMask, a: Self::Simd, b: Self::Simd) -> Self::Simd;

    /// Returns `true` if any of the lanes in the mask are `true`
    fn mask_any(mask: Self::SimdMask) -> bool;

    /// Performs a SIMD binary operation
    fn bin_op<F: Fn(Self::Simd, Self::Simd) -> Self::Simd>(
        left: Self::Simd,
        right: Self::Simd,
        op: F,
    ) -> Self::Simd;

    /// SIMD version of equal
    fn eq(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of not equal
    fn ne(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of less than
    fn lt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of less than or equal to
    fn le(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of greater than
    fn gt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// SIMD version of greater than or equal to
    fn ge(left: Self::Simd, right: Self::Simd) -> Self::SimdMask;

    /// Writes a SIMD result back to a slice
    fn write(simd_result: Self::Simd, slice: &mut [Self::Native]);
}

#[cfg(not(simd))]
pub trait ArrowNumericType: ArrowPrimitiveType {}

macro_rules! make_numeric_type {
    ($impl_ty:ty, $native_ty:ty, $simd_ty:ident, $simd_mask_ty:ident) => {
        #[cfg(simd)]
        impl ArrowNumericType for $impl_ty {
            type Simd = $simd_ty;

            type SimdMask = $simd_mask_ty;

            #[inline]
            fn lanes() -> usize {
                Self::Simd::lanes()
            }

            #[inline]
            fn init(value: Self::Native) -> Self::Simd {
                Self::Simd::splat(value)
            }

            #[inline]
            fn load(slice: &[Self::Native]) -> Self::Simd {
                unsafe { Self::Simd::from_slice_unaligned_unchecked(slice) }
            }

            #[inline]
            fn mask_init(value: bool) -> Self::SimdMask {
                Self::SimdMask::splat(value)
            }

            #[inline]
            fn mask_from_u64(mask: u64) -> Self::SimdMask {
                // this match will get removed by the compiler since the number of lanes is known at
                // compile-time for each concrete numeric type
                match Self::lanes() {
                    8 => {
                        // the bit position in each lane indicates the index of that lane
                        let vecidx = i64x8::new(1, 2, 4, 8, 16, 32, 64, 128);

                        // broadcast the lowermost 8 bits of mask to each lane
                        let vecmask = i64x8::splat((mask & 0xFF) as i64);
                        // compute whether the bit corresponding to each lanes index is set
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        // transmute is necessary because the different match arms return different
                        // mask types, at runtime only one of those expressions will exist per type,
                        // with the type being equal to `SimdMask`.
                        unsafe { std::mem::transmute(vecmask) }
                    }
                    16 => {
                        // same general logic as for 8 lanes, extended to 16 bits
                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        unsafe { std::mem::transmute(vecmask) }
                    }
                    32 => {
                        // compute two separate m32x16 vector masks from  from the lower-most 32 bits of `mask`
                        // and then combine them into one m16x32 vector mask by writing and reading a temporary
                        let tmp = &mut [0_i16; 32];

                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i16x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[0..16]);

                        let vecmask = i32x16::splat(((mask >> 16) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i16x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[16..32]);

                        unsafe { std::mem::transmute(i16x32::from_slice_unaligned(tmp)) }
                    }
                    64 => {
                        // compute four m32x16 vector masks from  from all 64 bits of `mask`
                        // and convert them into one m8x64 vector mask by writing and reading a temporary
                        let tmp = &mut [0_i8; 64];

                        let vecidx = i32x16::new(
                            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096,
                            8192, 16384, 32768,
                        );

                        let vecmask = i32x16::splat((mask & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[0..16]);

                        let vecmask = i32x16::splat(((mask >> 16) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[16..32]);

                        let vecmask = i32x16::splat(((mask >> 32) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[32..48]);

                        let vecmask = i32x16::splat(((mask >> 48) & 0xFFFF) as i32);
                        let vecmask = (vecidx & vecmask).eq(vecidx);

                        i8x16::from_cast(vecmask)
                            .write_to_slice_unaligned(&mut tmp[48..64]);

                        unsafe { std::mem::transmute(i8x64::from_slice_unaligned(tmp)) }
                    }
                    _ => panic!("Invalid number of vector lanes"),
                }
            }

            #[inline]
            fn mask_to_u64(mask: &Self::SimdMask) -> u64 {
                mask.bitmask() as u64
            }

            #[inline]
            fn mask_get(mask: &Self::SimdMask, idx: usize) -> bool {
                unsafe { mask.extract_unchecked(idx) }
            }

            #[inline]
            fn mask_set(mask: Self::SimdMask, idx: usize, value: bool) -> Self::SimdMask {
                unsafe { mask.replace_unchecked(idx, value) }
            }

            /// Selects elements of `a` and `b` using `mask`
            #[inline]
            fn mask_select(
                mask: Self::SimdMask,
                a: Self::Simd,
                b: Self::Simd,
            ) -> Self::Simd {
                mask.select(a, b)
            }

            #[inline]
            fn mask_any(mask: Self::SimdMask) -> bool {
                mask.any()
            }

            #[inline]
            fn bin_op<F: Fn(Self::Simd, Self::Simd) -> Self::Simd>(
                left: Self::Simd,
                right: Self::Simd,
                op: F,
            ) -> Self::Simd {
                op(left, right)
            }

            #[inline]
            fn eq(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.eq(right)
            }

            #[inline]
            fn ne(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.ne(right)
            }

            #[inline]
            fn lt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.lt(right)
            }

            #[inline]
            fn le(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.le(right)
            }

            #[inline]
            fn gt(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.gt(right)
            }

            #[inline]
            fn ge(left: Self::Simd, right: Self::Simd) -> Self::SimdMask {
                left.ge(right)
            }

            #[inline]
            fn write(simd_result: Self::Simd, slice: &mut [Self::Native]) {
                unsafe { simd_result.write_to_slice_unaligned_unchecked(slice) };
            }
        }

        #[cfg(not(simd))]
        impl ArrowNumericType for $impl_ty {}
    };
}

make_numeric_type!(Int8Type, i8, i8x64, m8x64);
make_numeric_type!(Int16Type, i16, i16x32, m16x32);
make_numeric_type!(Int32Type, i32, i32x16, m32x16);
make_numeric_type!(Int64Type, i64, i64x8, m64x8);
make_numeric_type!(UInt8Type, u8, u8x64, m8x64);
make_numeric_type!(UInt16Type, u16, u16x32, m16x32);
make_numeric_type!(UInt32Type, u32, u32x16, m32x16);
make_numeric_type!(UInt64Type, u64, u64x8, m64x8);
make_numeric_type!(Float32Type, f32, f32x16, m32x16);
make_numeric_type!(Float64Type, f64, f64x8, m64x8);

make_numeric_type!(TimestampSecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampMillisecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampMicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(TimestampNanosecondType, i64, i64x8, m64x8);
make_numeric_type!(Date32Type, i32, i32x16, m32x16);
make_numeric_type!(Date64Type, i64, i64x8, m64x8);
make_numeric_type!(Time32SecondType, i32, i32x16, m32x16);
make_numeric_type!(Time32MillisecondType, i32, i32x16, m32x16);
make_numeric_type!(Time64MicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(Time64NanosecondType, i64, i64x8, m64x8);
make_numeric_type!(IntervalYearMonthType, i32, i32x16, m32x16);
make_numeric_type!(IntervalDayTimeType, i64, i64x8, m64x8);
make_numeric_type!(DurationSecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationMillisecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationMicrosecondType, i64, i64x8, m64x8);
make_numeric_type!(DurationNanosecondType, i64, i64x8, m64x8);

/// A subtype of primitive type that represents signed numeric values.
///
/// SIMD operations are defined in this trait if available on the target system.
#[cfg(simd)]
pub trait ArrowSignedNumericType: ArrowNumericType
where
    Self::SignedSimd: Neg<Output = Self::SignedSimd>,
{
    /// Defines the SIMD type that should be used for this numeric type
    type SignedSimd;

    /// Loads a slice of signed numeric type into a SIMD register
    fn load_signed(slice: &[Self::Native]) -> Self::SignedSimd;

    /// Performs a SIMD unary operation on signed numeric type
    fn signed_unary_op<F: Fn(Self::SignedSimd) -> Self::SignedSimd>(
        a: Self::SignedSimd,
        op: F,
    ) -> Self::SignedSimd;

    /// Writes a signed SIMD result back to a slice
    fn write_signed(simd_result: Self::SignedSimd, slice: &mut [Self::Native]);
}

#[cfg(not(simd))]
pub trait ArrowSignedNumericType: ArrowNumericType
where
    Self::Native: Neg<Output = Self::Native>,
{
}

macro_rules! make_signed_numeric_type {
    ($impl_ty:ty, $simd_ty:ident) => {
        #[cfg(simd)]
        impl ArrowSignedNumericType for $impl_ty {
            type SignedSimd = $simd_ty;

            #[inline]
            fn load_signed(slice: &[Self::Native]) -> Self::SignedSimd {
                unsafe { Self::SignedSimd::from_slice_unaligned_unchecked(slice) }
            }

            #[inline]
            fn signed_unary_op<F: Fn(Self::SignedSimd) -> Self::SignedSimd>(
                a: Self::SignedSimd,
                op: F,
            ) -> Self::SignedSimd {
                op(a)
            }

            #[inline]
            fn write_signed(simd_result: Self::SignedSimd, slice: &mut [Self::Native]) {
                unsafe { simd_result.write_to_slice_unaligned_unchecked(slice) };
            }
        }

        #[cfg(not(simd))]
        impl ArrowSignedNumericType for $impl_ty {}
    };
}

make_signed_numeric_type!(Int8Type, i8x64);
make_signed_numeric_type!(Int16Type, i16x32);
make_signed_numeric_type!(Int32Type, i32x16);
make_signed_numeric_type!(Int64Type, i64x8);
make_signed_numeric_type!(Float32Type, f32x16);
make_signed_numeric_type!(Float64Type, f64x8);

/// A subtype of primitive type that represents temporal values.
pub trait ArrowTemporalType: ArrowPrimitiveType {}

impl ArrowTemporalType for TimestampSecondType {}
impl ArrowTemporalType for TimestampMillisecondType {}
impl ArrowTemporalType for TimestampMicrosecondType {}
impl ArrowTemporalType for TimestampNanosecondType {}
impl ArrowTemporalType for Date32Type {}
impl ArrowTemporalType for Date64Type {}
impl ArrowTemporalType for Time32SecondType {}
impl ArrowTemporalType for Time32MillisecondType {}
impl ArrowTemporalType for Time64MicrosecondType {}
impl ArrowTemporalType for Time64NanosecondType {}
// impl ArrowTemporalType for IntervalYearMonthType {}
// impl ArrowTemporalType for IntervalDayTimeType {}

/// A timestamp type allows us to create array builders that take a timestamp.
pub trait ArrowTimestampType: ArrowTemporalType {
    /// Returns the `TimeUnit` of this timestamp.
    fn get_time_unit() -> TimeUnit;
}

impl ArrowTimestampType for TimestampSecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Second
    }
}
impl ArrowTimestampType for TimestampMillisecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}
impl ArrowTimestampType for TimestampMicrosecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}
impl ArrowTimestampType for TimestampNanosecondType {
    fn get_time_unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

/// Allows conversion from supported Arrow types to a byte slice.
pub trait ToByteSlice {
    /// Converts this instance into a byte slice
    fn to_byte_slice(&self) -> &[u8];
}

impl<T: ArrowNativeType> ToByteSlice for [T] {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const T as *const u8;
        unsafe { from_raw_parts(raw_ptr, self.len() * size_of::<T>()) }
    }
}

impl<T: ArrowNativeType> ToByteSlice for T {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { from_raw_parts(raw_ptr, size_of::<T>()) }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl DataType {
    /// Parse a data type from a JSON representation
    pub(crate) fn from(json: &Value) -> Result<DataType> {
        let default_field = Field::new("", DataType::Boolean, true);
        match *json {
            Value::Object(ref map) => match map.get("name") {
                Some(s) if s == "null" => Ok(DataType::Null),
                Some(s) if s == "bool" => Ok(DataType::Boolean),
                Some(s) if s == "binary" => Ok(DataType::Binary),
                Some(s) if s == "largebinary" => Ok(DataType::LargeBinary),
                Some(s) if s == "utf8" => Ok(DataType::Utf8),
                Some(s) if s == "largeutf8" => Ok(DataType::LargeUtf8),
                Some(s) if s == "fixedsizebinary" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("byteWidth") {
                        Ok(DataType::FixedSizeBinary(size.as_i64().unwrap() as i32))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a byteWidth for fixedsizebinary".to_string(),
                        ))
                    }
                }
                Some(s) if s == "decimal" => {
                    // return a list with any type as its child isn't defined in the map
                    let precision = match map.get("precision") {
                        Some(p) => Ok(p.as_u64().unwrap() as usize),
                        None => Err(ArrowError::ParseError(
                            "Expecting a precision for decimal".to_string(),
                        )),
                    };
                    let scale = match map.get("scale") {
                        Some(s) => Ok(s.as_u64().unwrap() as usize),
                        _ => Err(ArrowError::ParseError(
                            "Expecting a scale for decimal".to_string(),
                        )),
                    };

                    Ok(DataType::Decimal(precision?, scale?))
                }
                Some(s) if s == "floatingpoint" => match map.get("precision") {
                    Some(p) if p == "HALF" => Ok(DataType::Float16),
                    Some(p) if p == "SINGLE" => Ok(DataType::Float32),
                    Some(p) if p == "DOUBLE" => Ok(DataType::Float64),
                    _ => Err(ArrowError::ParseError(
                        "floatingpoint precision missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "timestamp" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "timestamp unit missing or invalid".to_string(),
                        )),
                    };
                    let tz = match map.get("timezone") {
                        None => Ok(None),
                        Some(VString(tz)) => Ok(Some(tz.clone())),
                        _ => Err(ArrowError::ParseError(
                            "timezone must be a string".to_string(),
                        )),
                    };
                    Ok(DataType::Timestamp(unit?, tz?))
                }
                Some(s) if s == "date" => match map.get("unit") {
                    Some(p) if p == "DAY" => Ok(DataType::Date32(DateUnit::Day)),
                    Some(p) if p == "MILLISECOND" => {
                        Ok(DataType::Date64(DateUnit::Millisecond))
                    }
                    _ => Err(ArrowError::ParseError(
                        "date unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "time" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "time unit missing or invalid".to_string(),
                        )),
                    };
                    match map.get("bitWidth") {
                        Some(p) if p == 32 => Ok(DataType::Time32(unit?)),
                        Some(p) if p == 64 => Ok(DataType::Time64(unit?)),
                        _ => Err(ArrowError::ParseError(
                            "time bitWidth missing or invalid".to_string(),
                        )),
                    }
                }
                Some(s) if s == "duration" => match map.get("unit") {
                    Some(p) if p == "SECOND" => Ok(DataType::Duration(TimeUnit::Second)),
                    Some(p) if p == "MILLISECOND" => {
                        Ok(DataType::Duration(TimeUnit::Millisecond))
                    }
                    Some(p) if p == "MICROSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Microsecond))
                    }
                    Some(p) if p == "NANOSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Nanosecond))
                    }
                    _ => Err(ArrowError::ParseError(
                        "time unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "interval" => match map.get("unit") {
                    Some(p) if p == "DAY_TIME" => {
                        Ok(DataType::Interval(IntervalUnit::DayTime))
                    }
                    Some(p) if p == "YEAR_MONTH" => {
                        Ok(DataType::Interval(IntervalUnit::YearMonth))
                    }
                    _ => Err(ArrowError::ParseError(
                        "interval unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "int" => match map.get("isSigned") {
                    Some(&Value::Bool(true)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::Int8),
                            Some(16) => Ok(DataType::Int16),
                            Some(32) => Ok(DataType::Int32),
                            Some(64) => Ok(DataType::Int64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    Some(&Value::Bool(false)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::UInt8),
                            Some(16) => Ok(DataType::UInt16),
                            Some(32) => Ok(DataType::UInt32),
                            Some(64) => Ok(DataType::UInt64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    _ => Err(ArrowError::ParseError(
                        "int signed missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "list" => {
                    // return a list with any type as its child isn't defined in the map
                    Ok(DataType::List(Box::new(default_field)))
                }
                Some(s) if s == "largelist" => {
                    // return a largelist with any type as its child isn't defined in the map
                    Ok(DataType::LargeList(Box::new(default_field)))
                }
                Some(s) if s == "fixedsizelist" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("listSize") {
                        Ok(DataType::FixedSizeList(
                            Box::new(default_field),
                            size.as_i64().unwrap() as i32,
                        ))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a listSize for fixedsizelist".to_string(),
                        ))
                    }
                }
                Some(s) if s == "struct" => {
                    // return an empty `struct` type as its children aren't defined in the map
                    Ok(DataType::Struct(vec![]))
                }
                Some(other) => Err(ArrowError::ParseError(format!(
                    "invalid or unsupported type name: {} in {:?}",
                    other, json
                ))),
                None => Err(ArrowError::ParseError("type name missing".to_string())),
            },
            _ => Err(ArrowError::ParseError(
                "invalid json value type".to_string(),
            )),
        }
    }

    /// Generate a JSON representation of the data type
    pub fn to_json(&self) -> Value {
        match self {
            DataType::Null => json!({"name": "null"}),
            DataType::Boolean => json!({"name": "bool"}),
            DataType::Int8 => json!({"name": "int", "bitWidth": 8, "isSigned": true}),
            DataType::Int16 => json!({"name": "int", "bitWidth": 16, "isSigned": true}),
            DataType::Int32 => json!({"name": "int", "bitWidth": 32, "isSigned": true}),
            DataType::Int64 => json!({"name": "int", "bitWidth": 64, "isSigned": true}),
            DataType::UInt8 => json!({"name": "int", "bitWidth": 8, "isSigned": false}),
            DataType::UInt16 => json!({"name": "int", "bitWidth": 16, "isSigned": false}),
            DataType::UInt32 => json!({"name": "int", "bitWidth": 32, "isSigned": false}),
            DataType::UInt64 => json!({"name": "int", "bitWidth": 64, "isSigned": false}),
            DataType::Float16 => json!({"name": "floatingpoint", "precision": "HALF"}),
            DataType::Float32 => json!({"name": "floatingpoint", "precision": "SINGLE"}),
            DataType::Float64 => json!({"name": "floatingpoint", "precision": "DOUBLE"}),
            DataType::Utf8 => json!({"name": "utf8"}),
            DataType::LargeUtf8 => json!({"name": "largeutf8"}),
            DataType::Binary => json!({"name": "binary"}),
            DataType::LargeBinary => json!({"name": "largebinary"}),
            DataType::FixedSizeBinary(byte_width) => {
                json!({"name": "fixedsizebinary", "byteWidth": byte_width})
            }
            DataType::Struct(_) => json!({"name": "struct"}),
            DataType::Union(_) => json!({"name": "union"}),
            DataType::List(_) => json!({ "name": "list"}),
            DataType::LargeList(_) => json!({ "name": "largelist"}),
            DataType::FixedSizeList(_, length) => {
                json!({"name":"fixedsizelist", "listSize": length})
            }
            DataType::Time32(unit) => {
                json!({"name": "time", "bitWidth": 32, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Time64(unit) => {
                json!({"name": "time", "bitWidth": 64, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Date32(unit) | DataType::Date64(unit) => {
                json!({"name": "date", "unit": match unit {
                    DateUnit::Day => "DAY",
                    DateUnit::Millisecond => "MILLISECOND",
                }})
            }
            DataType::Timestamp(unit, None) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Timestamp(unit, Some(tz)) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }, "timezone": tz})
            }
            DataType::Interval(unit) => json!({"name": "interval", "unit": match unit {
                IntervalUnit::YearMonth => "YEAR_MONTH",
                IntervalUnit::DayTime => "DAY_TIME",
            }}),
            DataType::Duration(unit) => json!({"name": "duration", "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }}),
            DataType::Dictionary(_, _) => json!({ "name": "dictionary"}),
            DataType::Decimal(precision, scale) => {
                json!({"name": "decimal", "precision": precision, "scale": scale})
            }
        }
    }

    /// Returns true if this type is numeric: (UInt*, Unit*, or Float*)
    pub fn is_numeric(t: &DataType) -> bool {
        use DataType::*;
        matches!(
            t,
            UInt8
                | UInt16
                | UInt32
                | UInt64
                | Int8
                | Int16
                | Int32
                | Int64
                | Float32
                | Float64
        )
    }

    /// Compares the datatype with another, ignoring nested field names
    /// and metadata
    pub(crate) fn equals_datatype(&self, other: &DataType) -> bool {
        match (&self, other) {
            (DataType::List(a), DataType::List(b))
            | (DataType::LargeList(a), DataType::LargeList(b)) => {
                a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
                a_size == b_size
                    && a.is_nullable() == b.is_nullable()
                    && a.data_type().equals_datatype(b.data_type())
            }
            (DataType::Struct(a), DataType::Struct(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| {
                        a.is_nullable() == b.is_nullable()
                            && a.data_type().equals_datatype(b.data_type())
                    })
            }
            _ => self == other,
        }
    }
}

impl Field {
    /// Creates a new field
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        }
    }

    /// Creates a new field
    pub fn new_dict(
        name: &str,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: None,
        }
    }

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    #[inline]
    pub fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>) {
        // To make serde happy, convert Some(empty_map) to None.
        self.metadata = None;
        if let Some(v) = metadata {
            if !v.is_empty() {
                self.metadata = Some(v);
            }
        }
    }

    /// Returns the immutable reference to the `Field`'s optional custom metadata.
    #[inline]
    pub const fn metadata(&self) -> &Option<BTreeMap<String, String>> {
        &self.metadata
    }

    /// Returns an immutable reference to the `Field`'s name
    #[inline]
    pub const fn name(&self) -> &String {
        &self.name
    }

    /// Returns an immutable reference to the `Field`'s  data-type
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Indicates whether this `Field` supports null values
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the dictionary ID, if this is a dictionary type
    #[inline]
    pub const fn dict_id(&self) -> Option<i64> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_id),
            _ => None,
        }
    }

    /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type
    #[inline]
    pub const fn dict_is_ordered(&self) -> Option<bool> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_is_ordered),
            _ => None,
        }
    }

    /// Parse a `Field` definition from a JSON representation
    pub fn from(json: &Value) -> Result<Self> {
        match *json {
            Value::Object(ref map) => {
                let name = match map.get("name") {
                    Some(&Value::String(ref name)) => name.to_string(),
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'name' attribute".to_string(),
                        ));
                    }
                };
                let nullable = match map.get("nullable") {
                    Some(&Value::Bool(b)) => b,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'nullable' attribute".to_string(),
                        ));
                    }
                };
                let data_type = match map.get("type") {
                    Some(t) => DataType::from(t)?,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'type' attribute".to_string(),
                        ));
                    }
                };

                // Referenced example file: testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_custom_metadata.json.gz
                let metadata = match map.get("metadata") {
                    Some(&Value::Array(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for value in values {
                            match value.as_object() {
                                Some(map) => {
                                    if map.len() != 2 {
                                        return Err(ArrowError::ParseError(
                                            "Field 'metadata' must have exact two entries for each key-value map".to_string(),
                                        ));
                                    }
                                    if let (Some(k), Some(v)) =
                                        (map.get("key"), map.get("value"))
                                    {
                                        if let (Some(k_str), Some(v_str)) =
                                            (k.as_str(), v.as_str())
                                        {
                                            res.insert(
                                                k_str.to_string().clone(),
                                                v_str.to_string().clone(),
                                            );
                                        } else {
                                            return Err(ArrowError::ParseError("Field 'metadata' must have map value of string type".to_string()));
                                        }
                                    } else {
                                        return Err(ArrowError::ParseError("Field 'metadata' lacks map keys named \"key\" or \"value\"".to_string()));
                                    }
                                }
                                _ => {
                                    return Err(ArrowError::ParseError(
                                        "Field 'metadata' contains non-object key-value pair".to_string(),
                                    ));
                                }
                            }
                        }
                        Some(res)
                    }
                    // We also support map format, because Schema's metadata supports this.
                    // See https://github.com/apache/arrow/pull/5907
                    Some(&Value::Object(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for (k, v) in values {
                            if let Some(str_value) = v.as_str() {
                                res.insert(k.clone(), str_value.to_string().clone());
                            } else {
                                return Err(ArrowError::ParseError(
                                    format!("Field 'metadata' contains non-string value for key {}", k),
                                ));
                            }
                        }
                        Some(res)
                    }
                    Some(_) => {
                        return Err(ArrowError::ParseError(
                            "Field `metadata` is not json array".to_string(),
                        ));
                    }
                    _ => None,
                };

                // if data_type is a struct or list, get its children
                let data_type = match data_type {
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            if values.len() != 1 {
                                return Err(ArrowError::ParseError(
                                    "Field 'children' must have one element for a list data type".to_string(),
                                ));
                            }
                            match data_type {
                                    DataType::List(_) => {
                                        DataType::List(Box::new(Self::from(&values[0])?))
                                    }
                                    DataType::LargeList(_) => {
                                        DataType::LargeList(Box::new(Self::from(&values[0])?))
                                    }
                                    DataType::FixedSizeList(_, int) => DataType::FixedSizeList(
                                        Box::new(Self::from(&values[0])?),
                                        int,
                                    ),
                                    _ => unreachable!(
                                        "Data type should be a list, largelist or fixedsizelist"
                                    ),
                                }
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    DataType::Struct(mut fields) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            let struct_fields: Result<Vec<Field>> =
                                values.iter().map(|v| Field::from(v)).collect();
                            fields.append(&mut struct_fields?);
                            DataType::Struct(fields)
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    _ => data_type,
                };

                let mut dict_id = 0;
                let mut dict_is_ordered = false;

                let data_type = match map.get("dictionary") {
                    Some(dictionary) => {
                        let index_type = match dictionary.get("indexType") {
                            Some(t) => DataType::from(t)?,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'indexType' attribute".to_string(),
                                ));
                            }
                        };
                        dict_id = match dictionary.get("id") {
                            Some(Value::Number(n)) => n.as_i64().unwrap(),
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'id' attribute".to_string(),
                                ));
                            }
                        };
                        dict_is_ordered = match dictionary.get("isOrdered") {
                            Some(&Value::Bool(n)) => n,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'isOrdered' attribute".to_string(),
                                ));
                            }
                        };
                        DataType::Dictionary(Box::new(index_type), Box::new(data_type))
                    }
                    _ => data_type,
                };
                Ok(Field {
                    name,
                    nullable,
                    data_type,
                    dict_id,
                    dict_is_ordered,
                    metadata,
                })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for field".to_string(),
            )),
        }
    }

    /// Generate a JSON representation of the `Field`
    pub fn to_json(&self) -> Value {
        let children: Vec<Value> = match self.data_type() {
            DataType::Struct(fields) => fields.iter().map(|f| f.to_json()).collect(),
            DataType::List(field) => vec![field.to_json()],
            DataType::LargeList(field) => vec![field.to_json()],
            DataType::FixedSizeList(field, _) => vec![field.to_json()],
            _ => vec![],
        };
        match self.data_type() {
            DataType::Dictionary(ref index_type, ref value_type) => json!({
                "name": self.name,
                "nullable": self.nullable,
                "type": value_type.to_json(),
                "children": children,
                "dictionary": {
                    "id": self.dict_id,
                    "indexType": index_type.to_json(),
                    "isOrdered": self.dict_is_ordered
                }
            }),
            _ => json!({
                "name": self.name,
                "nullable": self.nullable,
                "type": self.data_type.to_json(),
                "children": children
            }),
        }
    }

    /// Merge field into self if it is compatible. Struct will be merged recursively.
    /// NOTE: `self` may be updated to unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    ///
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    pub fn try_merge(&mut self, from: &Field) -> Result<()> {
        // merge metadata
        match (self.metadata(), from.metadata()) {
            (Some(self_metadata), Some(from_metadata)) => {
                let mut merged = self_metadata.clone();
                for (key, from_value) in from_metadata {
                    if let Some(self_value) = self_metadata.get(key) {
                        if self_value != from_value {
                            return Err(ArrowError::SchemaError(format!(
                                "Fail to merge field due to conflicting metadata data value for key {}", key),
                            ));
                        }
                    } else {
                        merged.insert(key.clone(), from_value.clone());
                    }
                }
                self.set_metadata(Some(merged));
            }
            (None, Some(from_metadata)) => {
                self.set_metadata(Some(from_metadata.clone()));
            }
            _ => {}
        }
        if from.dict_id != self.dict_id {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_id".to_string(),
            ));
        }
        if from.dict_is_ordered != self.dict_is_ordered {
            return Err(ArrowError::SchemaError(
                "Fail to merge schema Field due to conflicting dict_is_ordered"
                    .to_string(),
            ));
        }
        match &mut self.data_type {
            DataType::Struct(nested_fields) => match &from.data_type {
                DataType::Struct(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if self_field.name != from_field.name {
                                continue;
                            }
                            is_new_field = false;
                            self_field.try_merge(&from_field)?;
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Union(nested_fields) => match &from.data_type {
                DataType::Union(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if from_field == self_field {
                                is_new_field = false;
                                break;
                            }
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            },
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::List(_)
            | DataType::Dictionary(_, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal(_, _) => {
                if self.data_type != from.data_type {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema Field due to conflicting datatype"
                            .to_string(),
                    ));
                }
            }
        }
        if from.nullable {
            self.nullable = from.nullable;
        }

        Ok(())
    }
}

// TODO: improve display with crate https://crates.io/crates/derive_more ?
impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub(crate) fields: Vec<Field>,
    /// A map of key-value pairs containing additional meta data.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub(crate) metadata: HashMap<String, String>,
}

impl Schema {
    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: HashMap::new(),
        }
    }

    /// Creates a new `Schema` from a sequence of `Field` values
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate arrow;
    /// # use arrow::datatypes::{Field, DataType, Schema};
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let schema = Schema::new(vec![field_a, field_b]);
    /// ```
    pub fn new(fields: Vec<Field>) -> Self {
        Self::new_with_metadata(fields, HashMap::new())
    }

    /// Creates a new `Schema` from a sequence of `Field` values
    /// and adds additional metadata in form of key value pairs.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate arrow;
    /// # use arrow::datatypes::{Field, DataType, Schema};
    /// # use std::collections::HashMap;
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let mut metadata: HashMap<String, String> = HashMap::new();
    /// metadata.insert("row_count".to_string(), "100".to_string());
    ///
    /// let schema = Schema::new_with_metadata(vec![field_a, field_b], metadata);
    /// ```
    #[inline]
    pub const fn new_with_metadata(
        fields: Vec<Field>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self { fields, metadata }
    }

    /// Merge schema into self if it is compatible. Struct fields will be merged recursively.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    ///
    /// let merged = Schema::try_merge(&vec![
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, false),
    ///         Field::new("c2", DataType::Utf8, false),
    ///     ]),
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///     merged,
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// );
    /// ```
    pub fn try_merge(schemas: &[Self]) -> Result<Self> {
        let mut merged = Self::empty();

        for schema in schemas {
            for (key, value) in schema.metadata.iter() {
                // merge metadata
                match merged.metadata.get(key) {
                    Some(old_val) => {
                        if old_val != value {
                            return Err(ArrowError::SchemaError(
                                "Fail to merge schema due to conflicting metadata"
                                    .to_string(),
                            ));
                        }
                    }
                    None => {
                        merged.metadata.insert(key.clone(), value.clone());
                    }
                }
            }
            // merge fields
            for field in &schema.fields {
                let mut new_field = true;
                for merged_field in &mut merged.fields {
                    if field.name != merged_field.name {
                        continue;
                    }
                    new_field = false;
                    merged_field.try_merge(field)?
                }
                // found a new field, add to field list
                if new_field {
                    merged.fields.push(field.clone());
                }
            }
        }

        Ok(merged)
    }

    /// Returns an immutable reference of the vector of `Field` instances
    #[inline]
    pub const fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name
    pub fn field_with_name(&self, name: &str) -> Result<&Field> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Returns a vector of immutable references to all `Field` instances selected by
    /// the dictionary ID they use
    pub fn fields_with_dict_id(&self, dict_id: i64) -> Vec<&Field> {
        self.fields
            .iter()
            .filter(|f| f.dict_id() == Some(dict_id))
            .collect()
    }

    /// Find the index of the column with the given name
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> =
            self.fields.iter().map(|f| f.name().clone()).collect();
        Err(ArrowError::InvalidArgumentError(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    #[inline]
    pub const fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// it's index
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name == name)
    }

    /// Generate a JSON representation of the `Schema`
    pub fn to_json(&self) -> Value {
        json!({
            "fields": self.fields.iter().map(|field| field.to_json()).collect::<Vec<Value>>(),
            "metadata": serde_json::to_value(&self.metadata).unwrap()
        })
    }

    /// Parse a `Schema` definition from a JSON representation
    pub fn from(json: &Value) -> Result<Self> {
        match *json {
            Value::Object(ref schema) => {
                let fields = if let Some(Value::Array(fields)) = schema.get("fields") {
                    fields
                        .iter()
                        .map(|f| Field::from(f))
                        .collect::<Result<_>>()?
                } else {
                    return Err(ArrowError::ParseError(
                        "Schema fields should be an array".to_string(),
                    ));
                };

                let metadata = if let Some(value) = schema.get("metadata") {
                    Self::from_metadata(value)?
                } else {
                    HashMap::default()
                };

                Ok(Self { fields, metadata })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for schema".to_string(),
            )),
        }
    }

    /// Parse a `metadata` definition from a JSON representation
    /// The JSON can either be an Object or an Array of Objects
    fn from_metadata(json: &Value) -> Result<HashMap<String, String>> {
        match json {
            Value::Array(_) => {
                let mut hashmap = HashMap::new();
                let values: Vec<MetadataKeyValue> = serde_json::from_value(json.clone())
                    .map_err(|_| {
                        ArrowError::JsonError(
                            "Unable to parse object into key-value pair".to_string(),
                        )
                    })?;
                for meta in values {
                    hashmap.insert(meta.key.clone(), meta.value);
                }
                Ok(hashmap)
            }
            Value::Object(md) => md
                .iter()
                .map(|(k, v)| {
                    if let Value::String(v) = v {
                        Ok((k.to_string(), v.to_string()))
                    } else {
                        Err(ArrowError::ParseError(
                            "metadata `value` field must be a string".to_string(),
                        ))
                    }
                })
                .collect::<Result<_>>(),
            _ => Err(ArrowError::ParseError(
                "`metadata` field must be an object".to_string(),
            )),
        }
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(
            &self
                .fields
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

/// A reference-counted reference to a [`Schema`](crate::datatypes::Schema).
pub type SchemaRef = Arc<Schema>;

#[derive(Deserialize)]
struct MetadataKeyValue {
    key: String,
    value: String,
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Number;
    use serde_json::Value::{Bool, Number as VNumber};
    use std::f32::NAN;

    #[test]
    fn test_list_datatype_equality() {
        // tests that list type equality is checked while ignoring list names
        let list_a = DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let list_b = DataType::List(Box::new(Field::new("array", DataType::Int32, true)));
        let list_c = DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_d = DataType::List(Box::new(Field::new("item", DataType::UInt32, true)));
        assert!(list_a.equals_datatype(&list_b));
        assert!(!list_a.equals_datatype(&list_c));
        assert!(!list_b.equals_datatype(&list_c));
        assert!(!list_a.equals_datatype(&list_d));

        let list_e =
            DataType::FixedSizeList(Box::new(Field::new("item", list_a, false)), 3);
        let list_f =
            DataType::FixedSizeList(Box::new(Field::new("array", list_b, false)), 3);
        let list_g = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::FixedSizeBinary(3), true)),
            3,
        );
        assert!(list_e.equals_datatype(&list_f));
        assert!(!list_e.equals_datatype(&list_g));
        assert!(!list_f.equals_datatype(&list_g));

        let list_h = DataType::Struct(vec![Field::new("f1", list_e, true)]);
        let list_i = DataType::Struct(vec![Field::new("f1", list_f.clone(), true)]);
        let list_j = DataType::Struct(vec![Field::new("f1", list_f.clone(), false)]);
        let list_k = DataType::Struct(vec![
            Field::new("f1", list_f.clone(), false),
            Field::new("f2", list_g.clone(), false),
            Field::new("f3", DataType::Utf8, true),
        ]);
        let list_l = DataType::Struct(vec![
            Field::new("ff1", list_f.clone(), false),
            Field::new("ff2", list_g.clone(), false),
            Field::new("ff3", DataType::LargeUtf8, true),
        ]);
        let list_m = DataType::Struct(vec![
            Field::new("ff1", list_f, false),
            Field::new("ff2", list_g, false),
            Field::new("ff3", DataType::Utf8, true),
        ]);
        assert!(list_h.equals_datatype(&list_i));
        assert!(!list_h.equals_datatype(&list_j));
        assert!(!list_k.equals_datatype(&list_l));
        assert!(list_k.equals_datatype(&list_m));
    }

    #[test]
    fn create_struct_type() {
        let _person = DataType::Struct(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);
    }

    #[test]
    fn serde_struct_type() {
        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: BTreeMap<String, String> = kv_array.iter().cloned().collect();

        // Non-empty map: should be converted as JSON obj { ... }
        let mut first_name = Field::new("first_name", DataType::Utf8, false);
        first_name.set_metadata(Some(field_metadata));

        // Empty map: should be omitted.
        let mut last_name = Field::new("last_name", DataType::Utf8, false);
        last_name.set_metadata(Some(BTreeMap::default()));

        let person = DataType::Struct(vec![
            first_name,
            last_name,
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);

        let serialized = serde_json::to_string(&person).unwrap();

        // NOTE that this is testing the default (derived) serialization format, not the
        // JSON format specified in metadata.md

        assert_eq!(
            "{\"Struct\":[\
             {\"name\":\"first_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{\"k\":\"v\"}},\
             {\"name\":\"last_name\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},\
             {\"name\":\"address\",\"data_type\":{\"Struct\":\
             [{\"name\":\"street\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},\
             {\"name\":\"zip\",\"data_type\":\"UInt16\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false}\
             ]},\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false}]}",
            serialized
        );

        let deserialized = serde_json::from_str(&serialized).unwrap();

        assert_eq!(person, deserialized);
    }

    #[test]
    fn struct_field_to_json() {
        let f = Field::new(
            "address",
            DataType::Struct(vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ]),
            false,
        );
        let value: Value = serde_json::from_str(
            r#"{
                "name": "address",
                "nullable": false,
                "type": {
                    "name": "struct"
                },
                "children": [
                    {
                        "name": "street",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "zip",
                        "nullable": false,
                        "type": {
                            "name": "int",
                            "bitWidth": 16,
                            "isSigned": false
                        },
                        "children": []
                    }
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(value, f.to_json());
    }

    #[test]
    fn primitive_field_to_json() {
        let f = Field::new("first_name", DataType::Utf8, false);
        let value: Value = serde_json::from_str(
            r#"{
                "name": "first_name",
                "nullable": false,
                "type": {
                    "name": "utf8"
                },
                "children": []
            }"#,
        )
        .unwrap();
        assert_eq!(value, f.to_json());
    }
    #[test]
    fn parse_struct_from_json() {
        let json = r#"
        {
            "name": "address",
            "type": {
                "name": "struct"
            },
            "nullable": false,
            "children": [
                {
                    "name": "street",
                    "type": {
                    "name": "utf8"
                    },
                    "nullable": false,
                    "children": []
                },
                {
                    "name": "zip",
                    "type": {
                    "name": "int",
                    "isSigned": false,
                    "bitWidth": 16
                    },
                    "nullable": false,
                    "children": []
                }
            ]
        }
        "#;
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = Field::from(&value).unwrap();

        let expected = Field::new(
            "address",
            DataType::Struct(vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ]),
            false,
        );

        assert_eq!(expected, dt);
    }

    #[test]
    fn parse_utf8_from_json() {
        let json = "{\"name\":\"utf8\"}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = DataType::from(&value).unwrap();
        assert_eq!(DataType::Utf8, dt);
    }

    #[test]
    fn parse_int32_from_json() {
        let json = "{\"name\": \"int\", \"isSigned\": true, \"bitWidth\": 32}";
        let value: Value = serde_json::from_str(json).unwrap();
        let dt = DataType::from(&value).unwrap();
        assert_eq!(DataType::Int32, dt);
    }

    #[test]
    fn schema_json() {
        // Add some custom metadata
        let metadata: HashMap<String, String> =
            [("Key".to_string(), "Value".to_string())]
                .iter()
                .cloned()
                .collect();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::Binary, false),
                Field::new("c3", DataType::FixedSizeBinary(3), false),
                Field::new("c4", DataType::Boolean, false),
                Field::new("c5", DataType::Date32(DateUnit::Day), false),
                Field::new("c6", DataType::Date64(DateUnit::Millisecond), false),
                Field::new("c7", DataType::Time32(TimeUnit::Second), false),
                Field::new("c8", DataType::Time32(TimeUnit::Millisecond), false),
                Field::new("c9", DataType::Time32(TimeUnit::Microsecond), false),
                Field::new("c10", DataType::Time32(TimeUnit::Nanosecond), false),
                Field::new("c11", DataType::Time64(TimeUnit::Second), false),
                Field::new("c12", DataType::Time64(TimeUnit::Millisecond), false),
                Field::new("c13", DataType::Time64(TimeUnit::Microsecond), false),
                Field::new("c14", DataType::Time64(TimeUnit::Nanosecond), false),
                Field::new("c15", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "c16",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string())),
                    false,
                ),
                Field::new(
                    "c17",
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("Africa/Johannesburg".to_string()),
                    ),
                    false,
                ),
                Field::new(
                    "c18",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("c19", DataType::Interval(IntervalUnit::DayTime), false),
                Field::new("c20", DataType::Interval(IntervalUnit::YearMonth), false),
                Field::new(
                    "c21",
                    DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                    false,
                ),
                Field::new(
                    "c22",
                    DataType::FixedSizeList(
                        Box::new(Field::new("bools", DataType::Boolean, false)),
                        5,
                    ),
                    false,
                ),
                Field::new(
                    "c23",
                    DataType::List(Box::new(Field::new(
                        "inner_list",
                        DataType::List(Box::new(Field::new(
                            "struct",
                            DataType::Struct(vec![]),
                            true,
                        ))),
                        false,
                    ))),
                    true,
                ),
                Field::new(
                    "c24",
                    DataType::Struct(vec![
                        Field::new("a", DataType::Utf8, false),
                        Field::new("b", DataType::UInt16, false),
                    ]),
                    false,
                ),
                Field::new("c25", DataType::Interval(IntervalUnit::YearMonth), true),
                Field::new("c26", DataType::Interval(IntervalUnit::DayTime), true),
                Field::new("c27", DataType::Duration(TimeUnit::Second), false),
                Field::new("c28", DataType::Duration(TimeUnit::Millisecond), false),
                Field::new("c29", DataType::Duration(TimeUnit::Microsecond), false),
                Field::new("c30", DataType::Duration(TimeUnit::Nanosecond), false),
                Field::new_dict(
                    "c31",
                    DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Utf8),
                    ),
                    true,
                    123,
                    true,
                ),
                Field::new("c32", DataType::LargeBinary, true),
                Field::new("c33", DataType::LargeUtf8, true),
                Field::new(
                    "c34",
                    DataType::LargeList(Box::new(Field::new(
                        "inner_large_list",
                        DataType::LargeList(Box::new(Field::new(
                            "struct",
                            DataType::Struct(vec![]),
                            false,
                        ))),
                        true,
                    ))),
                    true,
                ),
            ],
            metadata,
        );

        let expected = schema.to_json();
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c2",
                        "nullable": false,
                        "type": {
                            "name": "binary"
                        },
                        "children": []
                    },
                    {
                        "name": "c3",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizebinary",
                            "byteWidth": 3
                        },
                        "children": []
                    },
                    {
                        "name": "c4",
                        "nullable": false,
                        "type": {
                            "name": "bool"
                        },
                        "children": []
                    },
                    {
                        "name": "c5",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "DAY"
                        },
                        "children": []
                    },
                    {
                        "name": "c6",
                        "nullable": false,
                        "type": {
                            "name": "date",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c7",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c8",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c9",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c10",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 32,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c11",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c12",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c13",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c14",
                        "nullable": false,
                        "type": {
                            "name": "time",
                            "bitWidth": 64,
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c15",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c16",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MILLISECOND",
                            "timezone": "UTC"
                        },
                        "children": []
                    },
                    {
                        "name": "c17",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "MICROSECOND",
                            "timezone": "Africa/Johannesburg"
                        },
                        "children": []
                    },
                    {
                        "name": "c18",
                        "nullable": false,
                        "type": {
                            "name": "timestamp",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c19",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c20",
                        "nullable": false,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c21",
                        "nullable": false,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "item",
                                "nullable": true,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c22",
                        "nullable": false,
                        "type": {
                            "name": "fixedsizelist",
                            "listSize": 5
                        },
                        "children": [
                            {
                                "name": "bools",
                                "nullable": false,
                                "type": {
                                    "name": "bool"
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c23",
                        "nullable": true,
                        "type": {
                            "name": "list"
                        },
                        "children": [
                            {
                                "name": "inner_list",
                                "nullable": false,
                                "type": {
                                    "name": "list"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": true,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "name": "c24",
                        "nullable": false,
                        "type": {
                            "name": "struct"
                        },
                        "children": [
                            {
                                "name": "a",
                                "nullable": false,
                                "type": {
                                    "name": "utf8"
                                },
                                "children": []
                            },
                            {
                                "name": "b",
                                "nullable": false,
                                "type": {
                                    "name": "int",
                                    "bitWidth": 16,
                                    "isSigned": false
                                },
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "c25",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "YEAR_MONTH"
                        },
                        "children": []
                    },
                    {
                        "name": "c26",
                        "nullable": true,
                        "type": {
                            "name": "interval",
                            "unit": "DAY_TIME"
                        },
                        "children": []
                    },
                    {
                        "name": "c27",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "SECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c28",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MILLISECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c29",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "MICROSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c30",
                        "nullable": false,
                        "type": {
                            "name": "duration",
                            "unit": "NANOSECOND"
                        },
                        "children": []
                    },
                    {
                        "name": "c31",
                        "nullable": true,
                        "children": [],
                        "type": {
                          "name": "utf8"
                        },
                        "dictionary": {
                          "id": 123,
                          "indexType": {
                            "name": "int",
                            "bitWidth": 32,
                            "isSigned": true
                          },
                          "isOrdered": true
                        }
                    },
                    {
                        "name": "c32",
                        "nullable": true,
                        "type": {
                          "name": "largebinary"
                        },
                        "children": []
                    },
                    {
                        "name": "c33",
                        "nullable": true,
                        "type": {
                          "name": "largeutf8"
                        },
                        "children": []
                    },
                    {
                        "name": "c34",
                        "nullable": true,
                        "type": {
                          "name": "largelist"
                        },
                        "children": [
                            {
                                "name": "inner_large_list",
                                "nullable": true,
                                "type": {
                                    "name": "largelist"
                                },
                                "children": [
                                    {
                                        "name": "struct",
                                        "nullable": false,
                                        "type": {
                                            "name": "struct"
                                        },
                                        "children": []
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "metadata" : {
                    "Key": "Value"
                }
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(expected, value);

        // convert back to a schema
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema2 = Schema::from(&value).unwrap();

        assert_eq!(schema, schema2);

        // Check that empty metadata produces empty value in JSON and can be parsed
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ],
                "metadata": {}
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema = Schema::from(&value).unwrap();
        assert!(schema.metadata.is_empty());

        // Check that metadata field is not required in the JSON.
        let json = r#"{
                "fields": [
                    {
                        "name": "c1",
                        "nullable": false,
                        "type": {
                            "name": "utf8"
                        },
                        "children": []
                    }
                ]
            }"#;
        let value: Value = serde_json::from_str(&json).unwrap();
        let schema = Schema::from(&value).unwrap();
        assert!(schema.metadata.is_empty());
    }

    #[test]
    fn create_schema_string() {
        let schema = person_schema();
        assert_eq!(schema.to_string(),
        "Field { name: \"first_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: Some({\"k\": \"v\"}) }, \
        Field { name: \"last_name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
        Field { name: \"address\", data_type: Struct([\
            Field { name: \"street\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
            Field { name: \"zip\", data_type: UInt16, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }\
        ]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, \
        Field { name: \"interests\", data_type: Dictionary(Int32, Utf8), nullable: true, dict_id: 123, dict_is_ordered: true, metadata: None }")
    }

    #[test]
    fn schema_field_accessors() {
        let schema = person_schema();

        // test schema accessors
        assert_eq!(schema.fields().len(), 4);

        // test field accessors
        let first_name = &schema.fields()[0];
        assert_eq!(first_name.name(), "first_name");
        assert_eq!(first_name.data_type(), &DataType::Utf8);
        assert_eq!(first_name.is_nullable(), false);
        assert_eq!(first_name.dict_id(), None);
        assert_eq!(first_name.dict_is_ordered(), None);

        let metadata = first_name.metadata();
        assert!(metadata.is_some());
        let md = metadata.as_ref().unwrap();
        assert_eq!(md.len(), 1);
        let key = md.get("k");
        assert!(key.is_some());
        assert_eq!(key.unwrap(), "v");

        let interests = &schema.fields()[3];
        assert_eq!(interests.name(), "interests");
        assert_eq!(
            interests.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(interests.dict_id(), Some(123));
        assert_eq!(interests.dict_is_ordered(), Some(true));
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_index_of() {
        let schema = person_schema();
        assert_eq!(schema.index_of("first_name").unwrap(), 0);
        assert_eq!(schema.index_of("last_name").unwrap(), 1);
        schema.index_of("nickname").unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Unable to get field named \\\"nickname\\\". Valid fields: [\\\"first_name\\\", \\\"last_name\\\", \\\"address\\\", \\\"interests\\\"]"
    )]
    fn schema_field_with_name() {
        let schema = person_schema();
        assert_eq!(
            schema.field_with_name("first_name").unwrap().name(),
            "first_name"
        );
        assert_eq!(
            schema.field_with_name("last_name").unwrap().name(),
            "last_name"
        );
        schema.field_with_name("nickname").unwrap();
    }

    #[test]
    fn schema_field_with_dict_id() {
        let schema = person_schema();

        let fields_dict_123: Vec<_> = schema
            .fields_with_dict_id(123)
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(fields_dict_123, vec!["interests"]);

        assert!(schema.fields_with_dict_id(456).is_empty());
    }

    #[test]
    fn schema_equality() {
        let schema1 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        let schema2 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);

        assert_eq!(schema1, schema2);

        let schema3 = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float32, true),
        ]);
        let schema4 = Schema::new(vec![
            Field::new("C1", DataType::Utf8, false),
            Field::new("C2", DataType::Float64, true),
        ]);

        assert!(schema1 != schema3);
        assert!(schema1 != schema4);
        assert!(schema2 != schema3);
        assert!(schema2 != schema4);
        assert!(schema3 != schema4);

        let mut f = Field::new("c1", DataType::Utf8, false);
        f.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let schema5 = Schema::new(vec![
            f,
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::LargeBinary, true),
        ]);
        assert!(schema1 != schema5);
    }

    #[test]
    fn test_arrow_native_type_to_json() {
        assert_eq!(Some(Bool(true)), true.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1i64.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u8.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u16.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u32.into_json_value());
        assert_eq!(Some(VNumber(Number::from(1))), 1u64.into_json_value());
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01.into_json_value()
        );
        assert_eq!(
            Some(VNumber(Number::from_f64(0.01f64).unwrap())),
            0.01f64.into_json_value()
        );
        assert_eq!(None, NAN.into_json_value());
    }

    fn person_schema() -> Schema {
        let kv_array = [("k".to_string(), "v".to_string())];
        let field_metadata: BTreeMap<String, String> = kv_array.iter().cloned().collect();
        let mut first_name = Field::new("first_name", DataType::Utf8, false);
        first_name.set_metadata(Some(field_metadata));

        Schema::new(vec![
            first_name,
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
            Field::new_dict(
                "interests",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                123,
                true,
            ),
        ])
    }

    #[test]
    fn test_try_merge_field_with_metadata() {
        // 1. Different values for the same key should cause error.
        let metadata1: BTreeMap<String, String> =
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(metadata1));

        let metadata2: BTreeMap<String, String> =
            [("foo".to_string(), "baz".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(metadata2));

        assert!(
            Schema::try_merge(&[Schema::new(vec![f1]), Schema::new(vec![f2])]).is_err()
        );

        // 2. None + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let metadata2: BTreeMap<String, String> =
            [("missing".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect();
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(metadata2));

        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata.is_some());
        assert_eq!(f1.metadata.unwrap(), f2.metadata.unwrap());

        // 3. Some + Some
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let mut f2 = Field::new("first_name", DataType::Utf8, false);
        f2.set_metadata(Some(
            [("foo2".to_string(), "bar2".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));

        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata.is_some());
        assert_eq!(
            f1.metadata.unwrap(),
            [
                ("foo".to_string(), "bar".to_string()),
                ("foo2".to_string(), "bar2".to_string())
            ]
            .iter()
            .cloned()
            .collect()
        );

        // 4. Some + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        f1.set_metadata(Some(
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata.is_some());
        assert_eq!(
            f1.metadata.unwrap(),
            [("foo".to_string(), "bar".to_string())]
                .iter()
                .cloned()
                .collect()
        );

        // 5. None + None.
        let mut f1 = Field::new("first_name", DataType::Utf8, false);
        let f2 = Field::new("first_name", DataType::Utf8, false);
        assert!(f1.try_merge(&f2).is_ok());
        assert!(f1.metadata.is_none());
    }

    #[test]
    fn test_schema_merge() -> Result<()> {
        let merged = Schema::try_merge(&[
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new(
                    "address",
                    DataType::Struct(vec![Field::new("zip", DataType::UInt16, false)]),
                    false,
                ),
            ]),
            Schema::new_with_metadata(
                vec![
                    // nullable merge
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            // add new nested field
                            Field::new("street", DataType::Utf8, false),
                            // nullable merge on nested field
                            Field::new("zip", DataType::UInt16, true),
                        ]),
                        false,
                    ),
                    // new field
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>(),
            ),
        ])?;

        assert_eq!(
            merged,
            Schema::new_with_metadata(
                vec![
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, true),
                    Field::new(
                        "address",
                        DataType::Struct(vec![
                            Field::new("zip", DataType::UInt16, true),
                            Field::new("street", DataType::Utf8, false),
                        ]),
                        false,
                    ),
                    Field::new("number", DataType::Utf8, true),
                ],
                [("foo".to_string(), "bar".to_string())]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            )
        );

        // support merge union fields
        assert_eq!(
            Schema::try_merge(&[
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(vec![
                        Field::new("c11", DataType::Utf8, true),
                        Field::new("c12", DataType::Utf8, true),
                    ]),
                    false
                ),]),
                Schema::new(vec![Field::new(
                    "c1",
                    DataType::Union(vec![
                        Field::new("c12", DataType::Utf8, true),
                        Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                    ]),
                    false
                ),])
            ])?,
            Schema::new(vec![Field::new(
                "c1",
                DataType::Union(vec![
                    Field::new("c11", DataType::Utf8, true),
                    Field::new("c12", DataType::Utf8, true),
                    Field::new("c13", DataType::Time64(TimeUnit::Second), true),
                ]),
                false
            ),]),
        );

        // incompatible field should throw error
        assert!(Schema::try_merge(&[
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
            ]),
            Schema::new(vec![Field::new("last_name", DataType::Int64, false),])
        ])
        .is_err());

        // incompatible metadata should throw error
        assert!(Schema::try_merge(&[
            Schema::new_with_metadata(
                vec![Field::new("first_name", DataType::Utf8, false)],
                [("foo".to_string(), "bar".to_string()),]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            ),
            Schema::new_with_metadata(
                vec![Field::new("last_name", DataType::Utf8, false)],
                [("foo".to_string(), "baz".to_string()),]
                    .iter()
                    .cloned()
                    .collect::<HashMap<String, String>>()
            )
        ])
        .is_err());

        Ok(())
    }
}

#[cfg(all(test, simd_x86))]
mod arrow_numeric_type_tests {
    use crate::datatypes::{
        ArrowNumericType, Float32Type, Float64Type, Int32Type, Int64Type, Int8Type,
        UInt16Type,
    };
    use packed_simd::*;
    use FromCast;

    /// calculate the expected mask by iterating over all bits
    macro_rules! expected_mask {
        ($T:ty, $MASK:expr) => {{
            let mask = $MASK;
            // simd width of all types is currently 64 bytes -> 512 bits
            let lanes = 64 / std::mem::size_of::<$T>();
            // translate each set bit into a value of all ones (-1) of the correct type
            (0..lanes)
                .map(|i| (if (mask & (1 << i)) != 0 { -1 } else { 0 }))
                .collect::<Vec<$T>>()
        }};
    }

    #[test]
    fn test_mask_f64() {
        let mask = 0b10101010;
        let actual = Float64Type::mask_from_u64(mask);
        let expected = expected_mask!(i64, mask);
        let expected = m64x8::from_cast(i64x8::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_u64() {
        let mask = 0b01010101;
        let actual = Int64Type::mask_from_u64(mask);
        let expected = expected_mask!(i64, mask);
        let expected = m64x8::from_cast(i64x8::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_f32() {
        let mask = 0b10101010_10101010;
        let actual = Float32Type::mask_from_u64(mask);
        let expected = expected_mask!(i32, mask);
        let expected =
            m32x16::from_cast(i32x16::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_i32() {
        let mask = 0b01010101_01010101;
        let actual = Int32Type::mask_from_u64(mask);
        let expected = expected_mask!(i32, mask);
        let expected =
            m32x16::from_cast(i32x16::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_u16() {
        let mask = 0b01010101_01010101_10101010_10101010;
        let actual = UInt16Type::mask_from_u64(mask);
        let expected = expected_mask!(i16, mask);
        dbg!(&expected);
        let expected =
            m16x32::from_cast(i16x32::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_mask_i8() {
        let mask =
            0b01010101_01010101_10101010_10101010_01010101_01010101_10101010_10101010;
        let actual = Int8Type::mask_from_u64(mask);
        let expected = expected_mask!(i8, mask);
        let expected = m8x64::from_cast(i8x64::from_slice_unaligned(expected.as_slice()));

        assert_eq!(expected, actual);
    }
}
