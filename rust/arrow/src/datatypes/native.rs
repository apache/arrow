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

use serde_json::{Number, Value};

use super::DataType;

/// Trait declaring any type that is serializable to JSON. This includes all primitive types (bool, i32, etc.).
pub trait JsonSerializable: 'static {
    fn into_json_value(self) -> Option<Value>;
}

/// Trait expressing a Rust type that has the same in-memory representation
/// as Arrow. This includes `i16`, `f32`, but excludes `bool` (which in arrow is represented in bits).
/// In little endian machines, types that implement [`ArrowNativeType`] can be memcopied to arrow buffers
/// as is.
pub trait ArrowNativeType:
    std::fmt::Debug
    + Send
    + Sync
    + Copy
    + PartialOrd
    + std::str::FromStr
    + Default
    + JsonSerializable
{
    /// Convert native type from usize.
    #[inline]
    fn from_usize(_: usize) -> Option<Self> {
        None
    }

    /// Convert native type to usize.
    #[inline]
    fn to_usize(&self) -> Option<usize> {
        None
    }

    /// Convert native type to isize.
    #[inline]
    fn to_isize(&self) -> Option<isize> {
        None
    }

    /// Convert native type from i32.
    #[inline]
    fn from_i32(_: i32) -> Option<Self> {
        None
    }

    /// Convert native type from i64.
    #[inline]
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
        std::mem::size_of::<Self::Native>()
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
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for i16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for i16 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for i32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for i32 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }

    /// Convert native type from i32.
    #[inline]
    fn from_i32(val: i32) -> Option<Self> {
        Some(val)
    }
}

impl JsonSerializable for i64 {
    fn into_json_value(self) -> Option<Value> {
        Some(Value::Number(Number::from(self)))
    }
}

impl ArrowNativeType for i64 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }

    /// Convert native type from i64.
    #[inline]
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
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for u16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u16 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for u32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u32 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for u64 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl ArrowNativeType for u64 {
    #[inline]
    fn from_usize(v: usize) -> Option<Self> {
        num::FromPrimitive::from_usize(v)
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        num::ToPrimitive::to_usize(self)
    }

    #[inline]
    fn to_isize(&self) -> Option<isize> {
        num::ToPrimitive::to_isize(self)
    }
}

impl JsonSerializable for f32 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(self as f64 * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f64 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(self).map(Value::Number)
    }
}

impl ArrowNativeType for f32 {}
impl ArrowNativeType for f64 {}

/// Allows conversion from supported Arrow types to a byte slice.
pub trait ToByteSlice {
    /// Converts this instance into a byte slice
    fn to_byte_slice(&self) -> &[u8];
}

impl<T: ArrowNativeType> ToByteSlice for [T] {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const T as *const u8;
        unsafe {
            std::slice::from_raw_parts(raw_ptr, self.len() * std::mem::size_of::<T>())
        }
    }
}

impl<T: ArrowNativeType> ToByteSlice for T {
    #[inline]
    fn to_byte_slice(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of::<T>()) }
    }
}
