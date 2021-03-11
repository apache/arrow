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

use crate::datatypes::DataType;

/// A type that Rust's custom allocator knows how to allocate and deallocate.
/// This is implemented for all Arrow's physical types whose in-memory representation
/// matches Rust's physical types. Consider this trait sealed.
/// # Safety
/// Do not implement this trait.
pub unsafe trait NativeType:
    Sized + Copy + std::fmt::Debug + std::fmt::Display + PartialEq + Default + Sized + 'static
{
    type Bytes: AsRef<[u8]>;

    /// Whether a DataType is a valid type for this physical representation.
    fn is_valid(data_type: &DataType) -> bool;

    /// How this type represents itself as bytes in little endianess.
    /// This is used for IPC, where data is communicated with a specific endianess.
    fn to_le_bytes(&self) -> Self::Bytes;
}

unsafe impl NativeType for u8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt8
    }
}

unsafe impl NativeType for u16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt16
    }
}

unsafe impl NativeType for u32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt32
    }
}

unsafe impl NativeType for u64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::UInt64
    }
}

unsafe impl NativeType for i8 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int8
    }
}

unsafe impl NativeType for i16 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Int16
    }
}

unsafe impl NativeType for i32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int32 | DataType::Date32 | DataType::Time32(_)
        )
    }
}

unsafe impl NativeType for i64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        )
    }
}

unsafe impl NativeType for f32 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float32
    }
}

unsafe impl NativeType for f64 {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }

    #[inline]
    fn is_valid(data_type: &DataType) -> bool {
        data_type == &DataType::Float64
    }
}
