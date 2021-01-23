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

//! Contains `ArrayData`, a generic representation of Arrow array data which encapsulates
//! common attributes and operations for Arrow array.

use std::mem;
use std::sync::Arc;

use crate::datatypes::{DataType, IntervalUnit};
use crate::{bitmap::Bitmap, datatypes::ArrowNativeType};
use crate::{
    buffer::{Buffer, MutableBuffer},
    util::bit_util,
};

use super::equal::equal;

#[inline]
pub(crate) fn count_nulls(
    null_bit_buffer: Option<&Buffer>,
    offset: usize,
    len: usize,
) -> usize {
    if let Some(buf) = null_bit_buffer {
        len.checked_sub(buf.count_set_bits_offset(offset, len))
            .unwrap()
    } else {
        0
    }
}

/// creates 2 [`MutableBuffer`]s with a given `capacity` (in slots).
#[inline]
pub(crate) fn new_buffers(data_type: &DataType, capacity: usize) -> [MutableBuffer; 2] {
    let empty_buffer = MutableBuffer::new(0);
    match data_type {
        DataType::Null => [empty_buffer, MutableBuffer::new(0)],
        DataType::Boolean => {
            let bytes = bit_util::ceil(capacity, 8);
            let buffer = MutableBuffer::new(bytes);
            [buffer, empty_buffer]
        }
        DataType::UInt8 => [
            MutableBuffer::new(capacity * mem::size_of::<u8>()),
            empty_buffer,
        ],
        DataType::UInt16 => [
            MutableBuffer::new(capacity * mem::size_of::<u16>()),
            empty_buffer,
        ],
        DataType::UInt32 => [
            MutableBuffer::new(capacity * mem::size_of::<u32>()),
            empty_buffer,
        ],
        DataType::UInt64 => [
            MutableBuffer::new(capacity * mem::size_of::<u64>()),
            empty_buffer,
        ],
        DataType::Int8 => [
            MutableBuffer::new(capacity * mem::size_of::<i8>()),
            empty_buffer,
        ],
        DataType::Int16 => [
            MutableBuffer::new(capacity * mem::size_of::<i16>()),
            empty_buffer,
        ],
        DataType::Int32 => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Int64 => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Float32 => [
            MutableBuffer::new(capacity * mem::size_of::<f32>()),
            empty_buffer,
        ],
        DataType::Float64 => [
            MutableBuffer::new(capacity * mem::size_of::<f64>()),
            empty_buffer,
        ],
        DataType::Date32(_) | DataType::Time32(_) => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Date64(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Timestamp(_, _) => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Interval(IntervalUnit::YearMonth) => [
            MutableBuffer::new(capacity * mem::size_of::<i32>()),
            empty_buffer,
        ],
        DataType::Interval(IntervalUnit::DayTime) => [
            MutableBuffer::new(capacity * mem::size_of::<i64>()),
            empty_buffer,
        ],
        DataType::Utf8 | DataType::Binary => {
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i32>());
            // safety: `unsafe` code assumes that this buffer is initialized with one element
            buffer.push(0i32);
            [buffer, MutableBuffer::new(capacity * mem::size_of::<u8>())]
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i64>());
            // safety: `unsafe` code assumes that this buffer is initialized with one element
            buffer.push(0i64);
            [buffer, MutableBuffer::new(capacity * mem::size_of::<u8>())]
        }
        DataType::List(_) => {
            // offset buffer always starts with a zero
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i32>());
            buffer.push(0i32);
            [buffer, empty_buffer]
        }
        DataType::LargeList(_) => {
            // offset buffer always starts with a zero
            let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<i64>());
            buffer.push(0i64);
            [buffer, empty_buffer]
        }
        DataType::FixedSizeBinary(size) => {
            [MutableBuffer::new(capacity * *size as usize), empty_buffer]
        }
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => [
                MutableBuffer::new(capacity * mem::size_of::<u8>()),
                empty_buffer,
            ],
            DataType::UInt16 => [
                MutableBuffer::new(capacity * mem::size_of::<u16>()),
                empty_buffer,
            ],
            DataType::UInt32 => [
                MutableBuffer::new(capacity * mem::size_of::<u32>()),
                empty_buffer,
            ],
            DataType::UInt64 => [
                MutableBuffer::new(capacity * mem::size_of::<u64>()),
                empty_buffer,
            ],
            DataType::Int8 => [
                MutableBuffer::new(capacity * mem::size_of::<i8>()),
                empty_buffer,
            ],
            DataType::Int16 => [
                MutableBuffer::new(capacity * mem::size_of::<i16>()),
                empty_buffer,
            ],
            DataType::Int32 => [
                MutableBuffer::new(capacity * mem::size_of::<i32>()),
                empty_buffer,
            ],
            DataType::Int64 => [
                MutableBuffer::new(capacity * mem::size_of::<i64>()),
                empty_buffer,
            ],
            _ => unreachable!(),
        },
        DataType::Float16 => unreachable!(),
        DataType::FixedSizeList(_, _) | DataType::Struct(_) => {
            [empty_buffer, MutableBuffer::new(0)]
        }
        DataType::Decimal(_, _) => [
            MutableBuffer::new(capacity * mem::size_of::<u8>()),
            empty_buffer,
        ],
        DataType::Union(_) => unimplemented!(),
    }
}

/// Maps 2 [`MutableBuffer`]s into a vector of [Buffer]s whose size depends on `data_type`.
#[inline]
pub(crate) fn into_buffers(
    data_type: &DataType,
    buffer1: MutableBuffer,
    buffer2: MutableBuffer,
) -> Vec<Buffer> {
    match data_type {
        DataType::Null | DataType::Struct(_) => vec![],
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => vec![buffer1.into(), buffer2.into()],
        _ => vec![buffer1.into()],
    }
}

/// An generic representation of Arrow array data which encapsulates common attributes and
/// operations for Arrow array. Specific operations for different arrays types (e.g.,
/// primitive, list, struct) are implemented in `Array`.
#[derive(Debug, Clone)]
pub struct ArrayData {
    /// The data type for this array data
    data_type: DataType,

    /// The number of elements in this array data
    len: usize,

    /// The number of null elements in this array data
    null_count: usize,

    /// The offset into this array data, in number of items
    offset: usize,

    /// The buffers for this array data. Note that depending on the array types, this
    /// could hold different kinds of buffers (e.g., value buffer, value offset buffer)
    /// at different positions.
    buffers: Vec<Buffer>,

    /// The child(ren) of this array. Only non-empty for nested types, currently
    /// `ListArray` and `StructArray`.
    child_data: Vec<ArrayDataRef>,

    /// The null bitmap. A `None` value for this indicates all values are non-null in
    /// this array.
    null_bitmap: Option<Bitmap>,
}

pub type ArrayDataRef = Arc<ArrayData>;

impl ArrayData {
    pub fn new(
        data_type: DataType,
        len: usize,
        null_count: Option<usize>,
        null_bit_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrayDataRef>,
    ) -> Self {
        let null_count = match null_count {
            None => count_nulls(null_bit_buffer.as_ref(), offset, len),
            Some(null_count) => null_count,
        };
        let null_bitmap = null_bit_buffer.map(Bitmap::from);
        Self {
            data_type,
            len,
            null_count,
            offset,
            buffers,
            child_data,
            null_bitmap,
        }
    }

    /// Returns a builder to construct a `ArrayData` instance.
    #[inline]
    pub const fn builder(data_type: DataType) -> ArrayDataBuilder {
        ArrayDataBuilder::new(data_type)
    }

    /// Returns a reference to the data type of this array data
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns a slice of buffers for this array data
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers[..]
    }

    /// Returns a slice of children data arrays
    pub fn child_data(&self) -> &[ArrayDataRef] {
        &self.child_data[..]
    }

    /// Returns whether the element at index `i` is null
    pub fn is_null(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return !b.is_set(self.offset + i);
        }
        false
    }

    /// Returns a reference to the null bitmap of this array data
    #[inline]
    pub const fn null_bitmap(&self) -> &Option<Bitmap> {
        &self.null_bitmap
    }

    /// Returns a reference to the null buffer of this array data.
    pub fn null_buffer(&self) -> Option<&Buffer> {
        self.null_bitmap().as_ref().map(|b| b.buffer_ref())
    }

    /// Returns whether the element at index `i` is not null
    pub fn is_valid(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return b.is_set(self.offset + i);
        }
        true
    }

    /// Returns the length (i.e., number of elements) of this array
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    // Returns whether array data is empty
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the offset of this array
    #[inline]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the total number of nulls in this array
    #[inline]
    pub const fn null_count(&self) -> usize {
        self.null_count
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [ArrayData].
    pub fn get_buffer_memory_size(&self) -> usize {
        let mut size = 0;
        for buffer in &self.buffers {
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            size += bitmap.get_buffer_memory_size()
        }
        for child in &self.child_data {
            size += child.get_buffer_memory_size();
        }
        size
    }

    /// Returns the total number of bytes of memory occupied physically by this [ArrayData].
    pub fn get_array_memory_size(&self) -> usize {
        let mut size = 0;
        // Calculate size of the fields that don't have [get_array_memory_size] method internally.
        size += mem::size_of_val(self)
            - mem::size_of_val(&self.buffers)
            - mem::size_of_val(&self.null_bitmap)
            - mem::size_of_val(&self.child_data);

        // Calculate rest of the fields top down which contain actual data
        for buffer in &self.buffers {
            size += mem::size_of_val(&buffer);
            size += buffer.capacity();
        }
        if let Some(bitmap) = &self.null_bitmap {
            size += bitmap.get_array_memory_size()
        }
        for child in &self.child_data {
            size += child.get_array_memory_size();
        }

        size
    }

    /// Creates a zero-copy slice of itself. This creates a new [ArrayData]
    /// with a different offset, len and a shifted null bitmap.
    ///
    /// # Panics
    ///
    /// Panics if `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> ArrayData {
        assert!((offset + length) <= self.len());

        let mut new_data = self.clone();

        new_data.len = length;
        new_data.offset = offset + self.offset;

        new_data.null_count =
            count_nulls(new_data.null_buffer(), new_data.offset, new_data.len);

        new_data
    }

    /// Returns the `buffer` as a slice of type `T` starting at self.offset
    /// # Panics
    /// This function panics if:
    /// * the buffer is not byte-aligned with type T, or
    /// * the datatype is `Boolean` (it corresponds to a bit-packed buffer where the offset is not applicable)
    #[inline]
    pub(super) fn buffer<T: ArrowNativeType>(&self, buffer: usize) -> &[T] {
        let values = unsafe { self.buffers[buffer].as_slice().align_to::<T>() };
        if !values.0.is_empty() || !values.2.is_empty() {
            panic!("The buffer is not byte-aligned with its interpretation")
        };
        assert_ne!(self.data_type, DataType::Boolean);
        &values.1[self.offset..]
    }

    /// Returns a new empty [ArrayData] valid for `data_type`.
    pub(super) fn new_empty(data_type: &DataType) -> Self {
        let buffers = new_buffers(data_type, 0);
        let [buffer1, buffer2] = buffers;
        let buffers = into_buffers(data_type, buffer1, buffer2);

        let child_data = match data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal(_, _) => vec![],
            DataType::List(field) => {
                vec![Arc::new(Self::new_empty(field.data_type()))]
            }
            DataType::FixedSizeList(field, _) => {
                vec![Arc::new(Self::new_empty(field.data_type()))]
            }
            DataType::LargeList(field) => {
                vec![Arc::new(Self::new_empty(field.data_type()))]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| Arc::new(Self::new_empty(field.data_type())))
                .collect(),
            DataType::Union(_) => unimplemented!(),
            DataType::Dictionary(_, data_type) => {
                vec![Arc::new(Self::new_empty(data_type))]
            }
            DataType::Float16 => unreachable!(),
        };

        Self::new(data_type.clone(), 0, Some(0), None, 0, buffers, child_data)
    }
}

impl PartialEq for ArrayData {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

/// Builder for `ArrayData` type
#[derive(Debug)]
pub struct ArrayDataBuilder {
    data_type: DataType,
    len: usize,
    null_count: Option<usize>,
    null_bit_buffer: Option<Buffer>,
    offset: usize,
    buffers: Vec<Buffer>,
    child_data: Vec<ArrayDataRef>,
}

impl ArrayDataBuilder {
    #[inline]
    pub const fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            len: 0,
            null_count: None,
            null_bit_buffer: None,
            offset: 0,
            buffers: vec![],
            child_data: vec![],
        }
    }

    #[inline]
    pub const fn len(mut self, n: usize) -> Self {
        self.len = n;
        self
    }

    pub fn null_bit_buffer(mut self, buf: Buffer) -> Self {
        self.null_bit_buffer = Some(buf);
        self
    }

    #[inline]
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    pub fn buffers(mut self, v: Vec<Buffer>) -> Self {
        self.buffers = v;
        self
    }

    pub fn add_buffer(mut self, b: Buffer) -> Self {
        self.buffers.push(b);
        self
    }

    pub fn child_data(mut self, v: Vec<ArrayDataRef>) -> Self {
        self.child_data = v;
        self
    }

    pub fn add_child_data(mut self, r: ArrayDataRef) -> Self {
        self.child_data.push(r);
        self
    }

    pub fn build(self) -> ArrayDataRef {
        let data = ArrayData::new(
            self.data_type,
            self.len,
            self.null_count,
            self.null_bit_buffer,
            self.offset,
            self.buffers,
            self.child_data,
        );
        Arc::new(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::buffer::Buffer;
    use crate::util::bit_util;

    #[test]
    fn test_new() {
        let arr_data =
            ArrayData::new(DataType::Boolean, 10, Some(1), None, 2, vec![], vec![]);
        assert_eq!(10, arr_data.len());
        assert_eq!(1, arr_data.null_count());
        assert_eq!(2, arr_data.offset());
        assert_eq!(0, arr_data.buffers().len());
        assert_eq!(0, arr_data.child_data().len());
    }

    #[test]
    fn test_builder() {
        let child_arr_data = Arc::new(ArrayData::new(
            DataType::Int32,
            5,
            Some(0),
            None,
            0,
            vec![Buffer::from_slice_ref(&[1i32, 2, 3, 4, 5])],
            vec![],
        ));
        let v = vec![0, 1, 2, 3];
        let b1 = Buffer::from(&v[..]);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(20)
            .offset(5)
            .add_buffer(b1)
            .null_bit_buffer(Buffer::from(vec![
                0b01011111, 0b10110101, 0b01100011, 0b00011110,
            ]))
            .add_child_data(child_arr_data.clone())
            .build();

        assert_eq!(20, arr_data.len());
        assert_eq!(10, arr_data.null_count());
        assert_eq!(5, arr_data.offset());
        assert_eq!(1, arr_data.buffers().len());
        assert_eq!(&[0, 1, 2, 3], arr_data.buffers()[0].as_slice());
        assert_eq!(1, arr_data.child_data().len());
        assert_eq!(child_arr_data, arr_data.child_data()[0]);
    }

    #[test]
    fn test_null_count() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert_eq!(13, arr_data.null_count());

        // Test with offset
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(12)
            .offset(2)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert_eq!(10, arr_data.null_count());
    }

    #[test]
    fn test_null_buffer_ref() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let arr_data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        assert!(arr_data.null_buffer().is_some());
        assert_eq!(&bit_v, arr_data.null_buffer().unwrap().as_slice());
    }

    #[test]
    fn test_slice() {
        let mut bit_v: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut bit_v, 0);
        bit_util::set_bit(&mut bit_v, 3);
        bit_util::set_bit(&mut bit_v, 10);
        let data = ArrayData::builder(DataType::Int32)
            .len(16)
            .null_bit_buffer(Buffer::from(bit_v))
            .build();
        let data = data.as_ref();
        let new_data = data.slice(1, 15);
        assert_eq!(data.len() - 1, new_data.len());
        assert_eq!(1, new_data.offset());
        assert_eq!(data.null_count(), new_data.null_count());

        // slice of a slice (removes one null)
        let new_data = new_data.slice(1, 14);
        assert_eq!(data.len() - 2, new_data.len());
        assert_eq!(2, new_data.offset());
        assert_eq!(data.null_count() - 1, new_data.null_count());
    }

    #[test]
    fn test_equality() {
        let int_data = ArrayData::builder(DataType::Int32).build();
        let float_data = ArrayData::builder(DataType::Float32).build();
        assert_ne!(int_data, float_data);
    }

    #[test]
    fn test_count_nulls() {
        let null_buffer = Some(Buffer::from(vec![0b00010110, 0b10011111]));
        let count = count_nulls(null_buffer.as_ref(), 0, 16);
        assert_eq!(count, 7);

        let count = count_nulls(null_buffer.as_ref(), 4, 8);
        assert_eq!(count, 3);
    }
}
