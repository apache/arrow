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

use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::DataType;
use crate::util::bit_util;
use crate::{bitmap::Bitmap, datatypes::ArrowNativeType};

use super::{equal::equal, OffsetSizeTrait};

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

    /// Computes the logical validity bitmap of the array data using the
    /// parent's array data. The parent should be a list or struct, else
    /// the logical bitmap of the array is returned unaltered.
    ///
    /// Parent data is passed along with the parent's logical bitmap, as
    /// nested arrays could have a logical bitmap different to the physical
    /// one on the `ArrayData`.
    ///
    /// Safety
    ///
    /// As we index into [`ArrayData::child_data`], this function panics if
    /// array data is not a nested type, as it will not have child data.
    pub fn child_logical_null_buffer(
        &self,
        logical_null_buffer: Option<Buffer>,
        child_index: usize,
    ) -> Option<Buffer> {
        // This function should only be called when having nested data types.
        // However, as a convenience, we return the parent's logical buffer if
        // we do not encounter a nested type.
        let child_data = self.child_data().get(child_index).unwrap();

        // TODO: rationalise this logic, prefer not creating populated bitmaps, use Option matching
        // I found taking a Bitmap that's populated to be more convenient, but I was more concerned first
        // about accuracy, so I'll explore using Option<&Bitmap> directly.
        let parent_bitmap = logical_null_buffer.map(Bitmap::from).unwrap_or_else(|| {
            let ceil = bit_util::ceil(self.len(), 8);
            Bitmap::from(Buffer::from(vec![0b11111111; ceil]))
        });
        let self_null_bitmap = child_data.null_bitmap().clone().unwrap_or_else(|| {
            let ceil = bit_util::ceil(child_data.len(), 8);
            Bitmap::from(Buffer::from(vec![0b11111111; ceil]))
        });
        match self.data_type() {
            DataType::List(_) => Some(logical_list_bitmap::<i32>(
                self,
                parent_bitmap,
                self_null_bitmap,
            )),
            DataType::LargeList(_) => Some(logical_list_bitmap::<i64>(
                self,
                parent_bitmap,
                self_null_bitmap,
            )),
            DataType::FixedSizeList(_, len) => {
                let len = *len as usize;
                let array_len = self.len();
                let array_offset = self.offset();
                let bitmap_len = bit_util::ceil(array_len * len, 8);
                let mut buffer =
                    MutableBuffer::new(bitmap_len).with_bitset(bitmap_len, false);
                let mut null_slice = buffer.data_mut();
                (array_offset..array_len + array_offset).for_each(|index| {
                    let start = index * len;
                    let end = start + len;
                    let mask = parent_bitmap.is_set(index);
                    (start..end).for_each(|child_index| {
                        if mask && self_null_bitmap.is_set(child_index) {
                            bit_util::set_bit(&mut null_slice, child_index);
                        }
                    });
                });
                Some(buffer.freeze())
            }
            DataType::Struct(_) => (&parent_bitmap & &self_null_bitmap)
                .ok()
                .map(|bitmap| bitmap.bits),
            DataType::Union(_) => {
                panic!("Logical equality not yet implemented for union arrays")
            }
            _ => {
                panic!(
                    "Encountered array data with child data, but not a known nested type"
                )
            }
        }
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

#[inline]
fn logical_list_bitmap<OffsetSize: OffsetSizeTrait>(
    parent_data: &ArrayData,
    parent_bitmap: Bitmap,
    child_bitmap: Bitmap,
) -> Buffer {
    let offsets = parent_data.buffer::<OffsetSize>(0);
    let offset_start = offsets.first().unwrap().to_usize().unwrap();
    let offset_len = offsets.get(parent_data.len()).unwrap().to_usize().unwrap();
    let mut buffer = MutableBuffer::new_null(offset_len - offset_start);
    let mut null_slice = buffer.data_mut();

    offsets
        .windows(2)
        .enumerate()
        .take(offset_len - offset_start)
        .for_each(|(index, window)| {
            dbg!(&window, index);
            let start = window[0].to_usize().unwrap();
            let end = window[1].to_usize().unwrap();
            let mask = parent_bitmap.is_set(index);
            (start..end).for_each(|child_index| {
                if mask && child_bitmap.is_set(child_index) {
                    bit_util::set_bit(&mut null_slice, child_index - offset_start);
                }
            });
        });
    buffer.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::datatypes::{Field, ToByteSlice};
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
            vec![Buffer::from([1i32, 2, 3, 4, 5].to_byte_slice())],
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

    #[test]
    fn test_logical_null_buffer() {
        let child_data = ArrayData::builder(DataType::Int32)
            .len(11)
            .add_buffer(Buffer::from(
                vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].to_byte_slice(),
            ))
            .build();

        let data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(7)
        .add_buffer(Buffer::from(vec![0, 0, 3, 5, 6, 9, 10, 11].to_byte_slice()))
        .null_bit_buffer(Buffer::from(vec![0b01011010]))
        .add_child_data(child_data.clone())
        .build();

        // Get the child logical null buffer. The child is non-nullable, but because the list has nulls,
        // we expect the child to logically have some nulls, inherited from the parent:
        // [1, 2, 3, null, null, 6, 7, 8, 9, null, 11]
        let nulls = data.child_logical_null_buffer(data.null_buffer().cloned(), 0);
        let expected = Some(Buffer::from(vec![0b11100111, 0b00000101]));
        assert_eq!(nulls, expected);

        // test with offset
        let data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
        .len(4)
        .offset(3)
        .add_buffer(Buffer::from(vec![0, 0, 3, 5, 6, 9, 10, 11].to_byte_slice()))
        // the null_bit_buffer doesn't have an offset, i.e. cleared the 3 offset bits 0b[---]01011[010]
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .add_child_data(child_data)
        .build();

        let nulls = data.child_logical_null_buffer(data.null_buffer().cloned(), 0);

        let expected = Some(Buffer::from(vec![0b00101111]));
        assert_eq!(nulls, expected);
    }
}
