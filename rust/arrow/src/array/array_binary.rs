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

use std::fmt;
use std::mem;
use std::{any::Any, iter::FromIterator};
use std::{
    convert::{From, TryInto},
    sync::Arc,
};

use super::{
    array::print_long_array, raw_pointer::as_aligned_pointer, raw_pointer::RawPtrBox,
    Array, ArrayData, ArrayDataRef, FixedSizeListArray, GenericBinaryIter,
    GenericListArray, LargeListArray, ListArray, OffsetSizeTrait,
};
use crate::util::bit_util;
use crate::{buffer::Buffer, datatypes::ToByteSlice};
use crate::{buffer::MutableBuffer, datatypes::DataType};

/// Like OffsetSizeTrait, but specialized for Binary
// This allow us to expose a constant datatype for the GenericBinaryArray
pub trait BinaryOffsetSizeTrait: OffsetSizeTrait {
    const DATA_TYPE: DataType;
}

impl BinaryOffsetSizeTrait for i32 {
    const DATA_TYPE: DataType = DataType::Binary;
}

impl BinaryOffsetSizeTrait for i64 {
    const DATA_TYPE: DataType = DataType::LargeBinary;
}

pub struct GenericBinaryArray<OffsetSize: BinaryOffsetSizeTrait> {
    data: ArrayDataRef,
    value_offsets: RawPtrBox<OffsetSize>,
    value_data: RawPtrBox<u8>,
}

impl<OffsetSize: BinaryOffsetSizeTrait> GenericBinaryArray<OffsetSize> {
    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> OffsetSize {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_length(&self, mut i: usize) -> OffsetSize {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    /// Returns a clone of the value offset buffer
    pub fn value_offsets(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[1].clone()
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> OffsetSize {
        unsafe { *self.value_offsets.get().add(i) }
    }

    /// Returns the element at index `i` as a byte slice.
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.data.len(), "BinaryArray out of bounds access");
        let offset = i.checked_add(self.data.offset()).unwrap();
        unsafe {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.value_data.get().offset(pos.to_isize()),
                (self.value_offset_at(offset + 1) - pos).to_usize().unwrap(),
            )
        }
    }

    /// Creates a [GenericBinaryArray] from a vector of byte slices
    pub fn from_vec(v: Vec<&[u8]>) -> Self {
        let mut offsets = Vec::with_capacity(v.len() + 1);
        let mut values = Vec::new();
        let mut length_so_far: OffsetSize = OffsetSize::zero();
        offsets.push(length_so_far);
        for s in &v {
            length_so_far = length_so_far + OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s);
        }
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        GenericBinaryArray::<OffsetSize>::from(array_data)
    }

    /// Creates a [GenericBinaryArray] from a vector of Optional (null) byte slices
    pub fn from_opt_vec(v: Vec<Option<&[u8]>>) -> Self {
        v.into_iter().collect()
    }

    fn from_list(v: GenericListArray<OffsetSize>) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "BinaryArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "BinaryArray can only be created from List<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(v.data_ref().buffers()[0].clone())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder
                .null_count(v.data_ref().null_count())
                .null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }
}

impl<'a, T: BinaryOffsetSizeTrait> GenericBinaryArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> GenericBinaryIter<'a, T> {
        GenericBinaryIter::<'a, T>::new(&self)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> fmt::Debug for GenericBinaryArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}BinaryArray\n[\n", OffsetSize::prefix())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> Array for GenericBinaryArray<OffsetSize> {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [$name].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [$name].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> From<ArrayDataRef>
    for GenericBinaryArray<OffsetSize>
{
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.data_type(),
            &<OffsetSize as BinaryOffsetSizeTrait>::DATA_TYPE,
            "[Large]BinaryArray expects Datatype::[Large]Binary"
        );
        assert_eq!(
            data.buffers().len(),
            2,
            "BinaryArray data should contain 2 buffers only (offsets and values)"
        );
        let raw_value_offsets = data.buffers()[0].raw_data();
        let value_data = data.buffers()[1].raw_data();
        Self {
            data,
            value_offsets: RawPtrBox::new(as_aligned_pointer::<OffsetSize>(
                raw_value_offsets,
            )),
            value_data: RawPtrBox::new(value_data),
        }
    }
}

impl<Ptr, OffsetSize: BinaryOffsetSizeTrait> FromIterator<Option<Ptr>>
    for GenericBinaryArray<OffsetSize>
where
    Ptr: AsRef<[u8]>,
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets = Vec::with_capacity(data_len + 1);
        let mut values = Vec::new();
        let mut null_buf = MutableBuffer::new_null(data_len);
        let mut length_so_far: OffsetSize = OffsetSize::zero();
        offsets.push(length_so_far);

        {
            let null_slice = null_buf.data_mut();

            for (i, s) in iter.enumerate() {
                if let Some(s) = s {
                    let s = s.as_ref();
                    bit_util::set_bit(null_slice, i);
                    length_so_far =
                        length_so_far + OffsetSize::from_usize(s.len()).unwrap();
                    values.extend_from_slice(s);
                }
                // always add an element in offsets
                offsets.push(length_so_far);
            }
        }

        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(data_len)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .null_bit_buffer(null_buf.freeze())
            .build();
        Self::from(array_data)
    }
}

/// An array where each element is a byte whose maximum length is represented by a i32.
pub type BinaryArray = GenericBinaryArray<i32>;

/// An array where each element is a byte whose maximum length is represented by a i64.
pub type LargeBinaryArray = GenericBinaryArray<i64>;

impl<'a, T: BinaryOffsetSizeTrait> IntoIterator for &'a GenericBinaryArray<T> {
    type Item = Option<&'a [u8]>;
    type IntoIter = GenericBinaryIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GenericBinaryIter::<'a, T>::new(self)
    }
}

impl From<Vec<&[u8]>> for BinaryArray {
    fn from(v: Vec<&[u8]>) -> Self {
        BinaryArray::from_vec(v)
    }
}

impl From<Vec<Option<&[u8]>>> for BinaryArray {
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        BinaryArray::from_opt_vec(v)
    }
}

impl From<Vec<&[u8]>> for LargeBinaryArray {
    fn from(v: Vec<&[u8]>) -> Self {
        LargeBinaryArray::from_vec(v)
    }
}

impl From<Vec<Option<&[u8]>>> for LargeBinaryArray {
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        LargeBinaryArray::from_opt_vec(v)
    }
}

impl From<ListArray> for BinaryArray {
    fn from(v: ListArray) -> Self {
        BinaryArray::from_list(v)
    }
}

impl From<LargeListArray> for LargeBinaryArray {
    fn from(v: LargeListArray) -> Self {
        LargeBinaryArray::from_list(v)
    }
}

/// A type of `FixedSizeListArray` whose elements are binaries.
pub struct FixedSizeBinaryArray {
    data: ArrayDataRef,
    value_data: RawPtrBox<u8>,
    length: i32,
}

impl FixedSizeBinaryArray {
    /// Returns the element at index `i` as a byte slice.
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(
            i < self.data.len(),
            "FixedSizeBinaryArray out of bounds access"
        );
        let offset = i.checked_add(self.data.offset()).unwrap();
        unsafe {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.value_data.get().offset(pos as isize),
                (self.value_offset_at(offset + 1) - pos) as usize,
            )
        }
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        self.length
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.length * i as i32
    }
}

impl From<Vec<Vec<u8>>> for FixedSizeBinaryArray {
    fn from(data: Vec<Vec<u8>>) -> Self {
        let len = data.len();
        assert!(len > 0);
        let size = data[0].len();
        assert!(data.iter().all(|item| item.len() == size));
        let data = data.into_iter().flatten().collect::<Vec<_>>();
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(size as i32))
            .len(len)
            .add_buffer(Buffer::from(&data))
            .build();
        FixedSizeBinaryArray::from(array_data)
    }
}

impl From<Vec<Option<Vec<u8>>>> for FixedSizeBinaryArray {
    fn from(data: Vec<Option<Vec<u8>>>) -> Self {
        let len = data.len();
        assert!(len > 0);
        // try to estimate the size. This may not be possible no entry is valid => panic
        let size = data.iter().filter_map(|e| e.as_ref()).next().unwrap().len();
        assert!(data
            .iter()
            .filter_map(|e| e.as_ref())
            .all(|item| item.len() == size));

        let num_bytes = bit_util::ceil(len, 8);
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
        let null_slice = null_buf.data_mut();

        data.iter().enumerate().for_each(|(i, entry)| {
            if entry.is_some() {
                bit_util::set_bit(null_slice, i);
            }
        });

        let data = data
            .into_iter()
            .flat_map(|e| e.unwrap_or_else(|| vec![0; size]))
            .collect::<Vec<_>>();
        let data = ArrayData::new(
            DataType::FixedSizeBinary(size as i32),
            len,
            None,
            Some(null_buf.freeze()),
            0,
            vec![Buffer::from(&data)],
            vec![],
        );
        FixedSizeBinaryArray::from(Arc::new(data))
    }
}

impl From<ArrayDataRef> for FixedSizeBinaryArray {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "FixedSizeBinaryArray data should contain 1 buffer only (values)"
        );
        let value_data = data.buffers()[0].raw_data();
        let length = match data.data_type() {
            DataType::FixedSizeBinary(len) => *len,
            _ => panic!("Expected data type to be FixedSizeBinary"),
        };
        Self {
            data,
            value_data: RawPtrBox::new(value_data),
            length,
        }
    }
}

/// Creates a `FixedSizeBinaryArray` from `FixedSizeList<u8>` array
impl From<FixedSizeListArray> for FixedSizeBinaryArray {
    fn from(v: FixedSizeListArray) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "FixedSizeBinaryArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "FixedSizeBinaryArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(DataType::FixedSizeBinary(v.value_length()))
            .len(v.len())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder
                .null_count(v.data_ref().null_count())
                .null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }
}

impl fmt::Debug for FixedSizeBinaryArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FixedSizeBinaryArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl Array for FixedSizeBinaryArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [FixedSizeBinaryArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [FixedSizeBinaryArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

/// A type of `DecimalArray` whose elements are binaries.
pub struct DecimalArray {
    data: ArrayDataRef,
    value_data: RawPtrBox<u8>,
    precision: usize,
    scale: usize,
    length: i32,
}

impl DecimalArray {
    /// Returns the element at index `i` as i128.
    pub fn value(&self, i: usize) -> i128 {
        assert!(i < self.data.len(), "DecimalArray out of bounds access");
        let offset = i.checked_add(self.data.offset()).unwrap();
        let raw_val = unsafe {
            let pos = self.value_offset_at(offset);
            std::slice::from_raw_parts(
                self.value_data.get().offset(pos as isize),
                (self.value_offset_at(offset + 1) - pos) as usize,
            )
        };
        let as_array = raw_val.try_into();
        match as_array {
            Ok(v) if raw_val.len() == 16 => i128::from_le_bytes(v),
            _ => panic!("DecimalArray elements are not 128bit integers."),
        }
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub fn value_length(&self) -> i32 {
        self.length
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.length * i as i32
    }

    pub fn from_fixed_size_list_array(
        v: FixedSizeListArray,
        precision: usize,
        scale: usize,
    ) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "DecimalArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "DecimalArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(DataType::Decimal(precision, scale))
            .len(v.len())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder
                .null_count(v.data_ref().null_count())
                .null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }
}

impl From<ArrayDataRef> for DecimalArray {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DecimalArray data should contain 1 buffer only (values)"
        );
        let value_data = data.buffers()[0].raw_data();
        let (precision, scale) = match data.data_type() {
            DataType::Decimal(precision, scale) => (*precision, *scale),
            _ => panic!("Expected data type to be Decimal"),
        };
        let length = 16;
        Self {
            data,
            value_data: RawPtrBox::new(value_data),
            precision,
            scale,
            length,
        }
    }
}

impl fmt::Debug for DecimalArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DecimalArray<{}, {}>\n[\n", self.precision, self.scale)?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl Array for DecimalArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [DecimalArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [DecimalArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::Field;

    use super::*;

    #[test]
    fn test_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::Binary)
            .len(4)
            .offset(1)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!(5, binary_array.value_offset(0));
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offset(1));
        assert_eq!(7, binary_array.value_length(1));
    }

    #[test]
    fn test_large_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i64; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::LargeBinary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::LargeBinary)
            .len(4)
            .offset(1)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!(5, binary_array.value_offset(0));
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offset(1));
        assert_eq!(7, binary_array.value_length(1));
    }

    #[test]
    fn test_binary_array_from_list_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data1 = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array1 = BinaryArray::from(array_data1);

        let array_data2 = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_child_data(values_data)
            .build();
        let list_array = ListArray::from(array_data2);
        let binary_array2 = BinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value_offset(i), binary_array2.value_offset(i));
            assert_eq!(binary_array1.value_length(i), binary_array2.value_length(i));
        }
    }

    #[test]
    fn test_large_binary_array_from_list_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let values_data = ArrayData::builder(DataType::UInt8)
            .len(12)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let offsets: [i64; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data1 = ArrayData::builder(DataType::LargeBinary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array1 = LargeBinaryArray::from(array_data1);

        let array_data2 = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_child_data(values_data)
            .build();
        let list_array = LargeListArray::from(array_data2);
        let binary_array2 = LargeBinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value_offset(i), binary_array2.value_offset(i));
            assert_eq!(binary_array1.value_length(i), binary_array2.value_length(i));
        }
    }

    fn test_generic_binary_array_from_opt_vec<T: BinaryOffsetSizeTrait>() {
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let array = GenericBinaryArray::<T>::from_opt_vec(values);
        assert_eq!(array.len(), 5);
        assert_eq!(array.value(0), b"one");
        assert_eq!(array.value(1), b"two");
        assert_eq!(array.value(3), b"");
        assert_eq!(array.value(4), b"three");
        assert_eq!(array.is_null(0), false);
        assert_eq!(array.is_null(1), false);
        assert_eq!(array.is_null(2), true);
        assert_eq!(array.is_null(3), false);
        assert_eq!(array.is_null(4), false);
    }

    #[test]
    fn test_large_binary_array_from_opt_vec() {
        test_generic_binary_array_from_opt_vec::<i64>()
    }

    #[test]
    fn test_binary_array_from_opt_vec() {
        test_generic_binary_array_from_opt_vec::<i32>()
    }

    #[test]
    #[should_panic(
        expected = "BinaryArray can only be created from List<u8> arrays, mismatched \
                    data types."
    )]
    fn test_binary_array_from_incorrect_list_array_type() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from(values[..].to_byte_slice()))
            .build();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_child_data(values_data)
            .build();
        let list_array = ListArray::from(array_data);
        BinaryArray::from(list_array);
    }

    #[test]
    #[should_panic(
        expected = "BinaryArray can only be created from list array of u8 values \
                    (i.e. List<PrimitiveArray<u8>>)."
    )]
    fn test_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from(values[..].to_byte_slice()))
            .add_child_data(ArrayData::builder(DataType::Boolean).build())
            .build();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_child_data(values_data)
            .build();
        let list_array = ListArray::from(array_data);
        BinaryArray::from(list_array);
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let fixed_size_binary_array = FixedSizeBinaryArray::from(array_data);
        assert_eq!(3, fixed_size_binary_array.len());
        assert_eq!(0, fixed_size_binary_array.null_count());
        assert_eq!(
            [b'h', b'e', b'l', b'l', b'o'],
            fixed_size_binary_array.value(0)
        );
        assert_eq!(
            [b't', b'h', b'e', b'r', b'e'],
            fixed_size_binary_array.value(1)
        );
        assert_eq!(
            [b'a', b'r', b'r', b'o', b'w'],
            fixed_size_binary_array.value(2)
        );
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(10, fixed_size_binary_array.value_offset(2));
        for i in 0..3 {
            assert!(fixed_size_binary_array.is_valid(i));
            assert!(!fixed_size_binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(2)
            .offset(1)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let fixed_size_binary_array = FixedSizeBinaryArray::from(array_data);
        assert_eq!(
            [b't', b'h', b'e', b'r', b'e'],
            fixed_size_binary_array.value(0)
        );
        assert_eq!(
            [b'a', b'r', b'r', b'o', b'w'],
            fixed_size_binary_array.value(1)
        );
        assert_eq!(2, fixed_size_binary_array.len());
        assert_eq!(5, fixed_size_binary_array.value_offset(0));
        assert_eq!(5, fixed_size_binary_array.value_length());
        assert_eq!(10, fixed_size_binary_array.value_offset(1));
    }

    #[test]
    #[should_panic(
        expected = "FixedSizeBinaryArray can only be created from list array of u8 values \
                    (i.e. FixedSizeList<PrimitiveArray<u8>>)."
    )]
    fn test_fixed_size_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from(values[..].to_byte_slice()))
            .add_child_data(ArrayData::builder(DataType::Boolean).build())
            .build();

        let array_data = ArrayData::builder(DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Binary, false)),
            4,
        ))
        .len(3)
        .add_child_data(values_data)
        .build();
        let list_array = FixedSizeListArray::from(array_data);
        FixedSizeBinaryArray::from(list_array);
    }

    #[test]
    #[should_panic(expected = "BinaryArray out of bounds access")]
    fn test_binary_array_get_value_index_out_of_bound() {
        let values: [u8; 12] =
            [104, 101, 108, 108, 111, 112, 97, 114, 113, 117, 101, 116];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Binary)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        binary_array.value(4);
    }

    #[test]
    fn test_binary_array_fmt_debug() {
        let values: [u8; 15] = *b"hellotherearrow";

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(5))
            .len(3)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let arr = FixedSizeBinaryArray::from(array_data);
        assert_eq!(
            "FixedSizeBinaryArray<5>\n[\n  [104, 101, 108, 108, 111],\n  [116, 104, 101, 114, 101],\n  [97, 114, 114, 111, 119],\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_decimal_array() {
        // let val_8887: [u8; 16] = [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // let val_neg_8887: [u8; 16] = [64, 36, 75, 238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255];
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal(23, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let decimal_array = DecimalArray::from(array_data);
        assert_eq!(8_887_000_000, decimal_array.value(0));
        assert_eq!(-8_887_000_000, decimal_array.value(1));
        assert_eq!(16, decimal_array.value_length());
    }

    #[test]
    fn test_decimal_array_fmt_debug() {
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal(23, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let arr = DecimalArray::from(array_data);
        assert_eq!(
            "DecimalArray<23, 6>\n[\n  8887000000,\n  -8887000000,\n]",
            format!("{:?}", arr)
        );
    }
}
