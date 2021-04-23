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

use std::convert::{From, TryInto};
use std::fmt;
use std::mem;
use std::{any::Any, iter::FromIterator};

use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData,
    FixedSizeListArray, GenericBinaryIter, GenericListArray, OffsetSizeTrait,
};
use crate::buffer::Buffer;
use crate::error::ArrowError;
use crate::util::bit_util;
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
    data: ArrayData,
    value_offsets: RawPtrBox<OffsetSize>,
    value_data: RawPtrBox<u8>,
}

impl<OffsetSize: BinaryOffsetSizeTrait> GenericBinaryArray<OffsetSize> {
    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> OffsetSize {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// Returns a clone of the value data buffer
    pub fn value_data(&self) -> Buffer {
        self.data.buffers()[1].clone()
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[OffsetSize] {
        // Soundness
        //     pointer alignment & location is ensured by RawPtrBox
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.value_offsets.as_ptr().add(self.data.offset()),
                self.len() + 1,
            )
        }
    }

    /// Returns the element at index `i` as bytes slice
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        let end = *self.value_offsets().get_unchecked(i + 1);
        let start = *self.value_offsets().get_unchecked(i);

        // Soundness
        // pointer alignment & location is ensured by RawPtrBox
        // buffer bounds/offset is ensured by the value_offset invariants

        // Safety of `to_isize().unwrap()`
        // `start` and `end` are &OffsetSize, which is a generic type that implements the
        // OffsetSizeTrait. Currently, only i32 and i64 implement OffsetSizeTrait,
        // both of which should cleanly cast to isize on an architecture that supports
        // 32/64-bit offsets
        std::slice::from_raw_parts(
            self.value_data.as_ptr().offset(start.to_isize().unwrap()),
            (end - start).to_usize().unwrap(),
        )
    }

    /// Returns the element at index `i` as bytes slice
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.data.len(), "BinaryArray out of bounds access");
        //Soundness: length checked above, offset buffer length is 1 larger than logical array length
        let end = unsafe { self.value_offsets().get_unchecked(i + 1) };
        let start = unsafe { self.value_offsets().get_unchecked(i) };

        // Soundness
        // pointer alignment & location is ensured by RawPtrBox
        // buffer bounds/offset is ensured by the value_offset invariants

        // Safety of `to_isize().unwrap()`
        // `start` and `end` are &OffsetSize, which is a generic type that implements the
        // OffsetSizeTrait. Currently, only i32 and i64 implement OffsetSizeTrait,
        // both of which should cleanly cast to isize on an architecture that supports
        // 32/64-bit offsets
        unsafe {
            std::slice::from_raw_parts(
                self.value_data.as_ptr().offset(start.to_isize().unwrap()),
                (*end - *start).to_usize().unwrap(),
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
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s);
        }
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
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
            builder = builder.null_bit_buffer(bitmap.bits.clone())
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
        let prefix = if OffsetSize::is_large() { "Large" } else { "" };

        write!(f, "{}BinaryArray\n[\n", prefix)?;
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

    fn data(&self) -> &ArrayData {
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

impl<OffsetSize: BinaryOffsetSizeTrait> From<ArrayData>
    for GenericBinaryArray<OffsetSize>
{
    fn from(data: ArrayData) -> Self {
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
        let offsets = data.buffers()[0].as_ptr();
        let values = data.buffers()[1].as_ptr();
        Self {
            data,
            value_offsets: unsafe { RawPtrBox::new(offsets) },
            value_data: unsafe { RawPtrBox::new(values) },
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
            let null_slice = null_buf.as_slice_mut();

            for (i, s) in iter.enumerate() {
                if let Some(s) = s {
                    let s = s.as_ref();
                    bit_util::set_bit(null_slice, i);
                    length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                    values.extend_from_slice(s);
                }
                // always add an element in offsets
                offsets.push(length_so_far);
            }
        }

        // calculate actual data_len, which may be different from the iterator's upper bound
        let data_len = offsets.len() - 1;
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(data_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .null_bit_buffer(null_buf.into())
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

impl<OffsetSize: BinaryOffsetSizeTrait> From<Vec<Option<&[u8]>>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(v: Vec<Option<&[u8]>>) -> Self {
        GenericBinaryArray::<OffsetSize>::from_opt_vec(v)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> From<Vec<&[u8]>>
    for GenericBinaryArray<OffsetSize>
{
    fn from(v: Vec<&[u8]>) -> Self {
        GenericBinaryArray::<OffsetSize>::from_vec(v)
    }
}

impl<T: BinaryOffsetSizeTrait> From<GenericListArray<T>> for GenericBinaryArray<T> {
    fn from(v: GenericListArray<T>) -> Self {
        GenericBinaryArray::<T>::from_list(v)
    }
}

/// A type of `FixedSizeListArray` whose elements are binaries.
pub struct FixedSizeBinaryArray {
    data: ArrayData,
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
                self.value_data.as_ptr().offset(pos as isize),
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

    /// Create an array from an iterable argument of sparse byte slices.
    /// Sparsity means that items returned by the iterator are optional, i.e input argument can
    /// contain `None` items.
    ///
    /// # Examles
    ///
    /// ```
    /// use arrow::array::FixedSizeBinaryArray;
    /// let input_arg = vec![
    ///     None,
    ///     Some(vec![7, 8]),
    ///     Some(vec![9, 10]),
    ///     None,
    ///     Some(vec![13, 14]),
    ///     None,
    /// ];
    /// let array = FixedSizeBinaryArray::try_from_sparse_iter(input_arg.into_iter()).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if argument has length zero, or sizes of nested slices don't match.
    pub fn try_from_sparse_iter<T, U>(mut iter: T) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = Option<U>>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let mut byte = 0;
        let mut null_buf = MutableBuffer::from_len_zeroed(0);
        let mut buffer = MutableBuffer::from_len_zeroed(0);
        let mut prepend = 0;
        iter.try_for_each(|item| -> Result<(), ArrowError> {
            // extend null bitmask by one byte per each 8 items
            if byte == 0 {
                null_buf.push(0u8);
                byte = 8;
            }
            byte -= 1;

            if let Some(slice) = item {
                let slice = slice.as_ref();
                if let Some(size) = size {
                    if size != slice.len() {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Nested array size mismatch: one is {}, and the other is {}",
                            size,
                            slice.len()
                        )));
                    }
                } else {
                    size = Some(slice.len());
                    buffer.extend_zeros(slice.len() * prepend);
                }
                bit_util::set_bit(null_buf.as_slice_mut(), len);
                buffer.extend_from_slice(slice);
            } else if let Some(size) = size {
                buffer.extend_zeros(size);
            } else {
                prepend += 1;
            }

            len += 1;

            Ok(())
        })?;

        if len == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Input iterable argument has no data".to_owned(),
            ));
        }

        let size = size.unwrap_or(0);
        let array_data = ArrayData::new(
            DataType::FixedSizeBinary(size as i32),
            len,
            None,
            Some(null_buf.into()),
            0,
            vec![buffer.into()],
            vec![],
        );
        Ok(FixedSizeBinaryArray::from(array_data))
    }

    /// Create an array from an iterable argument of byte slices.
    ///
    /// # Examles
    ///
    /// ```
    /// use arrow::array::FixedSizeBinaryArray;
    /// let input_arg = vec![
    ///     vec![1, 2],
    ///     vec![3, 4],
    ///     vec![5, 6],
    /// ];
    /// let array = FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error if argument has length zero, or sizes of nested slices don't match.
    pub fn try_from_iter<T, U>(mut iter: T) -> Result<Self, ArrowError>
    where
        T: Iterator<Item = U>,
        U: AsRef<[u8]>,
    {
        let mut len = 0;
        let mut size = None;
        let mut buffer = MutableBuffer::from_len_zeroed(0);
        iter.try_for_each(|item| -> Result<(), ArrowError> {
            let slice = item.as_ref();
            if let Some(size) = size {
                if size != slice.len() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Nested array size mismatch: one is {}, and the other is {}",
                        size,
                        slice.len()
                    )));
                }
            } else {
                size = Some(slice.len());
            }
            buffer.extend_from_slice(slice);

            len += 1;

            Ok(())
        })?;

        if len == 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Input iterable argument has no data".to_owned(),
            ));
        }

        let size = size.unwrap_or(0);
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(size as i32))
            .len(len)
            .add_buffer(buffer.into())
            .build();
        Ok(FixedSizeBinaryArray::from(array_data))
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        self.length * i as i32
    }
}

impl From<ArrayData> for FixedSizeBinaryArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "FixedSizeBinaryArray data should contain 1 buffer only (values)"
        );
        let value_data = data.buffers()[0].as_ptr();
        let length = match data.data_type() {
            DataType::FixedSizeBinary(len) => *len,
            _ => panic!("Expected data type to be FixedSizeBinary"),
        };
        Self {
            data,
            value_data: unsafe { RawPtrBox::new(value_data) },
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
            builder = builder.null_bit_buffer(bitmap.bits.clone())
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

    fn data(&self) -> &ArrayData {
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
    data: ArrayData,
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
                self.value_data.as_ptr().offset(pos as isize),
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
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }
    pub fn precision(&self) -> usize {
        self.precision
    }

    pub fn scale(&self) -> usize {
        self.scale
    }
}

impl From<ArrayData> for DecimalArray {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DecimalArray data should contain 1 buffer only (values)"
        );
        let values = data.buffers()[0].as_ptr();
        let (precision, scale) = match data.data_type() {
            DataType::Decimal(precision, scale) => (*precision, *scale),
            _ => panic!("Expected data type to be Decimal"),
        };
        let length = 16;
        Self {
            data,
            value_data: unsafe { RawPtrBox::new(values) },
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

    fn data(&self) -> &ArrayData {
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
    use crate::{
        array::{LargeListArray, ListArray},
        datatypes::Field,
    };

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
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], unsafe {
            binary_array.value_unchecked(0)
        });
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([] as [u8; 0], unsafe { binary_array.value_unchecked(1) });
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(2)
        });
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::Binary)
            .len(4)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!(5, binary_array.value_offsets()[0]);
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offsets()[1]);
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
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], unsafe {
            binary_array.value_unchecked(0)
        });
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([] as [u8; 0], unsafe { binary_array.value_unchecked(1) });
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(2)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(2)
        });
        assert_eq!(5, binary_array.value_offsets()[2]);
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::LargeBinary)
            .len(4)
            .offset(1)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array = LargeBinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.value(1)
        );
        assert_eq!([b'p', b'a', b'r', b'q', b'u', b'e', b't'], unsafe {
            binary_array.value_unchecked(1)
        });
        assert_eq!(5, binary_array.value_offsets()[0]);
        assert_eq!(0, binary_array.value_length(0));
        assert_eq!(5, binary_array.value_offsets()[1]);
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
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array1 = BinaryArray::from(array_data1);

        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::UInt8, false)));
        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(values_data)
            .build();
        let list_array = ListArray::from(array_data2);
        let binary_array2 = BinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        assert_eq!(binary_array1.value_offsets(), binary_array2.value_offsets());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value(i), unsafe {
                binary_array2.value_unchecked(i)
            });
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
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let binary_array1 = LargeBinaryArray::from(array_data1);

        let data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::UInt8, false)));
        let array_data2 = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(values_data)
            .build();
        let list_array = LargeListArray::from(array_data2);
        let binary_array2 = LargeBinaryArray::from(list_array);

        assert_eq!(2, binary_array2.data().buffers().len());
        assert_eq!(0, binary_array2.data().child_data().len());

        assert_eq!(binary_array1.len(), binary_array2.len());
        assert_eq!(binary_array1.null_count(), binary_array2.null_count());
        assert_eq!(binary_array1.value_offsets(), binary_array2.value_offsets());
        for i in 0..binary_array1.len() {
            assert_eq!(binary_array1.value(i), binary_array2.value(i));
            assert_eq!(binary_array1.value(i), unsafe {
                binary_array2.value_unchecked(i)
            });
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
    fn test_binary_array_from_unbound_iter() {
        // iterator that doesn't declare (upper) size bound
        let value_iter = (0..)
            .scan(0usize, |pos, i| {
                if *pos < 10 {
                    *pos += 1;
                    Some(Some(format!("value {}", i)))
                } else {
                    // actually returns up to 10 values
                    None
                }
            })
            // limited using take()
            .take(100);

        let (_, upper_size_bound) = value_iter.size_hint();
        // the upper bound, defined by take above, is 100
        assert_eq!(upper_size_bound, Some(100));
        let binary_array: BinaryArray = value_iter.collect();
        // but the actual number of items in the array should be 10
        assert_eq!(binary_array.len(), 10);
    }

    #[test]
    #[should_panic(
        expected = "assertion failed: `(left == right)`\n  left: `UInt32`,\n \
                    right: `UInt8`: BinaryArray can only be created from List<u8> arrays, \
                    mismatched data types."
    )]
    fn test_binary_array_from_incorrect_list_array() {
        let values: [u32; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let values_data = ArrayData::builder(DataType::UInt32)
            .len(12)
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::UInt32, false)));
        let array_data = ArrayData::builder(data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
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
            .add_buffer(Buffer::from_slice_ref(&values))
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
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
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
