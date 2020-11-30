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

use std::any::Any;
use std::convert::From;
use std::fmt;
use std::mem;

use num::Num;

use super::{
    array::print_long_array, make_array, raw_pointer::as_aligned_pointer,
    raw_pointer::RawPtrBox, Array, ArrayDataRef, ArrayRef,
};
use crate::datatypes::ArrowNativeType;
use crate::datatypes::DataType;

/// trait declaring an offset size, relevant for i32 vs i64 array types.
pub trait OffsetSizeTrait: ArrowNativeType + Num + Ord {
    fn prefix() -> &'static str;

    fn to_isize(&self) -> isize;
}

impl OffsetSizeTrait for i32 {
    fn prefix() -> &'static str {
        ""
    }

    fn to_isize(&self) -> isize {
        num::ToPrimitive::to_isize(self).unwrap()
    }
}

impl OffsetSizeTrait for i64 {
    fn prefix() -> &'static str {
        "Large"
    }

    fn to_isize(&self) -> isize {
        num::ToPrimitive::to_isize(self).unwrap()
    }
}

pub struct GenericListArray<OffsetSize> {
    data: ArrayDataRef,
    values: ArrayRef,
    value_offsets: RawPtrBox<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> GenericListArray<OffsetSize> {
    /// Returns a reference to the values of this list.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        self.values.slice(
            self.value_offset(i).to_usize().unwrap(),
            self.value_length(i).to_usize().unwrap(),
        )
    }

    /// Returns the offset for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> OffsetSize {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_length(&self, mut i: usize) -> OffsetSize {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> OffsetSize {
        unsafe { *self.value_offsets.get().add(i) }
    }
}

impl<OffsetSize: OffsetSizeTrait> From<ArrayDataRef> for GenericListArray<OffsetSize> {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "ListArray data should contain a single buffer only (value offsets)"
        );
        assert_eq!(
            data.child_data().len(),
            1,
            "ListArray should contain a single child array (values array)"
        );
        let values = make_array(data.child_data()[0].clone());
        let raw_value_offsets = data.buffers()[0].raw_data();
        let value_offsets: *const OffsetSize = as_aligned_pointer(raw_value_offsets);
        unsafe {
            assert!(
                (*value_offsets.offset(0)).is_zero(),
                "offsets do not start at zero"
            );
        }
        Self {
            data,
            values,
            value_offsets: RawPtrBox::new(value_offsets),
        }
    }
}

impl<OffsetSize: 'static + OffsetSizeTrait> Array for GenericListArray<OffsetSize> {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [ListArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [ListArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

impl<OffsetSize: OffsetSizeTrait> fmt::Debug for GenericListArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}ListArray\n[\n", OffsetSize::prefix())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

/// A list array where each element is a variable-sized sequence of values with the same
/// type whose memory offsets between elements are represented by a i32.
pub type ListArray = GenericListArray<i32>;

/// A list array where each element is a variable-sized sequence of values with the same
/// type whose memory offsets between elements are represented by a i64.
pub type LargeListArray = GenericListArray<i64>;

/// A list array where each element is a fixed-size sequence of values with the same
/// type whose maximum length is represented by a i32.
pub struct FixedSizeListArray {
    data: ArrayDataRef,
    values: ArrayRef,
    length: i32,
}

impl FixedSizeListArray {
    /// Returns a reference to the values of this list.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        self.values
            .slice(self.value_offset(i) as usize, self.value_length() as usize)
    }

    /// Returns the offset for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub const fn value_length(&self) -> i32 {
        self.length
    }

    #[inline]
    const fn value_offset_at(&self, i: usize) -> i32 {
        i as i32 * self.length
    }
}

impl From<ArrayDataRef> for FixedSizeListArray {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            0,
            "FixedSizeListArray data should not contain a buffer for value offsets"
        );
        assert_eq!(
            data.child_data().len(),
            1,
            "FixedSizeListArray should contain a single child array (values array)"
        );
        let values = make_array(data.child_data()[0].clone());
        let length = match data.data_type() {
            DataType::FixedSizeList(_, len) => {
                // check that child data is multiple of length
                assert_eq!(
                    values.len() % *len as usize,
                    0,
                    "FixedSizeListArray child array length should be a multiple of {}",
                    len
                );
                *len
            }
            _ => {
                panic!("FixedSizeListArray data should contain a FixedSizeList data type")
            }
        };
        Self {
            data,
            values,
            length,
        }
    }
}

impl Array for FixedSizeListArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [FixedSizeListArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size() + self.values().get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [FixedSizeListArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size()
            + self.values().get_array_memory_size()
            + mem::size_of_val(self)
    }
}

impl fmt::Debug for FixedSizeListArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FixedSizeListArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::ArrayData,
        array::Int32Array,
        buffer::Buffer,
        datatypes::{Field, ToByteSlice},
        memory,
        util::bit_util,
    };

    use super::*;

    #[test]
    fn test_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(1));
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_large_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from(&[0i64, 3, 6, 8].to_byte_slice());

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(1));
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_fixed_size_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7, 8].to_byte_slice()))
            .build();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_child_data(value_data.clone())
            .build();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
        assert_eq!(
            0,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .offset(1)
            .add_child_data(value_data.clone())
            .build();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(
            3,
            list_array
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0)
        );
        assert_eq!(6, list_array.value_offset(1));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    #[should_panic(
        expected = "FixedSizeListArray child array length should be a multiple of 3"
    )]
    fn test_fixed_size_list_array_unequal_children() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        // Construct a list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            3,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build();
        FixedSizeListArray::from(list_data);
    }

    #[test]
    fn test_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from(
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].to_byte_slice(),
            ))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets =
            Buffer::from(&[0, 2, 2, 2, 4, 6, 6, 9, 9, 10].to_byte_slice());
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Buffer::from(null_bits))
            .build();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offset(3));
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array =
            sliced_array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(2, sliced_list_array.value_offset(2));
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offset(3));
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offset(5));
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    fn test_large_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from(
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].to_byte_slice(),
            ))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets =
            Buffer::from(&[0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10].to_byte_slice());
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Buffer::from(null_bits))
            .build();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offset(3));
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_offset(2));
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offset(3));
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offset(5));
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    fn test_fixed_size_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from(
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].to_byte_slice(),
            ))
            .build();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        // Construct a fixed size list array from the above two
        let list_data_type = DataType::FixedSizeList(
            Box::new(Field::new("item", DataType::Int32, false)),
            2,
        );
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Buffer::from(null_bits))
            .build();
        let list_array = FixedSizeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(5, list_array.len());
        assert_eq!(2, list_array.null_count());
        assert_eq!(6, list_array.value_offset(3));
        assert_eq!(2, list_array.value_length());

        let sliced_array = list_array.slice(1, 4);
        assert_eq!(4, sliced_array.len());
        assert_eq!(1, sliced_array.offset());
        assert_eq!(2, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, sliced_array.offset() + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_length());
        assert_eq!(6, sliced_list_array.value_offset(2));
        assert_eq!(8, sliced_list_array.value_offset(3));
    }

    #[test]
    #[should_panic(
        expected = "ListArray data should contain a single buffer only (value offsets)"
    )]
    fn test_list_array_invalid_buffer_len() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(
        expected = "ListArray should contain a single child array (values array)"
    )]
    fn test_list_array_invalid_child_array_len() {
        let value_offsets = Buffer::from(&[0, 2, 5, 7].to_byte_slice());
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(expected = "offsets do not start at zero")]
    fn test_list_array_invalid_value_offset_start() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        let value_offsets = Buffer::from(&[2, 2, 5, 7].to_byte_slice());

        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_primitive_array_alignment() {
        let ptr = memory::allocate_aligned(8);
        let buf = unsafe { Buffer::from_raw_parts(ptr, 8, 8) };
        let buf2 = buf.slice(1);
        let array_data = ArrayData::builder(DataType::Int32).add_buffer(buf2).build();
        Int32Array::from(array_data);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_list_array_alignment() {
        let ptr = memory::allocate_aligned(8);
        let buf = unsafe { Buffer::from_raw_parts(ptr, 8, 8) };
        let buf2 = buf.slice(1);

        let values: [i32; 8] = [0; 8];
        let value_data = ArrayData::builder(DataType::Int32)
            .add_buffer(Buffer::from(values.to_byte_slice()))
            .build();

        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .add_buffer(buf2)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data);
    }
}
