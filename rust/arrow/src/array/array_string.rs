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

use std::convert::From;
use std::fmt;
use std::mem;
use std::{any::Any, iter::FromIterator};

use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData, ArrayDataRef,
    GenericListArray, GenericStringIter, OffsetSizeTrait,
};
use crate::buffer::Buffer;
use crate::util::bit_util;
use crate::{buffer::MutableBuffer, datatypes::DataType};

/// Like OffsetSizeTrait, but specialized for Strings
// This allow us to expose a constant datatype for the GenericStringArray
pub trait StringOffsetSizeTrait: OffsetSizeTrait {
    const DATA_TYPE: DataType;
}

impl StringOffsetSizeTrait for i32 {
    const DATA_TYPE: DataType = DataType::Utf8;
}

impl StringOffsetSizeTrait for i64 {
    const DATA_TYPE: DataType = DataType::LargeUtf8;
}

/// Generic struct for \[Large\]StringArray
pub struct GenericStringArray<OffsetSize: StringOffsetSizeTrait> {
    data: ArrayDataRef,
    value_offsets: RawPtrBox<OffsetSize>,
    value_data: RawPtrBox<u8>,
}

impl<OffsetSize: StringOffsetSizeTrait> GenericStringArray<OffsetSize> {
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
        unsafe { *self.value_offsets.as_ptr().add(i) }
    }

    /// Returns the element at index `i` as &str
    pub fn value(&self, i: usize) -> &str {
        assert!(i < self.data.len(), "StringArray out of bounds access");
        let offset = i.checked_add(self.data.offset()).unwrap();
        unsafe {
            let pos = self.value_offset_at(offset);
            let slice = std::slice::from_raw_parts(
                self.value_data.as_ptr().offset(pos.to_isize()),
                (self.value_offset_at(offset + 1) - pos).to_usize().unwrap(),
            );

            std::str::from_utf8_unchecked(slice)
        }
    }

    fn from_list(v: GenericListArray<OffsetSize>) -> Self {
        assert_eq!(
            v.data().child_data()[0].child_data().len(),
            0,
            "StringArray can only be created from list array of u8 values \
             (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "StringArray can only be created from List<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(v.data_ref().buffers()[0].clone())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data().null_bitmap() {
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }

    pub(crate) fn from_vec(v: Vec<&str>) -> Self {
        let mut offsets =
            MutableBuffer::new((v.len() + 1) * std::mem::size_of::<OffsetSize>());
        let mut values = MutableBuffer::new(0);

        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        for s in &v {
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s.as_bytes());
        }
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(v.len())
            .add_buffer(offsets.into())
            .add_buffer(values.into())
            .build();
        Self::from(array_data)
    }

    pub(crate) fn from_opt_vec(v: Vec<Option<&str>>) -> Self {
        v.into_iter().collect()
    }

    /// Creates a `GenericStringArray` based on an iterator of values without nulls
    pub fn from_iter_values<Ptr, I: IntoIterator<Item = Ptr>>(iter: I) -> Self
    where
        Ptr: AsRef<str>,
    {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets =
            MutableBuffer::new((data_len + 1) * std::mem::size_of::<OffsetSize>());
        let mut values = MutableBuffer::new(0);

        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        for i in iter {
            let s = i.as_ref();
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s.as_bytes());
        }
        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(data_len)
            .add_buffer(offsets.into())
            .add_buffer(values.into())
            .build();
        Self::from(array_data)
    }
}

impl<'a, Ptr, OffsetSize: StringOffsetSizeTrait> FromIterator<Option<Ptr>>
    for GenericStringArray<OffsetSize>
where
    Ptr: AsRef<str>,
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (_, data_len) = iter.size_hint();
        let data_len = data_len.expect("Iterator must be sized"); // panic if no upper bound.

        let mut offsets =
            MutableBuffer::new((data_len + 1) * std::mem::size_of::<OffsetSize>());
        let mut values = MutableBuffer::new(0);
        let mut null_buf = MutableBuffer::new_null(data_len);
        let null_slice = null_buf.as_slice_mut();
        let mut length_so_far = OffsetSize::zero();
        offsets.push(length_so_far);

        for (i, s) in iter.enumerate() {
            if let Some(s) = s {
                let s = s.as_ref();
                // set null bit
                bit_util::set_bit(null_slice, i);

                length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                values.extend_from_slice(s.as_bytes());
            } else {
                values.extend_from_slice(b"");
            }
            offsets.push(length_so_far);
        }

        let array_data = ArrayData::builder(OffsetSize::DATA_TYPE)
            .len(data_len)
            .add_buffer(offsets.into())
            .add_buffer(values.into())
            .null_bit_buffer(null_buf.into())
            .build();
        Self::from(array_data)
    }
}

impl<'a, T: StringOffsetSizeTrait> IntoIterator for &'a GenericStringArray<T> {
    type Item = Option<&'a str>;
    type IntoIter = GenericStringIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        GenericStringIter::<'a, T>::new(self)
    }
}

impl<'a, T: StringOffsetSizeTrait> GenericStringArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> GenericStringIter<'a, T> {
        GenericStringIter::<'a, T>::new(&self)
    }
}

impl<OffsetSize: StringOffsetSizeTrait> fmt::Debug for GenericStringArray<OffsetSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}StringArray\n[\n", OffsetSize::prefix())?;
        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl<OffsetSize: StringOffsetSizeTrait> Array for GenericStringArray<OffsetSize> {
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

impl<OffsetSize: StringOffsetSizeTrait> From<ArrayDataRef>
    for GenericStringArray<OffsetSize>
{
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.data_type(),
            &<OffsetSize as StringOffsetSizeTrait>::DATA_TYPE,
            "[Large]StringArray expects Datatype::[Large]Utf8"
        );
        assert_eq!(
            data.buffers().len(),
            2,
            "StringArray data should contain 2 buffers only (offsets and values)"
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

impl<OffsetSize: StringOffsetSizeTrait> From<Vec<Option<&str>>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: Vec<Option<&str>>) -> Self {
        GenericStringArray::<OffsetSize>::from_opt_vec(v)
    }
}

impl<OffsetSize: StringOffsetSizeTrait> From<Vec<&str>>
    for GenericStringArray<OffsetSize>
{
    fn from(v: Vec<&str>) -> Self {
        GenericStringArray::<OffsetSize>::from_vec(v)
    }
}

/// An array where each element is a variable-sized sequence of bytes representing a string
/// whose maximum length (in bytes) is represented by a i32.
pub type StringArray = GenericStringArray<i32>;

/// An array where each element is a variable-sized sequence of bytes representing a string
/// whose maximum length (in bytes) is represented by a i64.
pub type LargeStringArray = GenericStringArray<i64>;

impl<T: StringOffsetSizeTrait> From<GenericListArray<T>> for GenericStringArray<T> {
    fn from(v: GenericListArray<T>) -> Self {
        GenericStringArray::<T>::from_list(v)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{ListBuilder, StringBuilder};

    use super::*;

    #[test]
    fn test_string_array_from_u8_slice() {
        let values: Vec<&str> = vec!["hello", "", "parquet"];

        // Array data: ["hello", "", "parquet"]
        let string_array = StringArray::from(values);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("parquet", string_array.value(2));
        assert_eq!(5, string_array.value_offset(2));
        assert_eq!(7, string_array.value_length(2));
        for i in 0..3 {
            assert!(string_array.is_valid(i));
            assert!(!string_array.is_null(i));
        }
    }

    #[test]
    #[should_panic(expected = "[Large]StringArray expects Datatype::[Large]Utf8")]
    fn test_string_array_from_int() {
        let array = LargeStringArray::from(vec!["a", "b"]);
        StringArray::from(array.data());
    }

    #[test]
    fn test_large_string_array_from_u8_slice() {
        let values: Vec<&str> = vec!["hello", "", "parquet"];

        // Array data: ["hello", "", "parquet"]
        let string_array = LargeStringArray::from(values);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("parquet", string_array.value(2));
        assert_eq!(5, string_array.value_offset(2));
        assert_eq!(7, string_array.value_length(2));
        for i in 0..3 {
            assert!(string_array.is_valid(i));
            assert!(!string_array.is_null(i));
        }
    }

    #[test]
    fn test_nested_string_array() {
        let string_builder = StringBuilder::new(3);
        let mut list_of_string_builder = ListBuilder::new(string_builder);

        list_of_string_builder.values().append_value("foo").unwrap();
        list_of_string_builder.values().append_value("bar").unwrap();
        list_of_string_builder.append(true).unwrap();

        list_of_string_builder
            .values()
            .append_value("foobar")
            .unwrap();
        list_of_string_builder.append(true).unwrap();
        let list_of_strings = list_of_string_builder.finish();

        assert_eq!(list_of_strings.len(), 2);

        let first_slot = list_of_strings.value(0);
        let first_list = first_slot.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(first_list.len(), 2);
        assert_eq!(first_list.value(0), "foo");
        assert_eq!(first_list.value(1), "bar");

        let second_slot = list_of_strings.value(1);
        let second_list = second_slot.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(second_list.len(), 1);
        assert_eq!(second_list.value(0), "foobar");
    }

    #[test]
    #[should_panic(expected = "StringArray out of bounds access")]
    fn test_string_array_get_value_index_out_of_bound() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values))
            .build();
        let string_array = StringArray::from(array_data);
        string_array.value(4);
    }

    #[test]
    fn test_string_array_fmt_debug() {
        let arr: StringArray = vec!["hello", "arrow"].into();
        assert_eq!(
            "StringArray\n[\n  \"hello\",\n  \"arrow\",\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_large_string_array_fmt_debug() {
        let arr: LargeStringArray = vec!["hello", "arrow"].into();
        assert_eq!(
            "LargeStringArray\n[\n  \"hello\",\n  \"arrow\",\n]",
            format!("{:?}", arr)
        );
    }

    #[test]
    fn test_string_array_from_iter() {
        let data = vec![Some("hello"), None, Some("arrow")];
        // from Vec<Option<&str>>
        let array1 = StringArray::from(data.clone());
        // from Iterator<Option<&str>>
        let array2: StringArray = data.clone().into_iter().collect();
        // from Iterator<Option<String>>
        let array3: StringArray =
            data.into_iter().map(|x| x.map(|s| s.to_string())).collect();

        assert_eq!(array1, array2);
        assert_eq!(array2, array3);
    }

    #[test]
    fn test_string_array_from_iter_values() {
        let data = vec!["hello", "hello2"];
        let array1 = StringArray::from_iter_values(data.iter());

        assert_eq!(array1.value(0), "hello");
        assert_eq!(array1.value(1), "hello2");
    }
}
