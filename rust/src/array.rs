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

///! Array types
use std::any::Any;
use std::convert::From;
use std::io::Write;
use std::mem;
use std::sync::Arc;

use crate::array_data::{ArrayData, ArrayDataRef};
use crate::buffer::{Buffer, MutableBuffer};
use crate::builder::*;
use crate::datatypes::*;
use crate::memory;
use crate::util::bit_util;

/// Trait for dealing with different types of array at runtime when the type of the
/// array is not known in advance
pub trait Array: Send + Sync {
    /// Returns the array as `Any` so that it can be downcast to a specific implementation
    fn as_any(&self) -> &Any;

    /// Returns a reference-counted pointer to the data of this array
    fn data(&self) -> ArrayDataRef;

    /// Returns a borrowed & reference-counted pointer to the data of this array
    fn data_ref(&self) -> &ArrayDataRef;

    /// Returns a reference to the data type of this array
    fn data_type(&self) -> &DataType {
        self.data_ref().data_type()
    }

    /// Returns the length (i.e., number of elements) of this array
    fn len(&self) -> usize {
        self.data().len()
    }

    /// Returns the offset of this array
    fn offset(&self) -> usize {
        self.data().offset()
    }

    /// Returns whether the element at index `i` is null
    fn is_null(&self, i: usize) -> bool {
        self.data().is_null(i)
    }

    /// Returns whether the element at index `i` is not null
    fn is_valid(&self, i: usize) -> bool {
        self.data().is_valid(i)
    }

    /// Returns the total number of nulls in this array
    fn null_count(&self) -> usize {
        self.data().null_count()
    }
}

pub type ArrayRef = Arc<Array>;

/// Constructs an array using the input `data`. Returns a reference-counted `Array`
/// instance.
fn make_array(data: ArrayDataRef) -> ArrayRef {
    // TODO: here data_type() needs to clone the type - maybe add a type tag enum to
    // avoid the cloning.
    match data.data_type().clone() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
    }
}

/// ----------------------------------------------------------------------------
/// Implementations of different array types

struct RawPtrBox<T> {
    inner: *const T,
}

impl<T> RawPtrBox<T> {
    fn new(inner: *const T) -> Self {
        Self { inner }
    }

    fn get(&self) -> *const T {
        self.inner
    }
}

unsafe impl<T> Send for RawPtrBox<T> {}
unsafe impl<T> Sync for RawPtrBox<T> {}

/// Array whose elements are of primitive types.
pub struct PrimitiveArray<T: ArrowPrimitiveType> {
    data: ArrayDataRef,
    /// Pointer to the value array. The lifetime of this must be <= to the value buffer
    /// stored in `data`, so it's safe to store.
    /// Also note that boolean arrays are bit-packed, so although the underlying pointer is of type
    /// bool it should be cast back to u8 before being used.
    /// i.e. `self.raw_values.get() as *const u8`
    raw_values: RawPtrBox<T::Native>,
}

pub type BooleanArray = PrimitiveArray<BooleanType>;
pub type Int8Array = PrimitiveArray<Int8Type>;
pub type Int16Array = PrimitiveArray<Int16Type>;
pub type Int32Array = PrimitiveArray<Int32Type>;
pub type Int64Array = PrimitiveArray<Int64Type>;
pub type UInt8Array = PrimitiveArray<UInt8Type>;
pub type UInt16Array = PrimitiveArray<UInt16Type>;
pub type UInt32Array = PrimitiveArray<UInt32Type>;
pub type UInt64Array = PrimitiveArray<UInt64Type>;
pub type Float32Array = PrimitiveArray<Float32Type>;
pub type Float64Array = PrimitiveArray<Float64Type>;

impl<T: ArrowPrimitiveType> Array for PrimitiveArray<T> {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }
}

/// Implementation for primitive arrays with numeric types.
/// Boolean arrays are bit-packed and so implemented separately.
impl<T: ArrowNumericType> PrimitiveArray<T> {
    pub fn new(length: usize, values: Buffer, null_count: usize, offset: usize) -> Self {
        let array_data = ArrayData::builder(T::get_data_type())
            .len(length)
            .add_buffer(values)
            .null_count(null_count)
            .offset(offset)
            .build();
        PrimitiveArray::from(array_data)
    }

    /// Returns a `Buffer` holds all the values of this array.
    ///
    /// Note this doesn't take account into the offset of this array.
    pub fn values(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    /// Returns the length of this array
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns a raw pointer to the values of this array.
    pub fn raw_values(&self) -> *const T::Native {
        unsafe { mem::transmute(self.raw_values.get().offset(self.data.offset() as isize)) }
    }

    /// Returns the primitive value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    pub fn value(&self, i: usize) -> T::Native {
        unsafe { *(self.raw_values().offset(i as isize)) }
    }

    /// Returns a slice for the given offset and length
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    pub fn value_slice(&self, offset: usize, len: usize) -> &[T::Native] {
        let raw = unsafe { std::slice::from_raw_parts(self.raw_values(), self.len()) };
        &raw[offset..offset + len]
    }

    // Returns a new primitive array builder
    pub fn builder(capacity: usize) -> PrimitiveArrayBuilder<T> {
        PrimitiveArrayBuilder::<T>::new(capacity)
    }
}

/// Specific implementation for Boolean arrays due to bit-packing
impl PrimitiveArray<BooleanType> {
    pub fn new(length: usize, values: Buffer, null_count: usize, offset: usize) -> Self {
        let array_data = ArrayData::builder(DataType::Boolean)
            .len(length)
            .add_buffer(values)
            .null_count(null_count)
            .offset(offset)
            .build();
        BooleanArray::from(array_data)
    }

    /// Returns a `Buffer` holds all the values of this array.
    ///
    /// Note this doesn't take account into the offset of this array.
    pub fn values(&self) -> Buffer {
        self.data.buffers()[0].clone()
    }

    /// Returns the boolean value at index `i`.
    pub fn value(&self, i: usize) -> bool {
        let offset = i + self.offset();
        assert!(offset < self.data.len());
        unsafe { bit_util::get_bit_raw(self.raw_values.get() as *const u8, offset) }
    }

    // Returns a new primitive array builder
    pub fn builder(capacity: usize) -> BooleanBuilder {
        BooleanBuilder::new(capacity)
    }
}

// TODO: the macro is needed here because we'd get "conflicting implementations" error
// otherwise with both `From<Vec<T::Native>>` and `From<Vec<Option<T::Native>>>`.
// We should revisit this in future.
macro_rules! def_numeric_from_vec {
    ( $ty:ident, $native_ty:ident, $ty_id:path ) => {
        impl From<Vec<$native_ty>> for PrimitiveArray<$ty> {
            fn from(data: Vec<$native_ty>) -> Self {
                let array_data = ArrayData::builder($ty_id)
                    .len(data.len())
                    .add_buffer(Buffer::from(data.to_byte_slice()))
                    .build();
                PrimitiveArray::from(array_data)
            }
        }

        // Constructs a primitive array from a vector. Should only be used for testing.
        impl From<Vec<Option<$native_ty>>> for PrimitiveArray<$ty> {
            fn from(data: Vec<Option<$native_ty>>) -> Self {
                let data_len = data.len();
                let num_bytes = bit_util::ceil(data_len, 8);
                let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                let mut val_buf = MutableBuffer::new(data_len * mem::size_of::<$native_ty>());

                {
                    let null = vec![0; mem::size_of::<$native_ty>()];
                    let null_slice = null_buf.data_mut();
                    for (i, v) in data.iter().enumerate() {
                        if let Some(n) = v {
                            bit_util::set_bit(null_slice, i);
                            // unwrap() in the following should be safe here since we've
                            // made sure enough space is allocated for the values.
                            val_buf.write(&n.to_byte_slice()).unwrap();
                        } else {
                            val_buf.write(&null).unwrap();
                        }
                    }
                }

                let array_data = ArrayData::builder($ty_id)
                    .len(data_len)
                    .add_buffer(val_buf.freeze())
                    .null_bit_buffer(null_buf.freeze())
                    .build();
                PrimitiveArray::from(array_data)
            }
        }
    };
}

def_numeric_from_vec!(Int8Type, i8, DataType::Int8);
def_numeric_from_vec!(Int16Type, i16, DataType::Int16);
def_numeric_from_vec!(Int32Type, i32, DataType::Int32);
def_numeric_from_vec!(Int64Type, i64, DataType::Int64);
def_numeric_from_vec!(UInt8Type, u8, DataType::UInt8);
def_numeric_from_vec!(UInt16Type, u16, DataType::UInt16);
def_numeric_from_vec!(UInt32Type, u32, DataType::UInt32);
def_numeric_from_vec!(UInt64Type, u64, DataType::UInt64);
def_numeric_from_vec!(Float32Type, f32, DataType::Float32);
def_numeric_from_vec!(Float64Type, f64, DataType::Float64);

/// Constructs a boolean array from a vector. Should only be used for testing.
impl From<Vec<bool>> for BooleanArray {
    fn from(data: Vec<bool>) -> Self {
        let num_byte = bit_util::ceil(data.len(), 8);
        let mut mut_buf = MutableBuffer::new(num_byte).with_bitset(num_byte, false);
        {
            let mut_slice = mut_buf.data_mut();
            for (i, b) in data.iter().enumerate() {
                if *b {
                    bit_util::set_bit(mut_slice, i);
                }
            }
        }
        let array_data = ArrayData::builder(DataType::Boolean)
            .len(data.len())
            .add_buffer(mut_buf.freeze())
            .build();
        BooleanArray::from(array_data)
    }
}

impl From<Vec<Option<bool>>> for BooleanArray {
    fn from(data: Vec<Option<bool>>) -> Self {
        let data_len = data.len();
        let num_byte = bit_util::ceil(data_len, 8);
        let mut null_buf = MutableBuffer::new(num_byte).with_bitset(num_byte, false);
        let mut val_buf = MutableBuffer::new(num_byte).with_bitset(num_byte, false);

        {
            let null_slice = null_buf.data_mut();
            let val_slice = val_buf.data_mut();

            for (i, v) in data.iter().enumerate() {
                if let Some(b) = v {
                    bit_util::set_bit(null_slice, i);
                    if *b {
                        bit_util::set_bit(val_slice, i);
                    }
                }
            }
        }

        let array_data = ArrayData::builder(DataType::Boolean)
            .len(data_len)
            .add_buffer(val_buf.freeze())
            .null_bit_buffer(null_buf.freeze())
            .build();
        BooleanArray::from(array_data)
    }
}

/// Constructs a `PrimitiveArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayDataRef> for PrimitiveArray<T> {
    default fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "PrimitiveArray data should contain a single buffer only (values buffer)"
        );
        let raw_values = data.buffers()[0].raw_data();
        assert!(
            memory::is_aligned::<u8>(raw_values, mem::align_of::<T::Native>()),
            "memory is not aligned"
        );
        Self {
            data,
            raw_values: RawPtrBox::new(raw_values as *const T::Native),
        }
    }
}

/// A list array where each element is a variable-sized sequence of values with the same
/// type.
pub struct ListArray {
    data: ArrayDataRef,
    values: ArrayRef,
    value_offsets: RawPtrBox<i32>,
}

impl ListArray {
    /// Returns an reference to the values of this list.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data().data_type().clone()
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
    pub fn value_length(&self, mut i: usize) -> i32 {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        unsafe { *self.value_offsets.get().offset(i as isize) }
    }
}

/// Constructs a `ListArray` from an array data reference.
impl From<ArrayDataRef> for ListArray {
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
        assert!(
            memory::is_aligned(raw_value_offsets, mem::align_of::<i32>()),
            "memory is not aligned"
        );
        let value_offsets = raw_value_offsets as *const i32;
        unsafe {
            assert_eq!(*value_offsets.offset(0), 0, "offsets do not start at zero");
            assert_eq!(
                *value_offsets.offset(data.len() as isize),
                values.data().len() as i32,
                "inconsistent offsets buffer and values array"
            );
        }
        Self {
            data: data.clone(),
            values,
            value_offsets: RawPtrBox::new(value_offsets),
        }
    }
}

impl Array for ListArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }
}

/// A special type of `ListArray` whose elements are binaries.
pub struct BinaryArray {
    data: ArrayDataRef,
    value_offsets: RawPtrBox<i32>,
    value_data: RawPtrBox<u8>,
}

impl BinaryArray {
    /// Returns the element at index `i` as a byte slice.
    pub fn get_value(&self, i: usize) -> &[u8] {
        assert!(i < self.data.len(), "BinaryArray out of bounds access");
        let offset = i.checked_add(self.data.offset()).unwrap();
        unsafe {
            let pos = self.value_offset_at(offset);
            ::std::slice::from_raw_parts(
                self.value_data.get().offset(pos as isize),
                (self.value_offset_at(offset + 1) - pos) as usize,
            )
        }
    }

    /// Returns the element at index `i` as a string.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    pub fn get_string(&self, i: usize) -> String {
        let slice = self.get_value(i);
        unsafe { String::from_utf8_unchecked(Vec::from(slice)) }
    }

    /// Returns the offset for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for the element at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_length(&self, mut i: usize) -> i32 {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    #[inline]
    fn value_offset_at(&self, i: usize) -> i32 {
        unsafe { *self.value_offsets.get().offset(i as isize) }
    }
}

impl From<ArrayDataRef> for BinaryArray {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            2,
            "BinaryArray data should contain 2 buffers only (offsets and values)"
        );
        let raw_value_offsets = data.buffers()[0].raw_data();
        assert!(
            memory::is_aligned(raw_value_offsets, mem::align_of::<i32>()),
            "memory is not aligned"
        );
        let value_data = data.buffers()[1].raw_data();
        Self {
            data: data.clone(),
            value_offsets: RawPtrBox::new(raw_value_offsets as *const i32),
            value_data: RawPtrBox::new(value_data),
        }
    }
}

impl<'a> From<Vec<&'a str>> for BinaryArray {
    fn from(v: Vec<&'a str>) -> Self {
        let mut offsets = vec![];
        let mut values = vec![];
        let mut length_so_far = 0;
        offsets.push(length_so_far);
        for s in &v {
            length_so_far += s.len() as i32;
            offsets.push(length_so_far as i32);
            values.extend_from_slice(s.as_bytes());
        }
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(v.len())
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        BinaryArray::from(array_data)
    }
}

/// Creates a `BinaryArray` from `List<u8>` array
impl From<ListArray> for BinaryArray {
    fn from(v: ListArray) -> Self {
        assert_eq!(
            v.data().child_data()[0].child_data().len(),
            0,
            "BinaryArray can only be created from list array of u8 values (i.e. List<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data().child_data()[0].data_type(),
            &DataType::UInt8,
            "BinaryArray can only be created from List<u8> arrays, mismatched data types."
        );

        let data = ArrayData::builder(DataType::Utf8)
            .len(v.len())
            .add_buffer(v.data().buffers()[0].clone())
            .add_buffer(v.data().child_data()[0].buffers()[0].clone())
            .build();
        Self::from(data)
    }
}

impl Array for BinaryArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }
}

/// A nested array type where each child (called *field*) is represented by a separate array.
pub struct StructArray {
    data: ArrayDataRef,
    boxed_fields: Vec<ArrayRef>,
}

impl StructArray {
    /// Returns the field at `pos`.
    pub fn column(&self, pos: usize) -> &ArrayRef {
        &self.boxed_fields[pos]
    }
}

impl From<ArrayDataRef> for StructArray {
    fn from(data: ArrayDataRef) -> Self {
        let mut boxed_fields = vec![];
        for cd in data.child_data() {
            boxed_fields.push(make_array(cd.clone()));
        }
        Self { data, boxed_fields }
    }
}

impl Array for StructArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the length (i.e., number of elements) of this array
    fn len(&self) -> usize {
        self.boxed_fields[0].len()
    }
}

impl From<Vec<(Field, ArrayRef)>> for StructArray {
    fn from(v: Vec<(Field, ArrayRef)>) -> Self {
        let (field_types, field_values): (Vec<_>, Vec<_>) = v.into_iter().unzip();

        // Check the length of the child arrays
        let length = field_values[0].len();
        for i in 1..field_values.len() {
            assert_eq!(
                length,
                field_values[i].len(),
                "all child arrays of a StructArray must have the same length"
            );
        }

        let data = ArrayData::builder(DataType::Struct(field_types))
            .child_data(field_values.into_iter().map(|a| a.data()).collect())
            .build();
        Self::from(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_data::ArrayData;
    use crate::buffer::Buffer;
    use crate::datatypes::{DataType, Field, ToByteSlice};
    use crate::memory;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_primitive_array_from_vec() {
        let buf = Buffer::from(&[0, 1, 2, 3, 4].to_byte_slice());
        let buf2 = buf.clone();
        let arr = Int32Array::new(5, buf, 0, 0);
        let slice = unsafe { ::std::slice::from_raw_parts(arr.raw_values(), 5) };
        assert_eq!(buf2, arr.values());
        assert_eq!(&[0, 1, 2, 3, 4], slice);
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i32, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_from_vec_option() {
        // Test building a primitive array with null values
        let arr = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(2, arr.null_count());
        for i in 0..5 {
            if i % 2 == 0 {
                assert!(!arr.is_null(i));
                assert!(arr.is_valid(i));
                assert_eq!(i as i32, arr.value(i));
            } else {
                assert!(arr.is_null(i));
                assert!(!arr.is_valid(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder() {
        // Test building an primitive array with ArrayData builder and offset
        let buf = Buffer::from(&[0, 1, 2, 3, 4].to_byte_slice());
        let buf2 = buf.clone();
        let data = ArrayData::builder(DataType::Int32)
            .len(5)
            .offset(2)
            .add_buffer(buf)
            .build();
        let arr = Int32Array::from(data);
        assert_eq!(buf2, arr.values());
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.null_count());
        for i in 0..3 {
            assert_eq!((i + 2) as i32, arr.value(i));
        }
    }

    #[test]
    #[should_panic(
        expected = "PrimitiveArray data should contain a single buffer only (values buffer)"
    )]
    fn test_primitive_array_invalid_buffer_len() {
        let data = ArrayData::builder(DataType::Int32).len(5).build();
        Int32Array::from(data);
    }

    #[test]
    fn test_boolean_array_new() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let buf2 = buf.clone();
        let arr = BooleanArray::new(10, buf, 0, 0);
        assert_eq!(buf2, arr.values());
        assert_eq!(10, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..10 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i == 3 || i == 6 || i == 9, arr.value(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_array_from_vec() {
        let buf = Buffer::from([10_u8]);
        let arr = BooleanArray::from(vec![false, true, false, true]);
        assert_eq!(buf, arr.values());
        assert_eq!(4, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..4 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i == 1 || i == 3, arr.value(i), "failed at {}", i)
        }
    }

    #[test]
    fn test_boolean_array_from_vec_option() {
        let buf = Buffer::from([10_u8]);
        let arr = BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);
        assert_eq!(buf, arr.values());
        assert_eq!(4, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(1, arr.null_count());
        for i in 0..4 {
            if i == 2 {
                assert!(arr.is_null(i));
                assert!(!arr.is_valid(i));
            } else {
                assert!(!arr.is_null(i));
                assert!(arr.is_valid(i));
                assert_eq!(i == 1 || i == 3, arr.value(i), "failed at {}", i)
            }
        }
    }

    #[test]
    fn test_boolean_array_builder() {
        // Test building a boolean array with ArrayData builder and offset
        // 000011011
        let buf = Buffer::from([27_u8]);
        let buf2 = buf.clone();
        let data = ArrayData::builder(DataType::Boolean)
            .len(5)
            .offset(2)
            .add_buffer(buf)
            .build();
        let arr = BooleanArray::from(data);
        assert_eq!(buf2, arr.values());
        assert_eq!(5, arr.len());
        assert_eq!(2, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..3 {
            assert_eq!(i != 0, arr.value(i), "failed at {}", i);
        }
    }

    #[test]
    #[should_panic(
        expected = "PrimitiveArray data should contain a single buffer only (values buffer)"
    )]
    fn test_boolean_array_invalid_buffer_len() {
        let data = ArrayData::builder(DataType::Boolean).len(5).build();
        BooleanArray::from(data);
    }

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
        let list_data_type = DataType::List(Box::new(DataType::Int32));
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
    }

    #[test]
    #[should_panic(expected = "ListArray data should contain a single buffer only (value offsets)")]
    fn test_list_array_invalid_buffer_len() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_child_data(value_data)
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(expected = "ListArray should contain a single child array (values array)")]
    fn test_list_array_invalid_child_array_len() {
        let value_offsets = Buffer::from(&[0, 2, 5, 7].to_byte_slice());
        let list_data_type = DataType::List(Box::new(DataType::Int32));
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

        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(expected = "inconsistent offsets buffer and values array")]
    fn test_list_array_invalid_value_offset_end() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        let value_offsets = Buffer::from(&[0, 2, 5, 7].to_byte_slice());

        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        ListArray::from(list_data);
    }

    #[test]
    fn test_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        // Array data: ["hello", "", "parquet"]
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.get_value(0));
        assert_eq!("hello", binary_array.get_string(0));
        assert_eq!([] as [u8; 0], binary_array.get_value(1));
        assert_eq!("", binary_array.get_string(1));
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.get_value(2)
        );
        assert_eq!("parquet", binary_array.get_string(2));
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(7, binary_array.value_length(2));
        for i in 0..3 {
            assert!(binary_array.is_valid(i));
            assert!(!binary_array.is_null(i));
        }

        // Test binary array with offset
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .offset(1)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        assert_eq!(
            [b'p', b'a', b'r', b'q', b'u', b'e', b't'],
            binary_array.get_value(1)
        );
        assert_eq!("parquet", binary_array.get_string(1));
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
        let array_data1 = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array1 = BinaryArray::from(array_data1);

        let array_data2 = ArrayData::builder(DataType::Utf8)
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
            assert_eq!(binary_array1.get_value(i), binary_array2.get_value(i));
            assert_eq!(binary_array1.get_string(i), binary_array2.get_string(i));
            assert_eq!(binary_array1.value_offset(i), binary_array2.value_offset(i));
            assert_eq!(binary_array1.value_length(i), binary_array2.value_length(i));
        }
    }

    #[test]
    #[should_panic(
        expected = "BinaryArray can only be created from List<u8> arrays, mismatched data types."
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
        expected = "BinaryArray can only be created from list array of u8 values (i.e. List<PrimitiveArray<u8>>)."
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
    #[should_panic(expected = "BinaryArray out of bounds access")]
    fn test_binary_array_get_value_index_out_of_bound() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
        binary_array.get_value(4);
    }

    #[test]
    fn test_struct_array_builder() {
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(4)
            .add_buffer(Buffer::from([false, false, true, true].to_byte_slice()))
            .build();
        let int_data = ArrayData::builder(DataType::Int64)
            .len(4)
            .add_buffer(Buffer::from([42, 28, 19, 31].to_byte_slice()))
            .build();
        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, false));
        field_types.push(Field::new("b", DataType::Int64, false));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .build();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());
    }

    #[test]
    fn test_struct_array_from() {
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(4)
            .add_buffer(Buffer::from([12_u8]))
            .build();
        let int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .add_buffer(Buffer::from([42, 28, 19, 31].to_byte_slice()))
            .build();
        let struct_array = StructArray::from(vec![
            (
                Field::new("b", DataType::Boolean, false),
                Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<Array>,
            ),
            (
                Field::new("c", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
            ),
        ]);
        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());
    }

    #[test]
    #[should_panic(expected = "all child arrays of a StructArray must have the same length")]
    fn test_invalid_struct_child_array_lengths() {
        StructArray::from(vec![
            (
                Field::new("b", DataType::Float32, false),
                Arc::new(Float32Array::from(vec![1.1])) as Arc<Array>,
            ),
            (
                Field::new("c", DataType::Float64, false),
                Arc::new(Float64Array::from(vec![2.2, 3.3])),
            ),
        ]);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_primitive_array_alignment() {
        let ptr = memory::allocate_aligned(8).unwrap();
        let buf = Buffer::from_raw_parts(ptr, 8);
        let buf2 = buf.slice(1);
        let array_data = ArrayData::builder(DataType::Int32).add_buffer(buf2).build();
        Int32Array::from(array_data);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_list_array_alignment() {
        let ptr = memory::allocate_aligned(8).unwrap();
        let buf = Buffer::from_raw_parts(ptr, 8);
        let buf2 = buf.slice(1);

        let values: [i32; 8] = [0; 8];
        let value_data = ArrayData::builder(DataType::Int32)
            .add_buffer(Buffer::from(values.to_byte_slice()))
            .build();

        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .add_buffer(buf2)
            .add_child_data(value_data.clone())
            .build();
        ListArray::from(list_data);
    }

    #[test]
    #[should_panic(expected = "memory is not aligned")]
    fn test_binary_array_alignment() {
        let ptr = memory::allocate_aligned(8).unwrap();
        let buf = Buffer::from_raw_parts(ptr, 8);
        let buf2 = buf.slice(1);

        let values: [u8; 12] = [0; 12];

        let array_data = ArrayData::builder(DataType::Utf8)
            .add_buffer(buf2)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        BinaryArray::from(array_data);
    }

    #[test]
    fn test_access_array_concurrently() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let ret = thread::spawn(move || a.value(3)).join();

        assert!(ret.is_ok());
        assert_eq!(8, ret.ok().unwrap());
    }
}
