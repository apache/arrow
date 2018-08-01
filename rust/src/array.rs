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
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use super::array_data::*;
use super::buffer::*;
use super::datatypes::*;
use util::bit_util;

/// Trait for dealing with different types of array at runtime when the type of the
/// array is not known in advance
pub trait Array: Send + Sync {
    /// Returns the array as Any so that it can be downcast to a specific implementation
    fn as_any(&self) -> &Any;

    /// Returns a reference-counted pointer to the data of this array
    fn data(&self) -> ArrayDataRef;

    /// Returns a borrowed & reference-counted pointer to the data of this array
    fn data_ref(&self) -> &ArrayDataRef;
}

pub type ArrayRef = Arc<Array>;

/// Constructs an array using the input `data`. Returns a reference-counted `Array`
/// instance.
fn make_array(data: ArrayDataRef) -> ArrayRef {
    // TODO: refactor the DataType enum and remove the clone here.
    match data.data_type() {
        DataType::Boolean => Arc::new(PrimitiveArray::<bool>::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(PrimitiveArray::<i8>::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(PrimitiveArray::<i16>::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(PrimitiveArray::<i32>::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(PrimitiveArray::<i64>::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(PrimitiveArray::<u8>::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(PrimitiveArray::<u16>::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(PrimitiveArray::<u32>::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(PrimitiveArray::<u64>::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(PrimitiveArray::<f32>::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(PrimitiveArray::<f64>::from(data)) as ArrayRef,
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
        Self { inner: inner }
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
    raw_values: RawPtrBox<u8>,
    _phantom: PhantomData<T>,
}

/// Macro to define primitive arrays for different data types and native types.
macro_rules! def_primitive_array {
    ($data_ty:path, $native_ty:ident) => {
        impl PrimitiveArray<$native_ty> {
            pub fn new(length: i64, values: Buffer, null_count: i64, offset: i64) -> Self {
                let array_data = ArrayData::builder($data_ty)
                    .length(length)
                    .add_buffer(values)
                    .null_count(null_count)
                    .offset(offset)
                    .build();
                PrimitiveArray::from(array_data)
            }

            /// Returns a `Buffer` holds all the values of this array.
            pub fn values(&self) -> Buffer {
                self.data.buffers()[0].copy()
            }

            /// Returns a raw pointer to the values of this array.
            pub fn raw_values(&self) -> *const $native_ty {
                unsafe { mem::transmute(self.raw_values.get().offset(self.data.offset() as isize)) }
            }

            /// Returns the primitive value at index `i`.
            pub fn value(&self, i: i64) -> $native_ty {
                unsafe { *(self.raw_values().offset(i as isize)) }
            }

            /// Determine the minimum value in the array
            pub fn min(&self) -> Option<$native_ty> {
                self.search(|a, b| a < b)
            }

            /// Determine the maximum value in the array
            pub fn max(&self) -> Option<$native_ty> {
                self.search(|a, b| a > b)
            }

            fn search<F>(&self, cmp: F) -> Option<$native_ty>
            where
                F: Fn($native_ty, $native_ty) -> bool,
            {
                let mut n: Option<$native_ty> = None;
                let data = self.data();
                for i in 0..data.length() {
                    if data.is_null(i) {
                        continue;
                    }
                    let mut m = self.value(i as i64);
                    match n {
                        None => n = Some(m),
                        Some(nn) => if cmp(m, nn) {
                            n = Some(m)
                        },
                    }
                }
                n
            }
        }

        /// Construct a primitive array from a vector. Should only be used for testing.
        impl From<Vec<$native_ty>> for PrimitiveArray<$native_ty> {
            fn from(data: Vec<$native_ty>) -> Self {
                let array_data = ArrayData::builder($data_ty)
                    .length(data.len() as i64)
                    .add_buffer(Buffer::from(data.to_byte_slice()))
                    .build();
                PrimitiveArray::from(array_data)
            }
        }

        impl From<Vec<Option<$native_ty>>> for PrimitiveArray<$native_ty> {
            fn from(data: Vec<Option<$native_ty>>) -> Self {
                const TY_SIZE: usize = mem::size_of::<$native_ty>();
                const NULL: [u8; TY_SIZE] = [0; TY_SIZE];

                let data_len = data.len() as i64;
                let size = bit_util::round_upto_multiple_of_64(data_len) as usize;
                let mut null_buffer = Vec::with_capacity(size);
                unsafe {
                    null_buffer.set_len(size);
                }
                let mut value_buffer: Vec<u8> = Vec::with_capacity(size * TY_SIZE);

                let mut i = 0;
                for n in data {
                    if let Some(v) = n {
                        bit_util::set_bit(&mut null_buffer[..], i);
                        value_buffer.extend_from_slice(&v.to_byte_slice());
                    } else {
                        value_buffer.extend_from_slice(&NULL);
                    }
                    i += 1;
                }

                let array_data = ArrayData::builder($data_ty)
                    .length(data_len)
                    .add_buffer(Buffer::from(Buffer::from(value_buffer)))
                    .null_bit_buffer(Buffer::from(null_buffer))
                    .build();
                PrimitiveArray::from(array_data)
            }
        }
    };
}

/// Constructs a `PrimitiveArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayDataRef> for PrimitiveArray<T> {
    fn from(data: ArrayDataRef) -> Self {
        assert!(data.buffers().len() == 1);
        let values = data.buffers()[0].copy();
        Self {
            data: data,
            raw_values: RawPtrBox::new(values.raw_data()),
            _phantom: PhantomData,
        }
    }
}

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

def_primitive_array!(DataType::Boolean, bool);
def_primitive_array!(DataType::UInt8, u8);
def_primitive_array!(DataType::UInt16, u16);
def_primitive_array!(DataType::UInt32, u32);
def_primitive_array!(DataType::UInt64, u64);
def_primitive_array!(DataType::Int8, i8);
def_primitive_array!(DataType::Int16, i16);
def_primitive_array!(DataType::Int32, i32);
def_primitive_array!(DataType::Int64, i64);
def_primitive_array!(DataType::Float32, f32);
def_primitive_array!(DataType::Float64, f64);

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

    /// Returns the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data().data_type()
    }

    /// Returns the offset for value at index `i`.
    #[inline]
    pub fn value_offset(&self, i: i64) -> i32 {
        self.value_offset_at(self.data.offset() + i)
    }

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, mut i: i64) -> i32 {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    #[inline]
    fn value_offset_at(&self, i: i64) -> i32 {
        unsafe { *self.value_offsets.get().offset(i as isize) }
    }
}

/// Constructs a `ListArray` from an array data reference.
impl From<ArrayDataRef> for ListArray {
    fn from(data: ArrayDataRef) -> Self {
        debug_assert!(data.buffers().len() == 1);
        debug_assert!(data.child_data().len() == 1);
        let values = make_array(data.child_data()[0].clone());
        let value_offsets = data.buffers()[0].raw_data() as *const u8 as *const i32;
        Self {
            data: data.clone(),
            values: values,
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
    pub fn get_value(&self, mut i: i64) -> &[u8] {
        i += self.data.offset();
        unsafe {
            let pos = self.value_offset_at(i);
            ::std::slice::from_raw_parts(
                self.value_data.get().offset(pos as isize),
                (self.value_offset_at(i + 1) - pos) as usize,
            )
        }
    }

    /// Returns the element at index `i` as a string.
    pub fn get_string(&self, i: i64) -> String {
        let slice = self.get_value(i);
        unsafe { String::from_utf8_unchecked(Vec::from(slice)) }
    }

    /// Returns the offset for the element at index `i`.
    #[inline]
    pub fn value_offset(&self, i: i64) -> i32 {
        self.value_offset_at(i)
    }

    /// Returns the length for the element at index `i`.
    #[inline]
    pub fn value_length(&self, mut i: i64) -> i32 {
        i += self.data.offset();
        self.value_offset_at(i + 1) - self.value_offset_at(i)
    }

    #[inline]
    fn value_offset_at(&self, i: i64) -> i32 {
        unsafe { *self.value_offsets.get().offset(i as isize) }
    }
}

impl From<ArrayDataRef> for BinaryArray {
    fn from(data: ArrayDataRef) -> Self {
        assert!(data.buffers().len() == 2);
        let value_offsets = data.buffers()[0].raw_data() as *const u8 as *const i32;
        let value_data = data.buffers()[1].raw_data();
        Self {
            data: data.clone(),
            value_offsets: RawPtrBox::new(value_offsets),
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
            .length(v.len() as i64)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        BinaryArray::from(array_data)
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
        Self {
            data: data,
            boxed_fields: boxed_fields,
        }
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
}

impl From<Vec<ArrayRef>> for StructArray {
    fn from(v: Vec<ArrayRef>) -> Self {
        let mut field_types = vec![];
        let mut count = 0;
        for arr in &v {
            let idx = count.to_string();
            count += 1;
            field_types.push(Field::new(&idx, arr.data().data_type(), false));
        }
        let mut builder = ArrayData::builder(DataType::Struct(field_types));
        for arr in v {
            builder = builder.add_child_data(arr.data().clone());
        }
        StructArray::from(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::{BinaryArray, ListArray, PrimitiveArray, StructArray};
    use array_data::ArrayData;
    use buffer::Buffer;
    use datatypes::{DataType, Field, ToByteSlice};

    #[test]
    fn test_primitive_array() {
        let values: Vec<i32> = vec![0, 1, 2, 3, 4];
        let buf = Buffer::from(&values[..].to_byte_slice());
        let buf2 = buf.copy();
        let pa = PrimitiveArray::<i32>::new(buf.len() as i64, buf, 0, 0);
        assert_eq!(buf2, pa.values());
        assert_eq!(2, pa.value(2));
        let raw_values = pa.raw_values();
        let slice = unsafe { ::std::slice::from_raw_parts(raw_values, 5) };
        assert_eq!(&[0, 1, 2, 3, 4], slice);
    }

    #[test]
    fn test_list_array() {
        // First, construct a value array.
        let values: Vec<i32> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let value_data = ArrayData::builder(DataType::Int32)
            .length(7)
            .add_buffer(Buffer::from(&values[..].to_byte_slice()))
            .build();

        // Then, build the list array.
        let value_offset_slice: Vec<i32> = vec![0, 2, 5, 7];
        let value_offsets = Buffer::from(value_offset_slice.to_byte_slice());
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type)
            .length(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(5, list_array.value_offset(2));
        assert_eq!(2, list_array.value_length(2));
    }

    #[test]
    fn test_binary_array() {
        let values: [u8; 12] = [
            b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
        ];
        let offsets: [i32; 4] = [0, 5, 5, 12];

        let array_data = ArrayData::builder(DataType::Utf8)
            .length(4)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let binary_array = BinaryArray::from(array_data);
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
    }

    #[test]
    fn test_struct_array() {
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .length(4)
            .add_buffer(Buffer::from([false, false, true, true].to_byte_slice()))
            .build();
        let int_data = ArrayData::builder(DataType::Int64)
            .length(4)
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
    fn test_buffer_array_min_max() {
        let a = PrimitiveArray::<i32>::from(vec![5, 6, 7, 8, 9]);
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

    #[test]
    fn test_buffer_array_min_max_with_nulls() {
        let a = PrimitiveArray::<i32>::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

    #[test]
    fn test_access_array_concurrently() {
        let a = PrimitiveArray::<i32>::from(vec![5, 6, 7, 8, 9]);

        let ret = thread::spawn(move || a.value(3)).join();

        assert!(ret.is_ok());
        assert_eq!(8, ret.ok().unwrap());
    }
}
