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

//! Defines a [`BufferBuilder`](crate::array::BufferBuilder) capable
//! of creating a [`Buffer`](crate::buffer::Buffer) which can be used
//! as an internal buffer in an [`ArrayData`](crate::array::ArrayData)
//! object.

use std::any::Any;
use std::collections::HashMap;
use std::io::Write;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use crate::array::*;
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

/// Builder for creating a [`Buffer`](crate::buffer::Buffer) object.
///
/// This builder is implemented for primitive types and creates a
/// buffer with a zero-copy `build()` method.
///
/// See trait [`BufferBuilderTrait`](crate::array::BufferBuilderTrait)
/// for further documentation and examples.
///
/// A [`Buffer`](crate::buffer::Buffer) is the underlying data
/// structure of Arrow's [`Arrays`](crate::array::Array).
///
/// For all supported types, there are type definitions for the
/// generic version of `BufferBuilder<T>`, e.g. `UInt8BufferBuilder`.
///
/// # Example:
///
/// ```
/// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
///
/// # fn main() -> arrow::error::Result<()> {
/// let mut builder = UInt8BufferBuilder::new(100);
/// builder.append_slice(&[42, 43, 44]);
/// builder.append(45);
/// let buffer = builder.finish();
///
/// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 43, 44, 45]);
/// # Ok(())
/// # }
/// ```
pub struct BufferBuilder<T: ArrowPrimitiveType> {
    buffer: MutableBuffer,
    len: usize,
    _marker: PhantomData<T>,
}

/// Trait for simplifying the construction of [`Buffers`](crate::buffer::Buffer).
///
/// This trait is used mainly to offer separate implementations for
/// numeric types and boolean types, while still be able to call methods on buffer builder
/// with generic primitive type.
/// Seperate implementations of this trait allow to add implementation-details,
/// e.g. the implementation for boolean types uses bit-packing.
pub trait BufferBuilderTrait<T: ArrowPrimitiveType> {
    /// Creates a new builder with initial capacity for _at least_ `capacity`
    /// elements of type `T`.
    ///
    /// The capacity can later be manually adjusted with the
    /// [`reserve()`](BufferBuilderTrait::reserve) method.
    /// Also the
    /// [`append()`](BufferBuilderTrait::append),
    /// [`append_slice()`](BufferBuilderTrait::append_slice) and
    /// [`advance()`](BufferBuilderTrait::advance)
    /// methods automatically increase the capacity if needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    ///
    /// assert!(builder.capacity() >= 10);
    /// ```
    fn new(capacity: usize) -> Self;

    /// Returns the current number of array elements in the internal buffer.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    fn len(&self) -> usize;

    /// Returns the actual capacity (number of elements) of the internal buffer.
    ///
    /// Note: the internal capacity returned by this method might be larger than
    /// what you'd expect after setting the capacity in the `new()` or `reserve()`
    /// functions.
    fn capacity(&self) -> usize;

    /// Increases the number of elements in the internal buffer by `n`
    /// and resizes the buffer as needed.
    ///
    /// The values of the newly added elements are undefined.
    /// This method is usually used when appending `NULL` values to the buffer
    /// as they still require physical memory space.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.advance(2);
    ///
    /// assert_eq!(builder.len(), 2);
    /// ```
    fn advance(&mut self, n: usize) -> Result<()>;

    /// Reserves memory for _at least_ `n` more elements of type `T`.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.reserve(10);
    ///
    /// assert!(builder.capacity() >= 20);
    /// ```
    fn reserve(&mut self, n: usize) -> Result<()>;

    /// Appends a value of type `T` into the builder,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append(42);
    ///
    /// assert_eq!(builder.len(), 1);
    /// ```
    fn append(&mut self, value: T::Native) -> Result<()>;

    /// Appends a value of type `T` into the builder N times,
    /// growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_n(10, 42);
    ///
    /// assert_eq!(builder.len(), 10);
    /// ```
    fn append_n(&mut self, n: usize, value: T::Native) -> Result<()>;

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// assert_eq!(builder.len(), 3);
    /// ```
    fn append_slice(&mut self, slice: &[T::Native]) -> Result<()>;

    /// Resets this builder and returns an immutable [`Buffer`](crate::buffer::Buffer).
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{UInt8BufferBuilder, BufferBuilderTrait};
    ///
    /// let mut builder = UInt8BufferBuilder::new(10);
    /// builder.append_slice(&[42, 44, 46]);
    ///
    /// let buffer = builder.finish();
    ///
    /// assert_eq!(unsafe { buffer.typed_data::<u8>() }, &[42, 44, 46]);
    /// ```
    fn finish(&mut self) -> Buffer;
}

impl<T: ArrowPrimitiveType> BufferBuilderTrait<T> for BufferBuilder<T> {
    default fn new(capacity: usize) -> Self {
        let buffer = MutableBuffer::new(capacity * mem::size_of::<T::Native>());
        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        let bit_capacity = self.buffer.capacity() * 8;
        bit_capacity / T::get_bit_width()
    }

    default fn advance(&mut self, i: usize) -> Result<()> {
        let new_buffer_len = (self.len + i) * mem::size_of::<T::Native>();
        self.buffer.resize(new_buffer_len)?;
        self.len += i;
        Ok(())
    }

    default fn reserve(&mut self, n: usize) -> Result<()> {
        let new_capacity = self.len + n;
        let byte_capacity = mem::size_of::<T::Native>() * new_capacity;
        self.buffer.reserve(byte_capacity)?;
        Ok(())
    }

    default fn append(&mut self, v: T::Native) -> Result<()> {
        self.reserve(1)?;
        self.write_bytes(v.to_byte_slice(), 1)
    }

    default fn append_n(&mut self, n: usize, v: T::Native) -> Result<()> {
        self.reserve(n)?;
        for _ in 0..n {
            self.write_bytes(v.to_byte_slice(), 1)?;
        }
        Ok(())
    }

    default fn append_slice(&mut self, slice: &[T::Native]) -> Result<()> {
        let array_slots = slice.len();
        self.reserve(array_slots)?;
        self.write_bytes(slice.to_byte_slice(), array_slots)
    }

    default fn finish(&mut self) -> Buffer {
        let buf = std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
        self.len = 0;
        buf.freeze()
    }
}

impl<T: ArrowPrimitiveType> BufferBuilder<T> {
    /// Writes a byte slice to the underlying buffer and updates the `len`, i.e. the
    /// number array elements in the builder.  Also, converts the `io::Result`
    /// required by the `Write` trait to the Arrow `Result` type.
    fn write_bytes(&mut self, bytes: &[u8], len_added: usize) -> Result<()> {
        let write_result = self.buffer.write(bytes);
        // `io::Result` has many options one of which we use, so pattern matching is
        // overkill here
        if write_result.is_err() {
            Err(ArrowError::MemoryError(
                "Could not write to Buffer, not big enough".to_string(),
            ))
        } else {
            self.len += len_added;
            Ok(())
        }
    }
}

impl BufferBuilderTrait<BooleanType> for BufferBuilder<BooleanType> {
    fn new(capacity: usize) -> Self {
        let byte_capacity = bit_util::ceil(capacity, 8);
        let actual_capacity = bit_util::round_upto_multiple_of_64(byte_capacity);
        let mut buffer = MutableBuffer::new(actual_capacity);
        buffer.set_null_bits(0, actual_capacity);
        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    fn advance(&mut self, i: usize) -> Result<()> {
        let new_buffer_len = bit_util::ceil(self.len + i, 8);
        self.buffer.resize(new_buffer_len)?;
        self.len += i;
        Ok(())
    }

    fn append(&mut self, v: bool) -> Result<()> {
        self.reserve(1)?;
        if v {
            // For performance the `len` of the buffer is not updated on each append but
            // is updated in the `freeze` method instead.
            unsafe {
                bit_util::set_bit_raw(self.buffer.raw_data_mut(), self.len);
            }
        }
        self.len += 1;
        Ok(())
    }

    fn append_n(&mut self, n: usize, v: bool) -> Result<()> {
        self.reserve(n)?;
        if v {
            unsafe {
                bit_util::set_bits_raw(self.buffer.raw_data_mut(), self.len, self.len + n)
            }
        }
        self.len += n;
        Ok(())
    }

    fn append_slice(&mut self, slice: &[bool]) -> Result<()> {
        self.reserve(slice.len())?;
        for v in slice {
            if *v {
                // For performance the `len` of the buffer is not
                // updated on each append but is updated in the
                // `freeze` method instead.
                unsafe {
                    bit_util::set_bit_raw(self.buffer.raw_data_mut(), self.len);
                }
            }
            self.len += 1;
        }
        Ok(())
    }

    fn reserve(&mut self, n: usize) -> Result<()> {
        let new_capacity = self.len + n;
        if new_capacity > self.capacity() {
            let new_byte_capacity = bit_util::ceil(new_capacity, 8);
            let existing_capacity = self.buffer.capacity();
            let new_capacity = self.buffer.reserve(new_byte_capacity)?;
            self.buffer
                .set_null_bits(existing_capacity, new_capacity - existing_capacity);
        }
        Ok(())
    }

    fn finish(&mut self) -> Buffer {
        // `append` does not update the buffer's `len` so do it before `freeze` is called.
        let new_buffer_len = bit_util::ceil(self.len, 8);
        debug_assert!(new_buffer_len >= self.buffer.len());
        let mut buf = std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
        self.len = 0;
        buf.resize(new_buffer_len).unwrap();
        buf.freeze()
    }
}

/// Trait for dealing with different array builders at runtime
pub trait ArrayBuilder: Any {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Builds the array
    fn finish(&mut self) -> ArrayRef;

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &Any;

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut Any;

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any>;
}

///  Array builder for fixed-width primitive types
pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    values_builder: BufferBuilder<T>,
    bitmap_builder: BooleanBufferBuilder,
}

impl<T: ArrowPrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.values_builder.len
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    /// Creates a new primitive array builder
    pub fn new(capacity: usize) -> Self {
        Self {
            values_builder: BufferBuilder::<T>::new(capacity),
            bitmap_builder: BooleanBufferBuilder::new(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.values_builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    pub fn append_value(&mut self, v: T::Native) -> Result<()> {
        self.bitmap_builder.append(true)?;
        self.values_builder.append(v)?;
        Ok(())
    }

    /// Appends a null slot into the builder
    pub fn append_null(&mut self) -> Result<()> {
        self.bitmap_builder.append(false)?;
        self.values_builder.advance(1)?;
        Ok(())
    }

    /// Appends an `Option<T>` into the builder
    pub fn append_option(&mut self, v: Option<T::Native>) -> Result<()> {
        match v {
            None => self.append_null()?,
            Some(v) => self.append_value(v)?,
        };
        Ok(())
    }

    /// Appends a slice of type `T` into the builder
    pub fn append_slice(&mut self, v: &[T::Native]) -> Result<()> {
        self.bitmap_builder.append_n(v.len(), true)?;
        self.values_builder.append_slice(v)?;
        Ok(())
    }

    /// Builds the `PrimitiveArray` and reset this builder.
    pub fn finish(&mut self) -> PrimitiveArray<T> {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - bit_util::count_set_bits(null_bit_buffer.data());
        let mut builder = ArrayData::builder(T::get_data_type())
            .len(len)
            .add_buffer(self.values_builder.finish());
        if null_count > 0 {
            builder = builder
                .null_count(null_count)
                .null_bit_buffer(null_bit_buffer);
        }
        let data = builder.build();
        PrimitiveArray::<T>::from(data)
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish_dict(&mut self, values: ArrayRef) -> DictionaryArray<T> {
        let len = self.len();
        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = len - bit_util::count_set_bits(null_bit_buffer.data());
        let data_type = DataType::Dictionary(
            Box::new(T::get_data_type()),
            Box::new(values.data_type().clone()),
        );
        let mut builder = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(self.values_builder.finish());
        if null_count > 0 {
            builder = builder
                .null_count(null_count)
                .null_bit_buffer(null_bit_buffer);
        }
        builder = builder.add_child_data(values.data());
        DictionaryArray::<T>::from(builder.build())
    }
}

///  Array builder for `ListArray`
pub struct ListBuilder<T: ArrayBuilder> {
    offsets_builder: Int32BufferBuilder,
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: usize,
}

impl<T: ArrayBuilder> ListBuilder<T> {
    /// Creates a new `ListArrayBuilder` from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = Int32BufferBuilder::new(capacity + 1);
        offsets_builder.append(0).unwrap();
        Self {
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len: 0,
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilder for ListBuilder<T>
where
    T: 'static,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.len
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrayBuilder> ListBuilder<T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    /// Finish the current variable-length list array slot
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.offsets_builder
            .append(self.values_builder.len() as i32)?;
        self.bitmap_builder.append(is_valid)?;
        self.len += 1;
        Ok(())
    }

    /// Builds the `ListArray` and reset this builder.
    pub fn finish(&mut self) -> ListArray {
        let len = self.len();
        self.len = 0;
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.bitmap_builder.finish();
        self.offsets_builder.append(0).unwrap();
        let data =
            ArrayData::builder(DataType::List(Box::new(values_data.data_type().clone())))
                .len(len)
                .null_count(len - bit_util::count_set_bits(null_bit_buffer.data()))
                .add_buffer(offset_buffer)
                .add_child_data(values_data)
                .null_bit_buffer(null_bit_buffer)
                .build();

        ListArray::from(data)
    }
}

///  Array builder for `ListArray`
pub struct FixedSizeListBuilder<T: ArrayBuilder> {
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: usize,
    list_len: i32,
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T> {
    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    pub fn new(values_builder: T, length: i32) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, length, capacity)
    }

    /// Creates a new `FixedSizeListBuilder` from a given values array builder
    /// `length` is the number of values within each array
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, length: i32, capacity: usize) -> Self {
        let mut offsets_builder = Int32BufferBuilder::new(capacity + 1);
        offsets_builder.append(0).unwrap();
        Self {
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len: 0,
            list_len: length,
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilder for FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.len
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    pub fn value_length(&self) -> i32 {
        self.list_len
    }

    /// Finish the current variable-length list array slot
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid)?;
        self.len += 1;
        Ok(())
    }

    /// Builds the `FixedSizeListBuilder` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeListArray {
        let len = self.len();
        self.len = 0;
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        // check that values_data length is multiple of len if we have data
        if len != 0 {
            assert!(
                values_data.len() / len == self.list_len as usize,
                "Values of FixedSizeList must have equal lengths, values have length {} and list has {}",
                values_data.len(),
                len
            );
        }

        let null_bit_buffer = self.bitmap_builder.finish();
        let data = ArrayData::builder(DataType::FixedSizeList(
            Box::new(values_data.data_type().clone()),
            self.list_len,
        ))
        .len(len)
        .null_count(len - bit_util::count_set_bits(null_bit_buffer.data()))
        .add_child_data(values_data)
        .null_bit_buffer(null_bit_buffer)
        .build();

        FixedSizeListArray::from(data)
    }
}

///  Array builder for `BinaryArray`
pub struct BinaryBuilder {
    builder: ListBuilder<UInt8Builder>,
}

pub struct StringBuilder {
    builder: ListBuilder<UInt8Builder>,
}

pub struct FixedSizeBinaryBuilder {
    builder: FixedSizeListBuilder<UInt8Builder>,
}

pub trait BinaryArrayBuilder: ArrayBuilder {}

impl BinaryArrayBuilder for BinaryBuilder {}
impl BinaryArrayBuilder for StringBuilder {}
impl BinaryArrayBuilder for FixedSizeBinaryBuilder {}

impl ArrayBuilder for BinaryBuilder {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for StringBuilder {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl ArrayBuilder for FixedSizeBinaryBuilder {
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl BinaryBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: ListBuilder::new(values_builder),
        }
    }

    /// Appends a single byte value into the builder's values array.
    ///
    /// Note, when appending individual byte values you must call `append` to delimit each
    /// distinct list value.
    pub fn append_byte(&mut self, value: u8) -> Result<()> {
        self.builder.values().append_value(value)?;
        Ok(())
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    pub fn append_value(&mut self, value: &[u8]) -> Result<()> {
        self.builder.values().append_slice(value)?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `BinaryArray` and reset this builder.
    pub fn finish(&mut self) -> BinaryArray {
        BinaryArray::from(self.builder.finish())
    }
}

impl StringBuilder {
    /// Creates a new `StringBuilder`,
    /// `capacity` is the number of bytes of string data to pre-allocate space for in this builder
    pub fn new(capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: ListBuilder::new(values_builder),
        }
    }

    /// Creates a new `StringBuilder`,
    /// `data_capacity` is the number of bytes of string data to pre-allocate space for in this builder
    /// `item_capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let values_builder = UInt8Builder::new(data_capacity);
        Self {
            builder: ListBuilder::with_capacity(values_builder, item_capacity),
        }
    }

    /// Appends a string into the builder.
    ///
    /// Automatically calls the `append` method to delimit the string appended in as a
    /// distinct array element.
    pub fn append_value(&mut self, value: &str) -> Result<()> {
        self.builder.values().append_slice(value.as_bytes())?;
        self.builder.append(true)?;
        Ok(())
    }

    /// Finish the current variable-length list array slot.
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.builder.append(is_valid)
    }

    /// Append a null value to the array.
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `StringArray` and reset this builder.
    pub fn finish(&mut self) -> StringArray {
        StringArray::from(self.builder.finish())
    }
}

impl FixedSizeBinaryBuilder {
    /// Creates a new `BinaryBuilder`, `capacity` is the number of bytes in the values
    /// array
    pub fn new(capacity: usize, byte_width: i32) -> Self {
        let values_builder = UInt8Builder::new(capacity);
        Self {
            builder: FixedSizeListBuilder::new(values_builder, byte_width),
        }
    }

    /// Appends a byte slice into the builder.
    ///
    /// Automatically calls the `append` method to delimit the slice appended in as a
    /// distinct array element.
    pub fn append_value(&mut self, value: &[u8]) -> Result<()> {
        assert_eq!(
            self.builder.value_length(),
            value.len() as i32,
            "Byte slice does not have the same length as FixedSizeBinaryBuilder value lengths"
        );
        self.builder.values().append_slice(value)?;
        self.builder.append(true)
    }

    /// Append a null value to the array.
    pub fn append_null(&mut self) -> Result<()> {
        let length: usize = self.builder.value_length() as usize;
        self.builder.values().append_slice(&vec![0u8; length][..])?;
        self.builder.append(false)
    }

    /// Builds the `FixedSizeBinaryArray` and reset this builder.
    pub fn finish(&mut self) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::from(self.builder.finish())
    }
}

/// Array builder for Struct types.
///
/// Note that callers should make sure that methods of all the child field builders are
/// properly called to maintain the consistency of the data structure.
pub struct StructBuilder {
    fields: Vec<Field>,
    field_anys: Vec<Box<Any>>,
    field_builders: Vec<Box<ArrayBuilder>>,
    bitmap_builder: BooleanBufferBuilder,
    len: usize,
}

impl ArrayBuilder for StructBuilder {
    /// Returns the number of array slots in the builder.
    ///
    /// Note that this always return the first child field builder's length, and it is
    /// the caller's responsibility to maintain the consistency that all the child field
    /// builder should have the equal number of elements.
    fn len(&self) -> usize {
        self.len
    }

    /// Builds the array.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }
}

impl StructBuilder {
    pub fn new(fields: Vec<Field>, builders: Vec<Box<ArrayBuilder>>) -> Self {
        let mut field_anys = Vec::with_capacity(builders.len());
        let mut field_builders = Vec::with_capacity(builders.len());

        // Create and maintain two references for each of the input builder. We need the
        // extra `Any` reference because we need to cast the builder to a specific type
        // in `field_builder()` by calling `downcast_mut`.
        for f in builders.into_iter() {
            let raw_f = Box::into_raw(f);
            let raw_f_copy = raw_f;
            unsafe {
                field_anys.push(Box::from_raw(raw_f).into_box_any());
                field_builders.push(Box::from_raw(raw_f_copy));
            }
        }

        Self {
            fields,
            field_anys,
            field_builders,
            bitmap_builder: BooleanBufferBuilder::new(0),
            len: 0,
        }
    }

    pub fn from_schema(schema: Schema, capacity: usize) -> Self {
        let fields = schema.fields();
        let mut builders = Vec::with_capacity(fields.len());
        for f in schema.fields() {
            builders.push(Self::from_field(f.clone(), capacity));
        }
        Self::new(schema.fields, builders)
    }

    fn from_field(f: Field, capacity: usize) -> Box<ArrayBuilder> {
        match f.data_type() {
            DataType::Boolean => Box::new(BooleanBuilder::new(capacity)),
            DataType::Int8 => Box::new(Int8Builder::new(capacity)),
            DataType::Int16 => Box::new(Int16Builder::new(capacity)),
            DataType::Int32 => Box::new(Int32Builder::new(capacity)),
            DataType::Int64 => Box::new(Int64Builder::new(capacity)),
            DataType::UInt8 => Box::new(UInt8Builder::new(capacity)),
            DataType::UInt16 => Box::new(UInt16Builder::new(capacity)),
            DataType::UInt32 => Box::new(UInt32Builder::new(capacity)),
            DataType::UInt64 => Box::new(UInt64Builder::new(capacity)),
            DataType::Float32 => Box::new(Float32Builder::new(capacity)),
            DataType::Float64 => Box::new(Float64Builder::new(capacity)),
            DataType::Binary => Box::new(BinaryBuilder::new(capacity)),
            DataType::FixedSizeBinary(len) => {
                Box::new(FixedSizeBinaryBuilder::new(capacity, *len))
            }
            DataType::Utf8 => Box::new(StringBuilder::new(capacity)),
            DataType::Date32(DateUnit::Day) => Box::new(Date32Builder::new(capacity)),
            DataType::Date64(DateUnit::Millisecond) => {
                Box::new(Date64Builder::new(capacity))
            }
            DataType::Time32(TimeUnit::Second) => {
                Box::new(Time32SecondBuilder::new(capacity))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Box::new(Time32MillisecondBuilder::new(capacity))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Box::new(Time64MicrosecondBuilder::new(capacity))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Box::new(Time64NanosecondBuilder::new(capacity))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Box::new(TimestampSecondBuilder::new(capacity))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Box::new(TimestampMillisecondBuilder::new(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(TimestampMicrosecondBuilder::new(capacity))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Box::new(TimestampNanosecondBuilder::new(capacity))
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Box::new(IntervalYearMonthBuilder::new(capacity))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Box::new(IntervalDayTimeBuilder::new(capacity))
            }
            DataType::Duration(TimeUnit::Second) => {
                Box::new(DurationSecondBuilder::new(capacity))
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Box::new(DurationMillisecondBuilder::new(capacity))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Box::new(DurationMicrosecondBuilder::new(capacity))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Box::new(DurationNanosecondBuilder::new(capacity))
            }
            DataType::Struct(fields) => {
                let schema = Schema::new(fields.clone());
                Box::new(Self::from_schema(schema, capacity))
            }
            t => panic!("Data type {:?} is not currently supported", t),
        }
    }

    /// Returns a mutable reference to the child field builder at index `i`.
    /// Result will be `None` if the input type `T` provided doesn't match the actual
    /// field builder's type.
    pub fn field_builder<T: ArrayBuilder>(&mut self, i: usize) -> Option<&mut T> {
        self.field_anys[i].downcast_mut::<T>()
    }

    /// Returns the number of fields for the struct this builder is building.
    pub fn num_fields(&self) -> usize {
        self.field_builders.len()
    }

    /// Appends an element (either null or non-null) to the struct. The actual elements
    /// should be appended for each child sub-array in a consistent way.
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid)?;
        self.len += 1;
        Ok(())
    }

    /// Appends a null element to the struct.
    pub fn append_null(&mut self) -> Result<()> {
        self.append(false)
    }

    /// Builds the `StructArray` and reset this builder.
    pub fn finish(&mut self) -> StructArray {
        let mut child_data = Vec::with_capacity(self.field_builders.len());
        for f in &mut self.field_builders {
            let arr = f.finish();
            child_data.push(arr.data());
        }

        let null_bit_buffer = self.bitmap_builder.finish();
        let null_count = self.len - bit_util::count_set_bits(null_bit_buffer.data());
        let mut builder = ArrayData::builder(DataType::Struct(self.fields.clone()))
            .len(self.len)
            .child_data(child_data);
        if null_count > 0 {
            builder = builder
                .null_count(null_count)
                .null_bit_buffer(null_bit_buffer);
        }

        self.len = 0;

        StructArray::from(builder.build())
    }
}

impl Drop for StructBuilder {
    fn drop(&mut self) {
        // To avoid double drop on the field array builders.
        let builders = std::mem::replace(&mut self.field_builders, Vec::new());
        std::mem::forget(builders);
    }
}

/// Array builder for `DictionaryArray`. For example to map a set of byte indices
/// to f32 values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
pub struct PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    keys_builder: PrimitiveBuilder<K>,
    values_builder: PrimitiveBuilder<V>,
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Creates a new `PrimitiveDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(
        keys_builder: PrimitiveBuilder<K>,
        values_builder: PrimitiveBuilder<V>,
    ) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }
}

impl<K, V> ArrayBuilder for PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Returns the builder as an non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.keys_builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<K, V> PrimitiveDictionaryBuilder<K, V>
where
    K: ArrowPrimitiveType,
    V: ArrowPrimitiveType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    pub fn append(&mut self, value: V::Native) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.to_byte_slice()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value)?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.to_byte_slice().into(), key);
            Ok(key)
        }
    }

    pub fn append_null(&mut self) -> Result<()> {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let value_ref: ArrayRef = Arc::new(self.values_builder.finish());
        self.keys_builder.finish_dict(value_ref)
    }
}

/// Array builder for `DictionaryArray`. For example to map a set of byte indices
/// to f32 values. Note that the use of a `HashMap` here will not scale to very large
/// arrays or result in an ordered dictionary.
pub struct StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    keys_builder: PrimitiveBuilder<K>,
    values_builder: StringBuilder,
    map: HashMap<Box<[u8]>, K::Native>,
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Creates a new `StringDictionaryBuilder` from a keys builder and a value builder.
    pub fn new(keys_builder: PrimitiveBuilder<K>, values_builder: StringBuilder) -> Self {
        Self {
            keys_builder,
            values_builder,
            map: HashMap::new(),
        }
    }
}

impl<K> ArrayBuilder for StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Returns the builder as an non-mutable `Any` reference.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.keys_builder.len()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<K> StringDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    /// Append a primitive value to the array. Return an existing index
    /// if already present in the values array or a new index if the
    /// value is appended to the values array.
    pub fn append(&mut self, value: &str) -> Result<K::Native> {
        if let Some(&key) = self.map.get(value.as_bytes()) {
            // Append existing value.
            self.keys_builder.append_value(key)?;
            Ok(key)
        } else {
            // Append new value.
            let key = K::Native::from_usize(self.values_builder.len())
                .ok_or(ArrowError::DictionaryKeyOverflowError)?;
            self.values_builder.append_value(value)?;
            self.keys_builder.append_value(key as K::Native)?;
            self.map.insert(value.as_bytes().into(), key);
            Ok(key)
        }
    }

    pub fn append_null(&mut self) -> Result<()> {
        self.keys_builder.append_null()
    }

    /// Builds the `DictionaryArray` and reset this builder.
    pub fn finish(&mut self) -> DictionaryArray<K> {
        self.map.clear();
        let value_ref: ArrayRef = Arc::new(self.values_builder.finish());
        self.keys_builder.finish_dict(value_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::Array;
    use crate::bitmap::Bitmap;

    #[test]
    fn test_builder_i32_empty() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(0, b.len());
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_builder_i32_alloc_zero_bytes() {
        let mut b = Int32BufferBuilder::new(0);
        b.append(123).unwrap();
        let a = b.finish();
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_builder_i32() {
        let mut b = Int32BufferBuilder::new(5);
        for i in 0..5 {
            b.append(i).unwrap();
        }
        assert_eq!(16, b.capacity());
        let a = b.finish();
        assert_eq!(20, a.len());
    }

    #[test]
    fn test_builder_i32_grow_buffer() {
        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        for i in 0..20 {
            b.append(i).unwrap();
        }
        assert_eq!(32, b.capacity());
        let a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_builder_finish() {
        let mut b = Int32BufferBuilder::new(5);
        assert_eq!(16, b.capacity());
        for i in 0..10 {
            b.append(i).unwrap();
        }
        let mut a = b.finish();
        assert_eq!(40, a.len());
        assert_eq!(0, b.len());
        assert_eq!(0, b.capacity());

        // Try build another buffer after cleaning up.
        for i in 0..20 {
            b.append(i).unwrap()
        }
        assert_eq!(32, b.capacity());
        a = b.finish();
        assert_eq!(80, a.len());
    }

    #[test]
    fn test_reserve() {
        let mut b = UInt8BufferBuilder::new(2);
        assert_eq!(64, b.capacity());
        b.reserve(64).unwrap();
        assert_eq!(64, b.capacity());
        b.reserve(65).unwrap();
        assert_eq!(128, b.capacity());

        let mut b = Int32BufferBuilder::new(2);
        assert_eq!(16, b.capacity());
        b.reserve(16).unwrap();
        assert_eq!(16, b.capacity());
        b.reserve(17).unwrap();
        assert_eq!(32, b.capacity());
    }

    #[test]
    fn test_append_slice() {
        let mut b = UInt8BufferBuilder::new(0);
        b.append_slice("Hello, ".as_bytes()).unwrap();
        b.append_slice("World!".as_bytes()).unwrap();
        let buffer = b.finish();
        assert_eq!(13, buffer.len());

        let mut b = Int32BufferBuilder::new(0);
        b.append_slice(&[32, 54]).unwrap();
        let buffer = b.finish();
        assert_eq!(8, buffer.len());
    }

    #[test]
    fn test_write_bytes() {
        let mut b = BooleanBufferBuilder::new(4);
        b.append(false).unwrap();
        b.append(true).unwrap();
        b.append(false).unwrap();
        b.append(true).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());

        let mut b = BooleanBufferBuilder::new(4);
        b.append_slice(&[false, true, false, true]).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());
    }

    #[test]
    fn test_write_bytes_i32() {
        let mut b = Int32BufferBuilder::new(4);
        let bytes = [8, 16, 32, 64].to_byte_slice();
        b.write_bytes(bytes, 4).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(16, b.capacity());
        let buffer = b.finish();
        assert_eq!(16, buffer.len());
    }

    #[test]
    #[should_panic(expected = "Could not write to Buffer, not big enough")]
    fn test_write_too_many_bytes() {
        let mut b = Int32BufferBuilder::new(0);
        let bytes = [8, 16, 32, 64].to_byte_slice();
        b.write_bytes(bytes, 4).unwrap();
    }

    #[test]
    fn test_boolean_array_builder_append_slice() {
        let arr1 =
            BooleanArray::from(vec![Some(true), Some(false), None, None, Some(false)]);

        let mut builder = BooleanArray::builder(0);
        builder.append_slice(&[true, false]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(false).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_boolean_builder_increases_buffer_len() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BooleanBufferBuilder::new(8);

        for i in 0..10 {
            if i == 3 || i == 6 || i == 9 {
                builder.append(true).unwrap();
            } else {
                builder.append(false).unwrap();
            }
        }
        let buf2 = builder.finish();

        assert_eq!(buf.len(), buf2.len());
        assert_eq!(buf.data(), buf2.data());
    }

    #[test]
    fn test_primitive_array_builder_i32() {
        let mut builder = Int32Array::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
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
    fn test_primitive_array_builder_date32() {
        let mut builder = Date32Array::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
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
    fn test_primitive_array_builder_timestamp_second() {
        let mut builder = TimestampSecondArray::builder(5);
        for i in 0..5 {
            builder.append_value(i).unwrap();
        }
        let arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, arr.offset());
        assert_eq!(0, arr.null_count());
        for i in 0..5 {
            assert!(!arr.is_null(i));
            assert!(arr.is_valid(i));
            assert_eq!(i as i64, arr.value(i));
        }
    }

    #[test]
    fn test_primitive_array_builder_bool() {
        // 00000010 01001000
        let buf = Buffer::from([72_u8, 2_u8]);
        let mut builder = BooleanArray::builder(10);
        for i in 0..10 {
            if i == 3 || i == 6 || i == 9 {
                builder.append_value(true).unwrap();
            } else {
                builder.append_value(false).unwrap();
            }
        }

        let arr = builder.finish();
        assert_eq!(buf, arr.values());
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
    fn test_primitive_array_builder_append_option() {
        let arr1 = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_option(Some(0)).unwrap();
        builder.append_option(None).unwrap();
        builder.append_option(Some(2)).unwrap();
        builder.append_option(None).unwrap();
        builder.append_option(Some(4)).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_append_null() {
        let arr1 = Int32Array::from(vec![Some(0), Some(2), None, None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_value(0).unwrap();
        builder.append_value(2).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(4).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_append_slice() {
        let arr1 = Int32Array::from(vec![Some(0), Some(2), None, None, Some(4)]);

        let mut builder = Int32Array::builder(5);
        builder.append_slice(&[0, 2]).unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(4).unwrap();
        let arr2 = builder.finish();

        assert_eq!(arr1.len(), arr2.len());
        assert_eq!(arr1.offset(), arr2.offset());
        assert_eq!(arr1.null_count(), arr2.null_count());
        for i in 0..5 {
            assert_eq!(arr1.is_null(i), arr2.is_null(i));
            assert_eq!(arr1.is_valid(i), arr2.is_valid(i));
            if arr1.is_valid(i) {
                assert_eq!(arr1.value(i), arr2.value(i));
            }
        }
    }

    #[test]
    fn test_primitive_array_builder_finish() {
        let mut builder = Int32Builder::new(5);
        builder.append_slice(&[2, 4, 6, 8]).unwrap();
        let mut arr = builder.finish();
        assert_eq!(4, arr.len());
        assert_eq!(0, builder.len());

        builder.append_slice(&[1, 3, 5, 7, 9]).unwrap();
        arr = builder.finish();
        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_value(4).unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        let values = list_array.values().data().buffers()[0].clone();
        assert_eq!(
            Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()),
            values
        );
        assert_eq!(
            Buffer::from(&[0, 3, 6, 8].to_byte_slice()),
            list_array.data().buffers()[0].clone()
        );
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(2, list_array.value_length(2));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }
    }

    #[test]
    fn test_list_array_builder_nulls() {
        let values_builder = Int32Builder::new(10);
        let mut builder = ListBuilder::new(values_builder);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(3, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length(2));
    }

    #[test]
    fn test_fixed_size_list_array_builder() {
        let values_builder = Int32Builder::new(10);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7, null]]
        builder.values().append_value(0).unwrap();
        builder.values().append_value(1).unwrap();
        builder.values().append_value(2).unwrap();
        builder.append(true).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_null().unwrap();
        builder.append(false).unwrap();
        builder.values().append_value(3).unwrap();
        builder.values().append_null().unwrap();
        builder.values().append_value(5).unwrap();
        builder.append(true).unwrap();
        builder.values().append_value(6).unwrap();
        builder.values().append_value(7).unwrap();
        builder.values().append_null().unwrap();
        builder.append(true).unwrap();
        let list_array = builder.finish();

        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    fn test_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = ListBuilder::new(values_builder);

        builder.values().append_slice(&[1, 2, 3]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_slice(&[4, 5, 6]).unwrap();
        builder.append(true).unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.values().append_slice(&[7, 8, 9]).unwrap();
        builder.append(true).unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_fixed_size_list_array_builder_finish() {
        let values_builder = Int32Array::builder(5);
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        builder.values().append_slice(&[1, 2, 3]).unwrap();
        builder.append(true).unwrap();
        builder.values().append_slice(&[4, 5, 6]).unwrap();
        builder.append(true).unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.values().append_slice(&[7, 8, 9]).unwrap();
        builder.append(true).unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_list_list_array_builder() {
        let primitive_builder = Int32Builder::new(10);
        let values_builder = ListBuilder::new(primitive_builder);
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder.values().values().append_value(1).unwrap();
        builder.values().values().append_value(2).unwrap();
        builder.values().append(true).unwrap();
        builder.values().values().append_value(3).unwrap();
        builder.values().values().append_value(4).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.values().values().append_value(5).unwrap();
        builder.values().values().append_value(6).unwrap();
        builder.values().values().append_value(7).unwrap();
        builder.values().append(true).unwrap();
        builder.values().append(false).unwrap();
        builder.values().values().append_value(8).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.values().values().append_value(9).unwrap();
        builder.values().values().append_value(10).unwrap();
        builder.values().append(true).unwrap();
        builder.append(true).unwrap();

        let list_array = builder.finish();

        assert_eq!(4, list_array.len());
        assert_eq!(1, list_array.null_count());
        assert_eq!(
            Buffer::from(&[0, 2, 5, 5, 6].to_byte_slice()),
            list_array.data().buffers()[0].clone()
        );

        assert_eq!(6, list_array.values().data().len());
        assert_eq!(1, list_array.values().data().null_count());
        assert_eq!(
            Buffer::from(&[0, 2, 4, 7, 7, 8, 10].to_byte_slice()),
            list_array.values().data().buffers()[0].clone()
        );

        assert_eq!(10, list_array.values().data().child_data()[0].len());
        assert_eq!(0, list_array.values().data().child_data()[0].null_count());
        assert_eq!(
            Buffer::from(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].to_byte_slice()),
            list_array.values().data().child_data()[0].buffers()[0].clone()
        );
    }

    #[test]
    fn test_binary_array_builder() {
        let mut builder = BinaryBuilder::new(20);

        builder.append_byte(b'h').unwrap();
        builder.append_byte(b'e').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_byte(b'w').unwrap();
        builder.append_byte(b'o').unwrap();
        builder.append_byte(b'r').unwrap();
        builder.append_byte(b'l').unwrap();
        builder.append_byte(b'd').unwrap();
        builder.append(true).unwrap();

        let array = builder.finish();

        let binary_array = BinaryArray::from(array);

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_string_array_builder() {
        let mut builder = StringBuilder::new(20);

        builder.append_value("hello").unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let array = builder.finish();

        let string_array = StringArray::from(array);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offset(2));
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_fixed_size_binary_builder() {
        let mut builder = FixedSizeBinaryBuilder::new(15, 5);

        //  [b"hello", null, "arrow"]
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"arrow").unwrap();
        let fixed_size_binary_array: FixedSizeBinaryArray = builder.finish();

        assert_eq!(
            &DataType::FixedSizeBinary(5),
            fixed_size_binary_array.data_type()
        );
        assert_eq!(3, fixed_size_binary_array.len());
        assert_eq!(1, fixed_size_binary_array.null_count());
        assert_eq!(10, fixed_size_binary_array.value_offset(2));
        assert_eq!(5, fixed_size_binary_array.value_length());
    }

    #[test]
    fn test_string_array_builder_finish() {
        let mut builder = StringBuilder::new(10);

        builder.append_value("hello").unwrap();
        builder.append_value("world").unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.append_value("arrow").unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_string_array_builder_append_string() {
        let mut builder = StringBuilder::new(20);

        let var = "hello".to_owned();
        builder.append_value(&var).unwrap();
        builder.append(true).unwrap();
        builder.append_value("world").unwrap();

        let array = builder.finish();

        let string_array = StringArray::from(array);

        assert_eq!(3, string_array.len());
        assert_eq!(0, string_array.null_count());
        assert_eq!("hello", string_array.value(0));
        assert_eq!("", string_array.value(1));
        assert_eq!("world", string_array.value(2));
        assert_eq!(5, string_array.value_offset(2));
        assert_eq!(5, string_array.value_length(2));
    }

    #[test]
    fn test_struct_array_builder() {
        let string_builder = StringBuilder::new(4);
        let int_builder = Int32Builder::new(4);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Utf8, false));
        field_builders.push(Box::new(string_builder) as Box<ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert_eq!(2, builder.num_fields());

        let string_builder = builder
            .field_builder::<StringBuilder>(0)
            .expect("builder at field 0 should be string builder");
        string_builder.append_value("joe").unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_value("mark").unwrap();

        let int_builder = builder
            .field_builder::<Int32Builder>(1)
            .expect("builder at field 1 should be int builder");
        int_builder.append_value(1).unwrap();
        int_builder.append_value(2).unwrap();
        int_builder.append_null().unwrap();
        int_builder.append_value(4).unwrap();

        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_null().unwrap();
        builder.append(true).unwrap();

        let arr = builder.finish();

        let struct_data = arr.data();
        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(
            &Some(Bitmap::from(Buffer::from(&[11_u8]))),
            struct_data.null_bitmap()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_count(2)
            .null_bit_buffer(Buffer::from(&[9_u8]))
            .add_buffer(Buffer::from(&[0, 3, 3, 3, 7].to_byte_slice()))
            .add_buffer(Buffer::from("joemark".as_bytes()))
            .build();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_count(1)
            .null_bit_buffer(Buffer::from(&[11_u8]))
            .add_buffer(Buffer::from(&[1, 2, 0, 4].to_byte_slice()))
            .build();

        assert_eq!(expected_string_data, arr.column(0).data());

        // TODO: implement equality for ArrayData
        assert_eq!(expected_int_data.len(), arr.column(1).data().len());
        assert_eq!(
            expected_int_data.null_count(),
            arr.column(1).data().null_count()
        );
        assert_eq!(
            expected_int_data.null_bitmap(),
            arr.column(1).data().null_bitmap()
        );
        let expected_value_buf = expected_int_data.buffers()[0].clone();
        let actual_value_buf = arr.column(1).data().buffers()[0].clone();
        for i in 0..expected_int_data.len() {
            if !expected_int_data.is_null(i) {
                assert_eq!(
                    expected_value_buf.data()[i * 4..(i + 1) * 4],
                    actual_value_buf.data()[i * 4..(i + 1) * 4]
                );
            }
        }
    }

    #[test]
    fn test_struct_array_builder_finish() {
        let int_builder = Int32Builder::new(10);
        let bool_builder = BooleanBuilder::new(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<ArrayBuilder>);
        fields.push(Field::new("f2", DataType::Boolean, false));
        field_builders.push(Box::new(bool_builder) as Box<ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[
                false, true, false, true, false, true, false, true, false, true,
            ])
            .unwrap();

        // Append slot values - all are valid.
        for _ in 0..10 {
            assert!(builder.append(true).is_ok())
        }

        assert_eq!(10, builder.len());

        let arr = builder.finish();

        assert_eq!(10, arr.len());
        assert_eq!(0, builder.len());

        builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_slice(&[1, 3, 5, 7, 9])
            .unwrap();
        builder
            .field_builder::<BooleanBuilder>(1)
            .unwrap()
            .append_slice(&[false, true, false, true, false])
            .unwrap();

        // Append slot values - all are valid.
        for _ in 0..5 {
            assert!(builder.append(true).is_ok())
        }

        assert_eq!(5, builder.len());

        let arr = builder.finish();

        assert_eq!(5, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_struct_array_builder_from_schema() {
        let mut fields = Vec::new();
        fields.push(Field::new("f1", DataType::Float32, false));
        fields.push(Field::new("f2", DataType::Utf8, false));
        let mut sub_fields = Vec::new();
        sub_fields.push(Field::new("g1", DataType::Int32, false));
        sub_fields.push(Field::new("g2", DataType::Boolean, false));
        let struct_type = DataType::Struct(sub_fields);
        fields.push(Field::new("f3", struct_type, false));

        let mut builder = StructBuilder::from_schema(Schema::new(fields), 5);
        assert_eq!(3, builder.num_fields());
        assert!(builder.field_builder::<Float32Builder>(0).is_some());
        assert!(builder.field_builder::<StringBuilder>(1).is_some());
        assert!(builder.field_builder::<StructBuilder>(2).is_some());
    }

    #[test]
    #[should_panic(expected = "Data type List(Int64) is not currently supported")]
    fn test_struct_array_builder_from_schema_unsupported_type() {
        let mut fields = Vec::new();
        fields.push(Field::new("f1", DataType::Int16, false));
        let list_type = DataType::List(Box::new(DataType::Int64));
        fields.push(Field::new("f2", list_type, false));

        let _ = StructBuilder::from_schema(Schema::new(fields), 5);
    }

    #[test]
    fn test_struct_array_builder_field_builder_type_mismatch() {
        let int_builder = Int32Builder::new(10);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(Field::new("f1", DataType::Int32, false));
        field_builders.push(Box::new(int_builder) as Box<ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert!(builder.field_builder::<BinaryBuilder>(0).is_none());
    }

    #[test]
    fn test_primitive_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        let array = builder.finish();

        // Keys are strongly typed.
        let aks: Vec<_> = array.keys().collect();

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &UInt32Array = av.as_any().downcast_ref::<UInt32Array>().unwrap();
        let avs: &[u32] = ava.value_slice(0, array.values().len());

        assert_eq!(array.is_null(0), false);
        assert_eq!(array.is_null(1), true);
        assert_eq!(array.is_null(2), false);

        assert_eq!(aks, vec![Some(0), None, Some(1)]);
        assert_eq!(avs, &[12345678, 22345678]);
    }

    #[test]
    fn test_string_dictionary_builder() {
        let key_builder = PrimitiveBuilder::<Int8Type>::new(5);
        let value_builder = StringBuilder::new(2);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        builder.append("abc").unwrap();
        builder.append_null().unwrap();
        builder.append("def").unwrap();
        builder.append("def").unwrap();
        builder.append("abc").unwrap();
        let array = builder.finish();

        // Keys are strongly typed.
        let aks: Vec<_> = array.keys().collect();

        // Values are polymorphic and so require a downcast.
        let av = array.values();
        let ava: &StringArray = av.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(aks, vec![Some(0), None, Some(1), Some(1), Some(0)]);
        assert_eq!(ava.value(0), "abc");
        assert_eq!(ava.value(1), "def");
    }

    #[test]
    fn test_primitive_dictionary_overflow() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(257);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(257);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        // 256 unique keys.
        for i in 0..256 {
            builder.append(i + 1000).unwrap();
        }
        // Special error if the key overflows (256th entry)
        assert_eq!(
            builder.append(1257),
            Err(ArrowError::DictionaryKeyOverflowError)
        );
    }
}
