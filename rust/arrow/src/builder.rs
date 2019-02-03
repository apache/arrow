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

//! Defines a `BufferBuilder` capable of creating a `Buffer` which can be used as an
//! internal buffer in an `ArrayData` object.

use std::any::Any;
use std::io::Write;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use crate::array::*;
use crate::array_data::ArrayData;
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

/// Buffer builder with zero-copy build method
pub struct BufferBuilder<T: ArrowPrimitiveType> {
    buffer: MutableBuffer,
    len: usize,
    _marker: PhantomData<T>,
}

pub type BooleanBufferBuilder = BufferBuilder<BooleanType>;
pub type Int8BufferBuilder = BufferBuilder<Int8Type>;
pub type Int16BufferBuilder = BufferBuilder<Int16Type>;
pub type Int32BufferBuilder = BufferBuilder<Int32Type>;
pub type Int64BufferBuilder = BufferBuilder<Int64Type>;
pub type UInt8BufferBuilder = BufferBuilder<UInt8Type>;
pub type UInt16BufferBuilder = BufferBuilder<UInt16Type>;
pub type UInt32BufferBuilder = BufferBuilder<UInt32Type>;
pub type UInt64BufferBuilder = BufferBuilder<UInt64Type>;
pub type Float32BufferBuilder = BufferBuilder<Float32Type>;
pub type Float64BufferBuilder = BufferBuilder<Float64Type>;

// Trait for buffer builder. This is used mainly to offer separate implementations for
// numeric types and boolean types, while still be able to call methods on buffer builder
// with generic primitive type.
pub trait BufferBuilderTrait<T: ArrowPrimitiveType> {
    fn new(capacity: usize) -> Self;
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn advance(&mut self, i: usize) -> Result<()>;
    fn reserve(&mut self, n: usize) -> Result<()>;
    fn append(&mut self, v: T::Native) -> Result<()>;
    fn append_slice(&mut self, slice: &[T::Native]) -> Result<()>;
    fn finish(&mut self) -> Buffer;
}

impl<T: ArrowPrimitiveType> BufferBuilderTrait<T> for BufferBuilder<T> {
    /// Creates a builder with a fixed initial capacity
    default fn new(capacity: usize) -> Self {
        let buffer = MutableBuffer::new(capacity * mem::size_of::<T::Native>());
        Self {
            buffer,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// Returns the number of array elements (slots) in the builder
    fn len(&self) -> usize {
        self.len
    }

    /// Returns the current capacity of the builder (number of elements)
    fn capacity(&self) -> usize {
        let bit_capacity = self.buffer.capacity() * 8;
        (bit_capacity / T::get_bit_width())
    }

    // Advances the `len` of the underlying `Buffer` by `i` slots of type T
    default fn advance(&mut self, i: usize) -> Result<()> {
        let new_buffer_len = (self.len + i) * mem::size_of::<T::Native>();
        self.buffer.resize(new_buffer_len)?;
        self.len += i;
        Ok(())
    }

    /// Reserves memory for `n` elements of type `T`.
    default fn reserve(&mut self, n: usize) -> Result<()> {
        let new_capacity = self.len + n;
        let byte_capacity = mem::size_of::<T::Native>() * new_capacity;
        self.buffer.reserve(byte_capacity)?;
        Ok(())
    }

    /// Appends a value into the builder, growing the internal buffer as needed.
    default fn append(&mut self, v: T::Native) -> Result<()> {
        self.reserve(1)?;
        self.write_bytes(v.to_byte_slice(), 1)
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    default fn append_slice(&mut self, slice: &[T::Native]) -> Result<()> {
        let array_slots = slice.len();
        self.reserve(array_slots)?;
        self.write_bytes(slice.to_byte_slice(), array_slots)
    }

    /// Reset this builder and returns an immutable `Buffer`.
    default fn finish(&mut self) -> Buffer {
        let buf = ::std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
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
    /// Creates a builder with a fixed initial capacity.
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

    // Advances the `len` of the underlying `Buffer` by `i` slots of type T
    fn advance(&mut self, i: usize) -> Result<()> {
        let new_buffer_len = bit_util::ceil(self.len + i, 8);
        self.buffer.resize(new_buffer_len)?;
        self.len += i;
        Ok(())
    }

    /// Appends a value into the builder, growing the internal buffer as needed.
    fn append(&mut self, v: bool) -> Result<()> {
        self.reserve(1)?;
        if v {
            // For performance the `len` of the buffer is not updated on each append but
            // is updated in the `freeze` method instead.
            unsafe {
                bit_util::set_bit_raw(self.buffer.raw_data() as *mut u8, self.len);
            }
        }
        self.len += 1;
        Ok(())
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    fn append_slice(&mut self, slice: &[bool]) -> Result<()> {
        let array_slots = slice.len();
        for i in 0..array_slots {
            self.append(slice[i])?;
        }
        Ok(())
    }

    /// Reserves memory for `n` elements of type `T`.
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

    /// Reset this builder and returns an immutable `Buffer`.
    fn finish(&mut self) -> Buffer {
        // `append` does not update the buffer's `len` so do it before `freeze` is called.
        let new_buffer_len = bit_util::ceil(self.len, 8);
        debug_assert!(new_buffer_len >= self.buffer.len());
        let mut buf = ::std::mem::replace(&mut self.buffer, MutableBuffer::new(0));
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

    /// Returns the builder as an non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &Any;

    /// Returns the builder as an mutable `Any` reference.
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

pub type BooleanBuilder = PrimitiveBuilder<BooleanType>;
pub type Int8Builder = PrimitiveBuilder<Int8Type>;
pub type Int16Builder = PrimitiveBuilder<Int16Type>;
pub type Int32Builder = PrimitiveBuilder<Int32Type>;
pub type Int64Builder = PrimitiveBuilder<Int64Type>;
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;
pub type Float32Builder = PrimitiveBuilder<Float32Type>;
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

impl<T: ArrowPrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
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
        self.bitmap_builder.append_slice(&vec![true; v.len()][..])?;
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
        let mut offsets_builder = Int32BufferBuilder::new(values_builder.len() + 1);
        offsets_builder.append(0).unwrap();
        Self {
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(values_builder.len()),
            values_builder,
            len: 0,
        }
    }
}

impl<T: ArrayBuilder> ArrayBuilder for ListBuilder<T>
where
    T: 'static,
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

///  Array builder for `BinaryArray`
pub struct BinaryBuilder {
    builder: ListBuilder<UInt8Builder>,
}

impl ArrayBuilder for BinaryBuilder {
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
    pub fn append_value(&mut self, value: u8) -> Result<()> {
        self.builder.values().append_value(value)?;
        Ok(())
    }

    /// Appends a `&String` or `&str` into the builder.
    ///
    /// Automatically calls the `append` method to delimit the string appended in as a
    /// distinct array element.
    pub fn append_string(&mut self, value: &str) -> Result<()> {
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

    /// Builds the `BinaryArray` and reset this builder.
    pub fn finish(&mut self) -> BinaryArray {
        BinaryArray::from(self.builder.finish())
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

    /// Returns the builder as an non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &Any {
        self
    }

    /// Returns the builder as an mutable `Any` reference.
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
            DataType::Utf8 => Box::new(BinaryBuilder::new(capacity)),
            DataType::Struct(fields) => {
                let schema = Schema::new(fields.clone());
                Box::new(Self::from_schema(schema, capacity))
            }
            t @ _ => panic!("Data type {:?} is not currently supported", t),
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
        StructArray::from(builder.build())
    }
}

impl Drop for StructBuilder {
    fn drop(&mut self) {
        // To avoid double drop on the field array builders.
        let builders = ::std::mem::replace(&mut self.field_builders, Vec::new());
        ::std::mem::forget(builders);
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

        builder.append_value(b'h').unwrap();
        builder.append_value(b'e').unwrap();
        builder.append_value(b'l').unwrap();
        builder.append_value(b'l').unwrap();
        builder.append_value(b'o').unwrap();
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append_value(b'w').unwrap();
        builder.append_value(b'o').unwrap();
        builder.append_value(b'r').unwrap();
        builder.append_value(b'l').unwrap();
        builder.append_value(b'd').unwrap();
        builder.append(true).unwrap();

        let array = builder.finish();

        let binary_array = BinaryArray::from(array);

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!("hello", binary_array.get_string(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!("", binary_array.get_string(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!("world", binary_array.get_string(2));
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_binary_array_builder_finish() {
        let mut builder = BinaryBuilder::new(10);

        builder.append_string("hello").unwrap();
        builder.append_string("world").unwrap();

        let mut arr = builder.finish();
        assert_eq!(2, arr.len());
        assert_eq!(0, builder.len());

        builder.append_string("arrow").unwrap();
        arr = builder.finish();
        assert_eq!(1, arr.len());
        assert_eq!(0, builder.len());
    }

    #[test]
    fn test_binary_array_builder_append_string() {
        let mut builder = BinaryBuilder::new(20);

        let var = "hello".to_owned();
        builder.append_string(&var).unwrap();
        builder.append(true).unwrap();
        builder.append_string("world").unwrap();

        let array = builder.finish();

        let binary_array = BinaryArray::from(array);

        assert_eq!(3, binary_array.len());
        assert_eq!(0, binary_array.null_count());
        assert_eq!([b'h', b'e', b'l', b'l', b'o'], binary_array.value(0));
        assert_eq!("hello", binary_array.get_string(0));
        assert_eq!([] as [u8; 0], binary_array.value(1));
        assert_eq!("", binary_array.get_string(1));
        assert_eq!([b'w', b'o', b'r', b'l', b'd'], binary_array.value(2));
        assert_eq!("world", binary_array.get_string(2));
        assert_eq!(5, binary_array.value_offset(2));
        assert_eq!(5, binary_array.value_length(2));
    }

    #[test]
    fn test_struct_array_builder() {
        let string_builder = BinaryBuilder::new(4);
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
            .field_builder::<BinaryBuilder>(0)
            .expect("builder at field 0 should be binary builder");
        string_builder.append_string("joe").unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_null().unwrap();
        string_builder.append_string("mark").unwrap();

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
        assert!(builder.field_builder::<BinaryBuilder>(1).is_some());
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

}
