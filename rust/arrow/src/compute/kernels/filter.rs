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

//! Defines miscellaneous array kernels.

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;
use crate::{
    bitmap::Bitmap,
    buffer::{Buffer, MutableBuffer},
    util::bit_util,
};
use std::{mem, sync::Arc};

/// trait for copying filtered null bitmap bits
trait CopyNullBit {
    fn copy_null_bit(&mut self, source_index: usize);
    fn copy_null_bits(&mut self, source_index: usize, count: usize);
    fn null_count(&self) -> usize;
    fn null_buffer(&mut self) -> Buffer;
}

/// no-op null bitmap copy implementation,
/// used when the filtered data array doesn't have a null bitmap
struct NullBitNoop {}

impl NullBitNoop {
    fn new() -> Self {
        NullBitNoop {}
    }
}

impl CopyNullBit for NullBitNoop {
    #[inline]
    fn copy_null_bit(&mut self, _source_index: usize) {
        // do nothing
    }

    #[inline]
    fn copy_null_bits(&mut self, _source_index: usize, _count: usize) {
        // do nothing
    }

    fn null_count(&self) -> usize {
        0
    }

    fn null_buffer(&mut self) -> Buffer {
        Buffer::from([0u8; 0])
    }
}

/// null bitmap copy implementation,
/// used when the filtered data array has a null bitmap
struct NullBitSetter<'a> {
    target_buffer: MutableBuffer,
    source_bytes: &'a [u8],
    target_index: usize,
    null_count: usize,
}

impl<'a> NullBitSetter<'a> {
    fn new(null_bitmap: &'a Bitmap) -> Self {
        let null_bytes = null_bitmap.buffer_ref().data();
        // create null bitmap buffer with same length and initialize null bitmap buffer to 1s
        let null_buffer =
            MutableBuffer::new(null_bytes.len()).with_bitset(null_bytes.len(), true);
        NullBitSetter {
            source_bytes: null_bytes,
            target_buffer: null_buffer,
            target_index: 0,
            null_count: 0,
        }
    }
}

impl<'a> CopyNullBit for NullBitSetter<'a> {
    #[inline]
    fn copy_null_bit(&mut self, source_index: usize) {
        if !bit_util::get_bit(self.source_bytes, source_index) {
            bit_util::unset_bit(self.target_buffer.data_mut(), self.target_index);
            self.null_count += 1;
        }
        self.target_index += 1;
    }

    #[inline]
    fn copy_null_bits(&mut self, source_index: usize, count: usize) {
        for i in 0..count {
            self.copy_null_bit(source_index + i);
        }
    }

    fn null_count(&self) -> usize {
        self.null_count
    }

    fn null_buffer(&mut self) -> Buffer {
        self.target_buffer.resize(self.target_index);
        // use mem::replace to detach self.target_buffer from self so that it can be returned
        let target_buffer = mem::replace(&mut self.target_buffer, MutableBuffer::new(0));
        target_buffer.freeze()
    }
}

fn get_null_bit_setter<'a>(data_array: &'a impl Array) -> Box<CopyNullBit + 'a> {
    if let Some(null_bitmap) = data_array.data_ref().null_bitmap() {
        // only return an actual null bit copy implementation if null_bitmap is set
        Box::new(NullBitSetter::new(null_bitmap))
    } else {
        // otherwise return a no-op copy null bit implementation
        // for improved performance when the filtered array doesn't contain NULLs
        Box::new(NullBitNoop::new())
    }
}

// transmute filter array to u64
// - optimize filtering with highly selective filters by skipping entire batches of 64 filter bits
// - if the data array being filtered doesn't have a null bitmap, no time is wasted to copy a null bitmap
fn filter_array_impl(
    filter_context: &FilterContext,
    data_array: &impl Array,
    array_type: DataType,
    value_size: usize,
) -> Result<ArrayDataBuilder> {
    if filter_context.filter_len > data_array.len() {
        return Err(ArrowError::ComputeError(
            "Filter array cannot be larger than data array".to_string(),
        ));
    }
    let filtered_count = filter_context.filtered_count;
    let filter_mask = &filter_context.filter_mask;
    let filter_u64 = &filter_context.filter_u64;
    let data_bytes = data_array.data_ref().buffers()[0].data();
    let mut target_buffer = MutableBuffer::new(filtered_count * value_size);
    target_buffer.resize(filtered_count * value_size);
    let target_bytes = target_buffer.data_mut();
    let mut target_byte_index: usize = 0;
    let mut null_bit_setter = get_null_bit_setter(data_array);
    let null_bit_setter = null_bit_setter.as_mut();
    let all_ones_batch = !0u64;
    let data_array_offset = data_array.offset();

    for (i, filter_batch) in filter_u64.iter().enumerate() {
        // foreach u64 batch
        let filter_batch = *filter_batch;
        if filter_batch == 0 {
            // if batch == 0, all items are filtered out, so skip entire batch
            continue;
        } else if filter_batch == all_ones_batch {
            // if batch == all 1s: copy all 64 values in one go
            let data_index = (i * 64) + data_array_offset;
            null_bit_setter.copy_null_bits(data_index, 64);
            let data_byte_index = data_index * value_size;
            let data_len = value_size * 64;
            target_bytes[target_byte_index..(target_byte_index + data_len)]
                .copy_from_slice(
                    &data_bytes[data_byte_index..(data_byte_index + data_len)],
                );
            target_byte_index += data_len;
            continue;
        }
        for (j, filter_mask) in filter_mask.iter().enumerate() {
            // foreach bit in batch:
            if (filter_batch & *filter_mask) != 0 {
                let data_index = (i * 64) + j + data_array_offset;
                null_bit_setter.copy_null_bit(data_index);
                // if filter bit == 1: copy data value bytes
                let data_byte_index = data_index * value_size;
                target_bytes[target_byte_index..(target_byte_index + value_size)]
                    .copy_from_slice(
                        &data_bytes[data_byte_index..(data_byte_index + value_size)],
                    );
                target_byte_index += value_size;
            }
        }
    }

    let mut array_data_builder = ArrayDataBuilder::new(array_type)
        .len(filtered_count)
        .add_buffer(target_buffer.freeze());
    if null_bit_setter.null_count() > 0 {
        array_data_builder = array_data_builder
            .null_count(null_bit_setter.null_count())
            .null_bit_buffer(null_bit_setter.null_buffer());
    }

    Ok(array_data_builder)
}

/// FilterContext can be used to improve performance when
/// filtering multiple data arrays with the same filter array.
#[derive(Debug)]
pub struct FilterContext {
    filter_u64: Vec<u64>,
    filter_len: usize,
    filtered_count: usize,
    filter_mask: Vec<u64>,
}

macro_rules! filter_primitive_array {
    ($context:expr, $array:expr, $array_type:ident) => {{
        let input_array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let output_array = $context.filter_primitive_array(input_array)?;
        Ok(Arc::new(output_array))
    }};
}

macro_rules! filter_dictionary_array {
    ($context:expr, $array:expr, $array_type:ident) => {{
        let input_array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let output_array = $context.filter_dictionary_array(input_array)?;
        Ok(Arc::new(output_array))
    }};
}

macro_rules! filter_boolean_item_list_array {
    ($context:expr, $array:expr, $list_type:ident, $list_builder_type:ident) => {{
        let input_array = $array.as_any().downcast_ref::<$list_type>().unwrap();
        let values_builder = BooleanBuilder::new($context.filtered_count);
        let mut builder = $list_builder_type::new(values_builder);
        for i in 0..$context.filter_u64.len() {
            // foreach u64 batch
            let filter_batch = $context.filter_u64[i];
            if filter_batch == 0 {
                // if batch == 0, all items are filtered out, so skip entire batch
                continue;
            }
            for j in 0..64 {
                // foreach bit in batch:
                if (filter_batch & $context.filter_mask[j]) != 0 {
                    let data_index = (i * 64) + j;
                    if input_array.is_null(data_index) {
                        builder.append(false)?;
                    } else {
                        let this_inner_list = input_array.value(data_index);
                        let inner_list = this_inner_list
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap();
                        for k in 0..inner_list.len() {
                            if inner_list.is_null(k) {
                                builder.values().append_null()?;
                            } else {
                                builder.values().append_value(inner_list.value(k))?;
                            }
                        }
                        builder.append(true)?;
                    }
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! filter_primitive_item_list_array {
    ($context:expr, $array:expr, $item_type:ident, $list_type:ident, $list_builder_type:ident) => {{
        let input_array = $array.as_any().downcast_ref::<$list_type>().unwrap();
        let values_builder = PrimitiveBuilder::<$item_type>::new($context.filtered_count);
        let mut builder = $list_builder_type::new(values_builder);
        for i in 0..$context.filter_u64.len() {
            // foreach u64 batch
            let filter_batch = $context.filter_u64[i];
            if filter_batch == 0 {
                // if batch == 0, all items are filtered out, so skip entire batch
                continue;
            }
            for j in 0..64 {
                // foreach bit in batch:
                if (filter_batch & $context.filter_mask[j]) != 0 {
                    let data_index = (i * 64) + j;
                    if input_array.is_null(data_index) {
                        builder.append(false)?;
                    } else {
                        let this_inner_list = input_array.value(data_index);
                        let inner_list = this_inner_list
                            .as_any()
                            .downcast_ref::<PrimitiveArray<$item_type>>()
                            .unwrap();
                        for k in 0..inner_list.len() {
                            if inner_list.is_null(k) {
                                builder.values().append_null()?;
                            } else {
                                builder.values().append_value(inner_list.value(k))?;
                            }
                        }
                        builder.append(true)?;
                    }
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! filter_non_primitive_item_list_array {
    ($context:expr, $array:expr, $item_array_type:ident, $item_builder:ident, $list_type:ident, $list_builder_type:ident) => {{
        let input_array = $array.as_any().downcast_ref::<$list_type>().unwrap();
        let values_builder = $item_builder::new($context.filtered_count);
        let mut builder = $list_builder_type::new(values_builder);
        for i in 0..$context.filter_u64.len() {
            // foreach u64 batch
            let filter_batch = $context.filter_u64[i];
            if filter_batch == 0 {
                // if batch == 0, all items are filtered out, so skip entire batch
                continue;
            }
            for j in 0..64 {
                // foreach bit in batch:
                if (filter_batch & $context.filter_mask[j]) != 0 {
                    let data_index = (i * 64) + j;
                    if input_array.is_null(data_index) {
                        builder.append(false)?;
                    } else {
                        let this_inner_list = input_array.value(data_index);
                        let inner_list = this_inner_list
                            .as_any()
                            .downcast_ref::<$item_array_type>()
                            .unwrap();
                        for k in 0..inner_list.len() {
                            if inner_list.is_null(k) {
                                builder.values().append_null()?;
                            } else {
                                builder.values().append_value(inner_list.value(k))?;
                            }
                        }
                        builder.append(true)?;
                    }
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

impl FilterContext {
    /// Returns a new instance of FilterContext
    pub fn new(filter_array: &BooleanArray) -> Result<Self> {
        if filter_array.offset() > 0 {
            return Err(ArrowError::ComputeError(
                "Filter array cannot have offset > 0".to_string(),
            ));
        }
        let filter_mask: Vec<u64> = (0..64).map(|x| 1u64 << x).collect();
        let filter_buffer = &filter_array.data_ref().buffers()[0];
        let filtered_count = filter_buffer.count_set_bits_offset(0, filter_array.len());

        let filter_bytes = filter_buffer.data();

        // add to the resulting len so is is a multiple of the size of u64
        let pad_addional_len = 8 - filter_bytes.len() % 8;

        // transmute filter_bytes to &[u64]
        let mut u64_buffer = MutableBuffer::new(filter_bytes.len() + pad_addional_len);

        u64_buffer.extend_from_slice(filter_bytes);
        u64_buffer.extend_from_slice(&vec![0; pad_addional_len]);
        let mut filter_u64 = u64_buffer.typed_data_mut::<u64>().to_owned();

        // mask of any bits outside of the given len
        if filter_array.len() % 64 != 0 {
            let last_idx = filter_u64.len() - 1;
            let mask = u64::MAX >> (64 - filter_array.len() % 64);
            filter_u64[last_idx] &= mask;
        }

        Ok(FilterContext {
            filter_u64,
            filter_len: filter_array.len(),
            filtered_count,
            filter_mask,
        })
    }

    /// Returns a new array, containing only the elements matching the filter
    pub fn filter(&self, array: &Array) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::UInt8 => filter_primitive_array!(self, array, UInt8Array),
            DataType::UInt16 => filter_primitive_array!(self, array, UInt16Array),
            DataType::UInt32 => filter_primitive_array!(self, array, UInt32Array),
            DataType::UInt64 => filter_primitive_array!(self, array, UInt64Array),
            DataType::Int8 => filter_primitive_array!(self, array, Int8Array),
            DataType::Int16 => filter_primitive_array!(self, array, Int16Array),
            DataType::Int32 => filter_primitive_array!(self, array, Int32Array),
            DataType::Int64 => filter_primitive_array!(self, array, Int64Array),
            DataType::Float32 => filter_primitive_array!(self, array, Float32Array),
            DataType::Float64 => filter_primitive_array!(self, array, Float64Array),
            DataType::Boolean => {
                let input_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut builder = BooleanArray::builder(self.filtered_count);
                for i in 0..self.filter_u64.len() {
                    // foreach u64 batch
                    let filter_batch = self.filter_u64[i];
                    if filter_batch == 0 {
                        // if batch == 0, all items are filtered out, so skip entire batch
                        continue;
                    }
                    for j in 0..64 {
                        // foreach bit in batch:
                        if (filter_batch & self.filter_mask[j]) != 0 {
                            let data_index = (i * 64) + j;
                            if input_array.is_null(data_index) {
                                builder.append_null()?;
                            } else {
                                builder.append_value(input_array.value(data_index))?;
                            }
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            },
            DataType::Date32(_) => filter_primitive_array!(self, array, Date32Array),
            DataType::Date64(_) => filter_primitive_array!(self, array, Date64Array),
            DataType::Time32(TimeUnit::Second) => {
                filter_primitive_array!(self, array, Time32SecondArray)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                filter_primitive_array!(self, array, Time32MillisecondArray)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                filter_primitive_array!(self, array, Time64MicrosecondArray)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                filter_primitive_array!(self, array, Time64NanosecondArray)
            }
            DataType::Duration(TimeUnit::Second) => {
                filter_primitive_array!(self, array, DurationSecondArray)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                filter_primitive_array!(self, array, DurationMillisecondArray)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                filter_primitive_array!(self, array, DurationMicrosecondArray)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                filter_primitive_array!(self, array, DurationNanosecondArray)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                filter_primitive_array!(self, array, TimestampSecondArray)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                filter_primitive_array!(self, array, TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                filter_primitive_array!(self, array, TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                filter_primitive_array!(self, array, TimestampNanosecondArray)
            }
            DataType::Binary => {
                let input_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let mut values: Vec<Option<&[u8]>> = Vec::with_capacity(self.filtered_count);
                for i in 0..self.filter_u64.len() {
                    // foreach u64 batch
                    let filter_batch = self.filter_u64[i];
                    if filter_batch == 0 {
                        // if batch == 0, all items are filtered out, so skip entire batch
                        continue;
                    }
                    for j in 0..64 {
                        // foreach bit in batch:
                        if (filter_batch & self.filter_mask[j]) != 0 {
                            let data_index = (i * 64) + j;
                            if input_array.is_null(data_index) {
                                values.push(None)
                            } else {
                                values.push(Some(input_array.value(data_index)))
                            }
                        }
                    }
                }
                Ok(Arc::new(BinaryArray::from(values)))
            }
            DataType::Utf8 => {
                let input_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut values: Vec<Option<&str>> = Vec::with_capacity(self.filtered_count);
                for i in 0..self.filter_u64.len() {
                    // foreach u64 batch
                    let filter_batch = self.filter_u64[i];
                    if filter_batch == 0 {
                        // if batch == 0, all items are filtered out, so skip entire batch
                        continue;
                    }
                    for j in 0..64 {
                        // foreach bit in batch:
                        if (filter_batch & self.filter_mask[j]) != 0 {
                            let data_index = (i * 64) + j;
                            if input_array.is_null(data_index) {
                                values.push(None)
                            } else {
                                values.push(Some(input_array.value(data_index)))
                            }
                        }
                    }
                }
                Ok(Arc::new(StringArray::from(values)))
            }
            DataType::Dictionary(ref key_type, ref value_type) => match (key_type.as_ref(), value_type.as_ref()) {
                (key_type, DataType::Utf8) => match key_type {
                    DataType::UInt8 => filter_dictionary_array!(self, array, UInt8DictionaryArray),
                    DataType::UInt16 => filter_dictionary_array!(self, array, UInt16DictionaryArray),
                    DataType::UInt32 => filter_dictionary_array!(self, array, UInt32DictionaryArray),
                    DataType::UInt64 => filter_dictionary_array!(self, array, UInt64DictionaryArray),
                    DataType::Int8 => filter_dictionary_array!(self, array, Int8DictionaryArray),
                    DataType::Int16 => filter_dictionary_array!(self, array, Int16DictionaryArray),
                    DataType::Int32 => filter_dictionary_array!(self, array, Int32DictionaryArray),
                    DataType::Int64 => filter_dictionary_array!(self, array, Int64DictionaryArray),
                    other => Err(ArrowError::ComputeError(format!(
                        "filter not supported for string dictionary with key of type {:?}",
                        other
                    )))
                }
                (key_type, value_type) => Err(ArrowError::ComputeError(format!(
                    "filter not supported for Dictionary({:?}, {:?})",
                    key_type, value_type
                )))
            }
            DataType::List(dt) => match dt.data_type() {
                DataType::UInt8 => {
                    filter_primitive_item_list_array!(self, array, UInt8Type, ListArray, ListBuilder)
                }
                DataType::UInt16 => {
                    filter_primitive_item_list_array!(self, array, UInt16Type, ListArray, ListBuilder)
                }
                DataType::UInt32 => {
                    filter_primitive_item_list_array!(self, array, UInt32Type, ListArray, ListBuilder)
                }
                DataType::UInt64 => {
                    filter_primitive_item_list_array!(self, array, UInt64Type, ListArray, ListBuilder)
                }
                DataType::Int8 => filter_primitive_item_list_array!(self, array, Int8Type, ListArray, ListBuilder),
                DataType::Int16 => {
                    filter_primitive_item_list_array!(self, array, Int16Type, ListArray, ListBuilder)
                }
                DataType::Int32 => {
                    filter_primitive_item_list_array!(self, array, Int32Type, ListArray, ListBuilder)
                }
                DataType::Int64 => {
                    filter_primitive_item_list_array!(self, array, Int64Type, ListArray, ListBuilder)
                }
                DataType::Float32 => {
                    filter_primitive_item_list_array!(self, array, Float32Type, ListArray, ListBuilder)
                }
                DataType::Float64 => {
                    filter_primitive_item_list_array!(self, array, Float64Type, ListArray, ListBuilder)
                }
                DataType::Boolean => {
                    filter_boolean_item_list_array!(self, array, ListArray, ListBuilder)
                }
                DataType::Date32(_) => {
                    filter_primitive_item_list_array!(self, array, Date32Type, ListArray, ListBuilder)
                }
                DataType::Date64(_) => {
                    filter_primitive_item_list_array!(self, array, Date64Type, ListArray, ListBuilder)
                }
                DataType::Time32(TimeUnit::Second) => {
                    filter_primitive_item_list_array!(self, array, Time32SecondType, ListArray, ListBuilder)
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    filter_primitive_item_list_array!(self, array, Time32MillisecondType, ListArray, ListBuilder)
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    filter_primitive_item_list_array!(self, array, Time64MicrosecondType, ListArray, ListBuilder)
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    filter_primitive_item_list_array!(self, array, Time64NanosecondType, ListArray, ListBuilder)
                }
                DataType::Duration(TimeUnit::Second) => {
                    filter_primitive_item_list_array!(self, array, DurationSecondType, ListArray, ListBuilder)
                }
                DataType::Duration(TimeUnit::Millisecond) => {
                    filter_primitive_item_list_array!(self, array, DurationMillisecondType, ListArray, ListBuilder)
                }
                DataType::Duration(TimeUnit::Microsecond) => {
                    filter_primitive_item_list_array!(self, array, DurationMicrosecondType, ListArray, ListBuilder)
                }
                DataType::Duration(TimeUnit::Nanosecond) => {
                    filter_primitive_item_list_array!(self, array, DurationNanosecondType, ListArray, ListBuilder)
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampSecondType, ListArray, ListBuilder)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampMillisecondType, ListArray, ListBuilder)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampMicrosecondType, ListArray, ListBuilder)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampNanosecondType, ListArray, ListBuilder)
                }
                DataType::Binary => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    BinaryArray,
                    BinaryBuilder,
                    ListArray,
                    ListBuilder
                ),
                DataType::LargeBinary => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    LargeBinaryArray,
                    LargeBinaryBuilder,
                    ListArray,
                    ListBuilder
                ),
                DataType::Utf8 => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    StringArray,
                    StringBuilder,
                    ListArray
                    ,ListBuilder
                ),
                DataType::LargeUtf8 => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    LargeStringArray,
                    LargeStringBuilder,
                    ListArray,
                    ListBuilder
                ),
                other => {
                    Err(ArrowError::ComputeError(format!(
                        "filter not supported for List({:?})",
                        other
                    )))
                }
            }
            DataType::LargeList(dt) => match dt.data_type() {
                DataType::UInt8 => {
                    filter_primitive_item_list_array!(self, array, UInt8Type, LargeListArray, LargeListBuilder)
                }
                DataType::UInt16 => {
                    filter_primitive_item_list_array!(self, array, UInt16Type, LargeListArray, LargeListBuilder)
                }
                DataType::UInt32 => {
                    filter_primitive_item_list_array!(self, array, UInt32Type, LargeListArray, LargeListBuilder)
                }
                DataType::UInt64 => {
                    filter_primitive_item_list_array!(self, array, UInt64Type, LargeListArray, LargeListBuilder)
                }
                DataType::Int8 => filter_primitive_item_list_array!(self, array, Int8Type, LargeListArray, LargeListBuilder),
                DataType::Int16 => {
                    filter_primitive_item_list_array!(self, array, Int16Type, LargeListArray, LargeListBuilder)
                }
                DataType::Int32 => {
                    filter_primitive_item_list_array!(self, array, Int32Type, LargeListArray, LargeListBuilder)
                }
                DataType::Int64 => {
                    filter_primitive_item_list_array!(self, array, Int64Type, LargeListArray, LargeListBuilder)
                }
                DataType::Float32 => {
                    filter_primitive_item_list_array!(self, array, Float32Type, LargeListArray, LargeListBuilder)
                }
                DataType::Float64 => {
                    filter_primitive_item_list_array!(self, array, Float64Type, LargeListArray, LargeListBuilder)
                }
                DataType::Boolean => {
                    filter_boolean_item_list_array!(self, array, LargeListArray, LargeListBuilder)
                }
                DataType::Date32(_) => {
                    filter_primitive_item_list_array!(self, array, Date32Type, LargeListArray, LargeListBuilder)
                }
                DataType::Date64(_) => {
                    filter_primitive_item_list_array!(self, array, Date64Type, LargeListArray, LargeListBuilder)
                }
                DataType::Time32(TimeUnit::Second) => {
                    filter_primitive_item_list_array!(self, array, Time32SecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    filter_primitive_item_list_array!(self, array, Time32MillisecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    filter_primitive_item_list_array!(self, array, Time64MicrosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    filter_primitive_item_list_array!(self, array, Time64NanosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Duration(TimeUnit::Second) => {
                    filter_primitive_item_list_array!(self, array, DurationSecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Duration(TimeUnit::Millisecond) => {
                    filter_primitive_item_list_array!(self, array, DurationMillisecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Duration(TimeUnit::Microsecond) => {
                    filter_primitive_item_list_array!(self, array, DurationMicrosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Duration(TimeUnit::Nanosecond) => {
                    filter_primitive_item_list_array!(self, array, DurationNanosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampSecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampMillisecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampMicrosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    filter_primitive_item_list_array!(self, array, TimestampNanosecondType, LargeListArray, LargeListBuilder)
                }
                DataType::Binary => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    BinaryArray,
                    BinaryBuilder,
                    LargeListArray,
                    LargeListBuilder
                ),
                DataType::LargeBinary => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    LargeBinaryArray,
                    LargeBinaryBuilder,
                    LargeListArray,
                    LargeListBuilder
                ),
                DataType::Utf8 => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    StringArray,
                    StringBuilder,
                    LargeListArray,
                    LargeListBuilder
                ),
                DataType::LargeUtf8 => filter_non_primitive_item_list_array!(
                    self,
                    array,
                    LargeStringArray,
                    LargeStringBuilder,
                    LargeListArray,
                    LargeListBuilder
                ),
                other => {
                    Err(ArrowError::ComputeError(format!(
                        "filter not supported for LargeList({:?})",
                        other
                    )))
                }
            }
            other => Err(ArrowError::ComputeError(format!(
                "filter not supported for {:?}",
                other
            ))),
        }
    }

    /// Returns a new PrimitiveArray<T> containing only those values from the array passed as the data_array parameter,
    /// selected by the BooleanArray passed as the filter_array parameter
    pub fn filter_primitive_array<T>(
        &self,
        data_array: &PrimitiveArray<T>,
    ) -> Result<PrimitiveArray<T>>
    where
        T: ArrowNumericType,
    {
        let array_type = T::DATA_TYPE;
        let value_size = mem::size_of::<T::Native>();
        let array_data_builder =
            filter_array_impl(self, data_array, array_type, value_size)?;
        let data = array_data_builder.build();
        Ok(PrimitiveArray::<T>::from(data))
    }

    /// Returns a new DictionaryArray<T> containing only those keys from the array passed as the data_array parameter,
    /// selected by the BooleanArray passed as the filter_array parameter. The values are cloned from the data_array.
    pub fn filter_dictionary_array<T>(
        &self,
        data_array: &DictionaryArray<T>,
    ) -> Result<DictionaryArray<T>>
    where
        T: ArrowNumericType,
    {
        let array_type = data_array.data_type().clone();
        let value_size = mem::size_of::<T::Native>();
        let mut array_data_builder =
            filter_array_impl(self, data_array, array_type, value_size)?;
        // copy dictionary values from input array
        array_data_builder =
            array_data_builder.add_child_data(data_array.values().data());
        let data = array_data_builder.build();
        Ok(DictionaryArray::<T>::from(data))
    }
}

/// Returns a new array, containing only the elements matching the filter.
pub fn filter(array: &Array, filter: &BooleanArray) -> Result<ArrayRef> {
    FilterContext::new(filter)?.filter(array)
}

/// Returns a new PrimitiveArray<T> containing only those values from the array passed as the data_array parameter,
/// selected by the BooleanArray passed as the filter_array parameter
pub fn filter_primitive_array<T>(
    data_array: &PrimitiveArray<T>,
    filter_array: &BooleanArray,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
{
    FilterContext::new(filter_array)?.filter_primitive_array(data_array)
}

/// Returns a new DictionaryArray<T> containing only those keys from the array passed as the data_array parameter,
/// selected by the BooleanArray passed as the filter_array parameter. The values are cloned from the data_array.
pub fn filter_dictionary_array<T>(
    data_array: &DictionaryArray<T>,
    filter_array: &BooleanArray,
) -> Result<DictionaryArray<T>>
where
    T: ArrowNumericType,
{
    FilterContext::new(filter_array)?.filter_dictionary_array(data_array)
}

/// Returns a new RecordBatch with arrays containing only values matching the filter.
/// The same FilterContext is re-used when filtering arrays in the RecordBatch for better performance.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    filter_array: &BooleanArray,
) -> Result<RecordBatch> {
    let filter_context = FilterContext::new(filter_array)?;
    let filtered_arrays = record_batch
        .columns()
        .iter()
        .map(|a| filter_context.filter(a.as_ref()))
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(record_batch.schema(), filtered_arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Buffer;
    use crate::datatypes::ToByteSlice;

    macro_rules! def_temporal_test {
        ($test:ident, $array_type: ident, $data: expr) => {
            #[test]
            fn $test() {
                let a = $data;
                let b = BooleanArray::from(vec![true, false, true, false]);
                let c = filter(&a, &b).unwrap();
                let d = c.as_ref().as_any().downcast_ref::<$array_type>().unwrap();
                assert_eq!(2, d.len());
                assert_eq!(1, d.value(0));
                assert_eq!(3, d.value(1));
            }
        };
    }

    def_temporal_test!(
        test_filter_date32,
        Date32Array,
        Date32Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_date64,
        Date64Array,
        Date64Array::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_second,
        Time32SecondArray,
        Time32SecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time32_millisecond,
        Time32MillisecondArray,
        Time32MillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_microsecond,
        Time64MicrosecondArray,
        Time64MicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_time64_nanosecond,
        Time64NanosecondArray,
        Time64NanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_second,
        DurationSecondArray,
        DurationSecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_millisecond,
        DurationMillisecondArray,
        DurationMillisecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_microsecond,
        DurationMicrosecondArray,
        DurationMicrosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_duration_nanosecond,
        DurationNanosecondArray,
        DurationNanosecondArray::from(vec![1, 2, 3, 4])
    );
    def_temporal_test!(
        test_filter_timestamp_second,
        TimestampSecondArray,
        TimestampSecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_millisecond,
        TimestampMillisecondArray,
        TimestampMillisecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_microsecond,
        TimestampMicrosecondArray,
        TimestampMicrosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );
    def_temporal_test!(
        test_filter_timestamp_nanosecond,
        TimestampNanosecondArray,
        TimestampNanosecondArray::from_vec(vec![1, 2, 3, 4], None)
    );

    #[test]
    fn test_filter_array() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = BooleanArray::from(vec![true, false, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(5, d.value(0));
        assert_eq!(8, d.value(1));
    }

    #[test]
    fn test_filter_array_slice() {
        let a_slice = Int32Array::from(vec![5, 6, 7, 8, 9]).slice(1, 4);
        let a = a_slice.as_ref();
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(6, d.value(0));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_array_low_density() {
        // this test exercises the all 0's branch of the filter algorithm
        let mut data_values = (1..=65).collect::<Vec<i32>>();
        let mut filter_values =
            (1..=65).map(|i| matches!(i % 65, 0)).collect::<Vec<bool>>();
        // set up two more values after the batch
        data_values.extend_from_slice(&[66, 67]);
        filter_values.extend_from_slice(&[false, true]);
        let a = Int32Array::from(data_values);
        let b = BooleanArray::from(filter_values);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(65, d.value(0));
        assert_eq!(67, d.value(1));
    }

    #[test]
    fn test_filter_array_high_density() {
        // this test exercises the all 1's branch of the filter algorithm
        let mut data_values = (1..=65).map(Some).collect::<Vec<_>>();
        let mut filter_values = (1..=65)
            .map(|i| !matches!(i % 65, 0))
            .collect::<Vec<bool>>();
        // set second data value to null
        data_values[1] = None;
        // set up two more values after the batch
        data_values.extend_from_slice(&[Some(66), None, Some(67), None]);
        filter_values.extend_from_slice(&[false, true, true, true]);
        let a = Int32Array::from(data_values);
        let b = BooleanArray::from(filter_values);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(67, d.len());
        assert_eq!(3, d.null_count());
        assert_eq!(1, d.value(0));
        assert_eq!(true, d.is_null(1));
        assert_eq!(64, d.value(63));
        assert_eq!(true, d.is_null(64));
        assert_eq!(67, d.value(65));
    }

    #[test]
    fn test_filter_string_array() {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let b = BooleanArray::from(vec![true, false, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
    }

    #[test]
    fn test_filter_primative_array_with_null() {
        let a = Int32Array::from(vec![Some(5), None]);
        let b = BooleanArray::from(vec![false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, d.len());
        assert_eq!(true, d.is_null(0));
    }

    #[test]
    fn test_filter_string_array_with_null() {
        let a = StringArray::from(vec![Some("hello"), None, Some("world"), None]);
        let b = BooleanArray::from(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!(false, d.is_null(0));
        assert_eq!(true, d.is_null(1));
    }

    #[test]
    fn test_filter_binary_array_with_null() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world"), None];
        let a = BinaryArray::from(data);
        let b = BooleanArray::from(vec![true, false, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(b"hello", d.value(0));
        assert_eq!(false, d.is_null(0));
        assert_eq!(true, d.is_null(1));
    }

    #[test]
    fn test_filter_array_slice_with_null() {
        let a_slice =
            Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]).slice(1, 4);
        let a = a_slice.as_ref();
        let b = BooleanArray::from(vec![true, false, false, true]);
        // filtering with sliced filter array is not currently supported
        // let b_slice = BooleanArray::from(vec![true, false, false, true, false]).slice(1, 4);
        // let b = b_slice.as_any().downcast_ref().unwrap();
        let c = filter(a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!(true, d.is_null(0));
        assert_eq!(false, d.is_null(1));
        assert_eq!(9, d.value(1));
    }

    #[test]
    fn test_filter_dictionary_array() {
        let values = vec![Some("hello"), None, Some("world"), Some("!")];
        let a: Int8DictionaryArray = values.iter().copied().collect();
        let b = BooleanArray::from(vec![false, true, true, false]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<Int8DictionaryArray>()
            .unwrap();
        let value_array = d.values();
        let values = value_array.as_any().downcast_ref::<StringArray>().unwrap();
        // values are cloned in the filtered dictionary array
        assert_eq!(3, values.len());
        // but keys are filtered
        assert_eq!(2, d.len());
        assert_eq!(true, d.is_null(0));
        assert_eq!("world", values.value(d.keys().value(1) as usize));
    }

    #[test]
    fn test_filter_string_array_with_negated_boolean_array() {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let mut bb = BooleanBuilder::new(2);
        bb.append_value(false).unwrap();
        bb.append_value(true).unwrap();
        bb.append_value(false).unwrap();
        bb.append_value(true).unwrap();
        let b = bb.finish();
        let b = crate::compute::not(&b).unwrap();

        let c = filter(&a, &b).unwrap();
        let d = c.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, d.len());
        assert_eq!("hello", d.value(0));
        assert_eq!("world", d.value(1));
    }

    #[test]
    fn test_filter_list_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
            .build();

        let value_offsets = Buffer::from(&[0i64, 3, 6, 8, 8].to_byte_slice());

        let list_data_type =
            DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(4)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Buffer::from([0b00000111]))
            .build();

        //  a = [[0, 1, 2], [3, 4, 5], [6, 7], null]
        let a = LargeListArray::from(list_data);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = filter(&a, &b).unwrap();
        let d = c
            .as_ref()
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();

        assert_eq!(DataType::Int32, d.value_type());

        // result should be [[3, 4, 5], null]
        assert_eq!(2, d.len());
        assert_eq!(1, d.null_count());
        assert_eq!(true, d.is_null(1));

        assert_eq!(0, d.value_offset(0));
        assert_eq!(3, d.value_length(0));
        assert_eq!(3, d.value_offset(1));
        assert_eq!(0, d.value_length(1));
        assert_eq!(
            Buffer::from(&[3, 4, 5].to_byte_slice()),
            d.values().data().buffers()[0].clone()
        );
        assert_eq!(
            Buffer::from(&[0i64, 3, 3].to_byte_slice()),
            d.data().buffers()[0].clone()
        );
        let inner_list = d.value(0);
        let inner_list = inner_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, inner_list.len());
        assert_eq!(0, inner_list.null_count());
        assert_eq!(inner_list, &Int32Array::from(vec![3, 4, 5]));
    }
}
