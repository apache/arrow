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

use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::mem::size_of;
use std::mem::transmute;
use std::rc::Rc;
use std::result::Result::Ok;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;
use std::vec::Vec;

use crate::arrow::converter::{
    BinaryConverter, BoolConverter, Converter, Float32Converter, Float64Converter,
    Int16Converter, Int32Converter, Int64Converter, Int8Converter, Int96Converter,
    UInt16Converter, UInt32Converter, UInt64Converter, UInt8Converter, Utf8Converter,
};
use crate::arrow::record_reader::RecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{LogicalType, Repetition, Type as PhysicalType};
use crate::column::page::PageIterator;
use crate::column::reader::ColumnReaderImpl;
use crate::data_type::{
    BoolType, ByteArrayType, DataType, DoubleType, FloatType, Int32Type, Int64Type,
    Int96Type,
};
use crate::errors::{ParquetError, ParquetError::ArrowError, Result};
use crate::file::reader::{FilePageIterator, FileReader};
use crate::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescPtr, Type, TypePtr,
};
use crate::schema::visitor::TypeVisitor;
use arrow::array::{
    Array, ArrayData, ArrayDataBuilder, ArrayDataRef, ArrayRef, BinaryArray,
    BinaryBuilder, BooleanBufferBuilder, BufferBuilderTrait, FixedSizeBinaryArray,
    FixedSizeBinaryBuilder, Int16BufferBuilder, ListArray, ListBuilder, PrimitiveArray,
    PrimitiveBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{
    BooleanType as ArrowBooleanType, DataType as ArrowType,
    Date32Type as ArrowDate32Type, Date64Type as ArrowDate64Type,
    DurationMicrosecondType as ArrowDurationMicrosecondType,
    DurationMillisecondType as ArrowDurationMillisecondType,
    DurationNanosecondType as ArrowDurationNanosecondType,
    DurationSecondType as ArrowDurationSecondType, Field,
    Float32Type as ArrowFloat32Type, Float64Type as ArrowFloat64Type,
    Int16Type as ArrowInt16Type, Int32Type as ArrowInt32Type,
    Int64Type as ArrowInt64Type, Int8Type as ArrowInt8Type, IntervalUnit,
    Time32MillisecondType as ArrowTime32MillisecondType,
    Time32SecondType as ArrowTime32SecondType,
    Time64MicrosecondType as ArrowTime64MicrosecondType,
    Time64NanosecondType as ArrowTime64NanosecondType, TimeUnit as ArrowTimeUnit,
    TimestampMicrosecondType as ArrowTimestampMicrosecondType,
    TimestampMillisecondType as ArrowTimestampMillisecondType,
    TimestampNanosecondType as ArrowTimestampNanosecondType,
    TimestampSecondType as ArrowTimestampSecondType, ToByteSlice,
    UInt16Type as ArrowUInt16Type, UInt32Type as ArrowUInt32Type,
    UInt64Type as ArrowUInt64Type, UInt8Type as ArrowUInt8Type,
};

use arrow::util::bit_util;
use std::any::Any;
use std::convert::TryFrom;

/// Array reader reads parquet data into arrow array.
pub trait ArrayReader {
    fn as_any(&self) -> &dyn Any;

    /// Returns the arrow type of this array reader.
    fn get_data_type(&self) -> &ArrowType;

    /// Reads at most `batch_size` records into an arrow array and return it.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef>;

    /// Returns the definition levels of data from last call of `next_batch`.
    /// The result is used by parent array reader to calculate its own definition
    /// levels and repetition levels, so that its parent can calculate null bitmap.
    fn get_def_levels(&self) -> Option<&[i16]>;

    /// Return the repetition levels of data from last call of `next_batch`.
    /// The result is used by parent array reader to calculate its own definition
    /// levels and repetition levels, so that its parent can calculate null bitmap.
    fn get_rep_levels(&self) -> Option<&[i16]>;
}

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct PrimitiveArrayReader<T: DataType> {
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    column_desc: ColumnDescPtr,
    record_reader: RecordReader<T>,
    _type_marker: PhantomData<T>,
}

impl<T: DataType> PrimitiveArrayReader<T> {
    /// Construct primitive array reader.
    pub fn new(
        mut pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
    ) -> Result<Self> {
        let data_type = parquet_to_arrow_field(column_desc.as_ref())?
            .data_type()
            .clone();

        let mut record_reader = RecordReader::<T>::new(column_desc.clone());
        record_reader.set_page_reader(
            pages
                .next()
                .ok_or_else(|| general_err!("Can't build array without pages!"))??,
        )?;

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            column_desc,
            record_reader,
            _type_marker: PhantomData,
        })
    }
}

/// Implementation of primitive array reader.
impl<T: DataType> ArrayReader for PrimitiveArrayReader<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type of primitive array.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    /// Reads at most `batch_size` records into array.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let mut records_read = 0usize;
        while records_read < batch_size {
            let records_to_read = batch_size - records_read;

            let records_read_once = self.record_reader.read_records(records_to_read)?;
            records_read = records_read + records_read_once;
            // Record reader exhausted
            if records_read_once < records_to_read {
                if let Some(page_reader) = self.pages.next() {
                    // Read from new page reader
                    self.record_reader.set_page_reader(page_reader?)?;
                } else {
                    // Page reader also exhausted
                    break;
                }
            }
        }

        // convert to arrays
        let array = match (&self.data_type, T::get_physical_type()) {
            (ArrowType::Boolean, PhysicalType::BOOLEAN) => unsafe {
                BoolConverter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<BoolType>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int8, PhysicalType::INT32) => unsafe {
                Int8Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int16, PhysicalType::INT32) => unsafe {
                Int16Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int32, PhysicalType::INT32) => unsafe {
                Int32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt8, PhysicalType::INT32) => unsafe {
                UInt8Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt16, PhysicalType::INT32) => unsafe {
                UInt16Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt32, PhysicalType::INT32) => unsafe {
                UInt32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int64, PhysicalType::INT64) => unsafe {
                Int64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt64, PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Float32, PhysicalType::FLOAT) => unsafe {
                Float32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<FloatType>,
                >(&mut self.record_reader))
            },
            (ArrowType::Float64, PhysicalType::DOUBLE) => unsafe {
                Float64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<DoubleType>,
                >(&mut self.record_reader))
            },
            (ArrowType::Timestamp(_, _), PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Date32(_), PhysicalType::INT32) => unsafe {
                UInt32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Date64(_), PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Time32(_), PhysicalType::INT32) => unsafe {
                UInt32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Time64(_), PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Interval(IntervalUnit::YearMonth), PhysicalType::INT32) => unsafe {
                UInt32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Interval(IntervalUnit::DayTime), PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Duration(_), PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (arrow_type, physical_type) => Err(general_err!(
                "Reading {:?} type from parquet {:?} is not supported yet.",
                arrow_type,
                physical_type
            )),
        }?;

        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels()?;
        self.rep_levels_buffer = self.record_reader.consume_rep_levels()?;
        self.record_reader.reset();
        Ok(array)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    column_desc: ColumnDescPtr,
    column_reader: Option<ColumnReaderImpl<T>>,
    _parquet_type_marker: PhantomData<T>,
    _converter_marker: PhantomData<C>,
}

impl<T, C> ArrayReader for ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        // Try to initialized column reader
        if self.column_reader.is_none() {
            let init_result = self.next_column_reader()?;
            if !init_result {
                return Err(general_err!("No page left!"));
            }
        }

        assert!(self.column_reader.is_some());

        let mut data_buffer: Vec<T::T> = Vec::with_capacity(batch_size);
        data_buffer.resize_with(batch_size, T::T::default);

        let mut def_levels_buffer = if self.column_desc.max_def_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut rep_levels_buffer = if self.column_desc.max_rep_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut num_read = 0;

        while num_read < batch_size {
            let num_to_read = batch_size - num_read;
            let cur_data_buf = &mut data_buffer[num_read..];
            let cur_def_levels_buf =
                def_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let cur_rep_levels_buf =
                rep_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let (data_read, levels_read) =
                self.column_reader.as_mut().unwrap().read_batch(
                    num_to_read,
                    cur_def_levels_buf,
                    cur_rep_levels_buf,
                    cur_data_buf,
                )?;

            // Fill space
            if levels_read > data_read {
                def_levels_buffer.iter().for_each(|cur_def_levels_buf| {
                    let (mut level_pos, mut data_pos) = (levels_read, data_read);
                    while level_pos > 0 && data_pos > 0 {
                        if cur_def_levels_buf[level_pos - 1]
                            == self.column_desc.max_def_level()
                        {
                            cur_data_buf.swap(level_pos - 1, data_pos - 1);
                            level_pos -= 1;
                            data_pos -= 1;
                        } else {
                            level_pos -= 1;
                        }
                    }
                });
            }

            let values_read = max(levels_read, data_read);
            num_read += values_read;
            // current page exhausted && page iterator exhausted
            if values_read < num_to_read && !self.next_column_reader()? {
                break;
            }
        }

        data_buffer.truncate(num_read);
        def_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));
        rep_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));

        self.def_levels_buffer = def_levels_buffer;
        self.rep_levels_buffer = rep_levels_buffer;

        let data: Vec<Option<T::T>> = if self.def_levels_buffer.is_some() {
            data_buffer
                .into_iter()
                .zip(self.def_levels_buffer.as_ref().unwrap().iter())
                .map(|(t, def_level)| {
                    if *def_level == self.column_desc.max_def_level() {
                        Some(t)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            data_buffer.into_iter().map(|t| Some(t)).collect()
        };

        C::convert(data)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_ref().map(|t| t.as_slice())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|t| t.as_slice())
    }
}

impl<T, C> ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    fn new(pages: Box<dyn PageIterator>, column_desc: ColumnDescPtr) -> Result<Self> {
        let data_type = parquet_to_arrow_field(column_desc.as_ref())?
            .data_type()
            .clone();

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            column_desc,
            column_reader: None,
            _parquet_type_marker: PhantomData,
            _converter_marker: PhantomData,
        })
    }

    fn next_column_reader(&mut self) -> Result<bool> {
        Ok(match self.pages.next() {
            Some(page) => {
                self.column_reader =
                    Some(ColumnReaderImpl::<T>::new(self.column_desc.clone(), page?));
                true
            }
            None => false,
        })
    }
}

/// Implementation of struct array reader.
pub struct ListArrayReader {
    item_reader: Box<dyn ArrayReader>,
    data_type: ArrowType,
    item_type: ArrowType,
    list_def_level: i16,
    list_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl ListArrayReader {
    /// Construct struct array reader.
    pub fn new(
        item_reader: Box<dyn ArrayReader>,
        data_type: ArrowType,
        item_type: ArrowType,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            item_reader,
            data_type,
            item_type,
            list_def_level: def_level,
            list_rep_level: rep_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

macro_rules! build_empty_list_array_with_primitive_items {
    ($item_type:ident) => {{
        let values_builder = PrimitiveBuilder::<$item_type>::new(0);
        let mut builder = ListBuilder::new(values_builder);
        let empty_list_array = builder.finish();
        Ok(Arc::new(empty_list_array))
    }};
}

macro_rules! build_empty_list_array_with_non_primitive_items {
    ($builder:ident) => {{
        let values_builder = $builder::new(0);
        let mut builder = ListBuilder::new(values_builder);
        let empty_list_array = builder.finish();
        Ok(Arc::new(empty_list_array))
    }};
}

fn build_empty_list_array(item_type: ArrowType) -> Result<ArrayRef> {
    match item_type {
        ArrowType::UInt8 => build_empty_list_array_with_primitive_items!(ArrowUInt8Type),
        ArrowType::UInt16 => {
            build_empty_list_array_with_primitive_items!(ArrowUInt16Type)
        }
        ArrowType::UInt32 => {
            build_empty_list_array_with_primitive_items!(ArrowUInt32Type)
        }
        ArrowType::UInt64 => {
            build_empty_list_array_with_primitive_items!(ArrowUInt64Type)
        }
        ArrowType::Int8 => build_empty_list_array_with_primitive_items!(ArrowInt8Type),
        ArrowType::Int16 => build_empty_list_array_with_primitive_items!(ArrowInt16Type),
        ArrowType::Int32 => build_empty_list_array_with_primitive_items!(ArrowInt32Type),
        ArrowType::Int64 => build_empty_list_array_with_primitive_items!(ArrowInt64Type),
        ArrowType::Float32 => {
            build_empty_list_array_with_primitive_items!(ArrowFloat32Type)
        }
        ArrowType::Float64 => {
            build_empty_list_array_with_primitive_items!(ArrowFloat64Type)
        }
        ArrowType::Boolean => {
            build_empty_list_array_with_primitive_items!(ArrowBooleanType)
        }
        ArrowType::Date32(_) => {
            build_empty_list_array_with_primitive_items!(ArrowDate32Type)
        }
        ArrowType::Date64(_) => {
            build_empty_list_array_with_primitive_items!(ArrowDate64Type)
        }
        ArrowType::Time32(ArrowTimeUnit::Second) => {
            build_empty_list_array_with_primitive_items!(ArrowTime32SecondType)
        }
        ArrowType::Time32(ArrowTimeUnit::Millisecond) => {
            build_empty_list_array_with_primitive_items!(ArrowTime32MillisecondType)
        }
        ArrowType::Time64(ArrowTimeUnit::Microsecond) => {
            build_empty_list_array_with_primitive_items!(ArrowTime64MicrosecondType)
        }
        ArrowType::Time64(ArrowTimeUnit::Nanosecond) => {
            build_empty_list_array_with_primitive_items!(ArrowTime64NanosecondType)
        }
        ArrowType::Duration(ArrowTimeUnit::Second) => {
            build_empty_list_array_with_primitive_items!(ArrowDurationSecondType)
        }
        ArrowType::Duration(ArrowTimeUnit::Millisecond) => {
            build_empty_list_array_with_primitive_items!(ArrowDurationMillisecondType)
        }
        ArrowType::Duration(ArrowTimeUnit::Microsecond) => {
            build_empty_list_array_with_primitive_items!(ArrowDurationMicrosecondType)
        }
        ArrowType::Duration(ArrowTimeUnit::Nanosecond) => {
            build_empty_list_array_with_primitive_items!(ArrowDurationNanosecondType)
        }
        ArrowType::Timestamp(ArrowTimeUnit::Second, _) => {
            build_empty_list_array_with_primitive_items!(ArrowTimestampSecondType)
        }
        ArrowType::Timestamp(ArrowTimeUnit::Millisecond, _) => {
            build_empty_list_array_with_primitive_items!(ArrowTimestampMillisecondType)
        }
        ArrowType::Timestamp(ArrowTimeUnit::Microsecond, _) => {
            build_empty_list_array_with_primitive_items!(ArrowTimestampMicrosecondType)
        }
        ArrowType::Timestamp(ArrowTimeUnit::Nanosecond, _) => {
            build_empty_list_array_with_primitive_items!(ArrowTimestampNanosecondType)
        }
        ArrowType::Utf8 => {
            build_empty_list_array_with_non_primitive_items!(StringBuilder)
        }
        ArrowType::Binary => {
            build_empty_list_array_with_non_primitive_items!(BinaryBuilder)
        }
        _ => Err(ParquetError::General(format!(
            "ListArray of type List({:?}) is not supported by array_reader",
            item_type
        ))),
    }
}

macro_rules! remove_primitive_array_indices {
    ($arr: expr, $item_type:ty, $indices:expr) => {{
        let mut new_data_vec = Vec::new();
        let array_data = match $arr.as_any().downcast_ref::<PrimitiveArray<$item_type>>() {
            Some(a) => a,
            _ => return Err(ParquetError::General(format!("Error generating next batch for ListArray: {:?} cannot be downcast to PrimitiveArray", $arr))),
        };
        for i in 0..array_data.len() {
            if !$indices.contains(&i) {
                if array_data.is_null(i) {
                    new_data_vec.push(None);
                } else {
                    new_data_vec.push(Some(array_data.value(i).into()));
                }
            }
        }
        Ok(Arc::new(<PrimitiveArray<$item_type>>::from(new_data_vec)))
    }};
}

macro_rules! remove_timestamp_array_indices {
    ($arr: expr, $array_type:ty, $indices:expr, $time_unit:expr) => {{
        let mut new_data_vec = Vec::new();
        let array_data = match $arr.as_any().downcast_ref::<PrimitiveArray<$array_type>>() {
            Some(a) => a,
            _ => return Err(ParquetError::General(format!("Error generating next batch for ListArray: {:?} cannot be downcast to PrimitiveArray<>", $arr))),
        };
        for i in 0..array_data.len() {
            if !$indices.contains(&i) {
                if array_data.is_null(i) {
                    new_data_vec.push(None);
                } else {
                    new_data_vec.push(Some(array_data.value(i).into()));
                }
            }
        }
        Ok(Arc::new(<PrimitiveArray<$array_type>>::from_opt_vec(new_data_vec, $time_unit)))
    }};
}

macro_rules! remove_binary_array_indices {
    ($arr: expr, $array_type:ty, $item_builder:ident, $indices:expr) => {{
        let array_data = match $arr.as_any().downcast_ref::<$array_type>() {
            Some(a) => a,
            _ => return Err(ParquetError::General(format!("Error generating next batch for ListArray: {:?} cannot be downcast to PrimitiveArray", $arr))),
        };
        let mut builder = BinaryBuilder::new(array_data.len());

        for i in 0..array_data.len() {
            if !$indices.contains(&i) {
                if array_data.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(array_data.value(i))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! remove_fixed_size_binary_array_indices {
    ($arr: expr, $array_type:ty, $item_builder:ident, $indices:expr, $len:expr) => {{
        let array_data = match $arr.as_any().downcast_ref::<$array_type>() {
            Some(a) => a,
            _ => return Err(ParquetError::General(format!("Error generating next batch for ListArray: {:?} cannot be downcast to PrimitiveArray", $arr))),
        };
        let mut builder = FixedSizeBinaryBuilder::new(array_data.len(), $len);
        for i in 0..array_data.len() {
            if !$indices.contains(&i) {
                if array_data.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(array_data.value(i))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! remove_string_array_indices {
    ($arr: expr, $array_type:ty, $indices:expr) => {{
        let mut new_data_vec = Vec::new();
        let array_data = match $arr.as_any().downcast_ref::<$array_type>() {
            Some(a) => a,
            _ => return Err(ParquetError::General(format!("Error generating next batch for ListArray: {:?} cannot be downcast to PrimitiveArray<>", $arr))),
        };
        for i in 0..array_data.len() {
            if !$indices.contains(&i) {
                if array_data.is_null(i) {
                    new_data_vec.push(None);
                } else {
                    new_data_vec.push(Some(array_data.value(i).into()));
                }
            }
        }
        match <$array_type>::try_from(new_data_vec.clone()) {
            Ok(a) => Ok(Arc::new(a)),
            _ => Err(ParquetError::General(format!("Error generating next batch for ListArray: non-primitive Arrow array cannot be built from {:?}", new_data_vec)))
        }
    }};
}

fn remove_indices(
    arr: ArrayRef,
    item_type: ArrowType,
    indices: Vec<usize>,
) -> Result<ArrayRef> {
    match item_type {
        ArrowType::UInt8 => remove_primitive_array_indices!(arr, ArrowUInt8Type, indices),
        ArrowType::UInt16 => {
            remove_primitive_array_indices!(arr, ArrowUInt16Type, indices)
        }
        ArrowType::UInt32 => {
            remove_primitive_array_indices!(arr, ArrowUInt32Type, indices)
        }
        ArrowType::UInt64 => {
            remove_primitive_array_indices!(arr, ArrowUInt64Type, indices)
        }
        ArrowType::Int8 => remove_primitive_array_indices!(arr, ArrowInt8Type, indices),
        ArrowType::Int16 => remove_primitive_array_indices!(arr, ArrowInt16Type, indices),
        ArrowType::Int32 => remove_primitive_array_indices!(arr, ArrowInt32Type, indices),
        ArrowType::Int64 => remove_primitive_array_indices!(arr, ArrowInt64Type, indices),
        ArrowType::Float32 => {
            remove_primitive_array_indices!(arr, ArrowFloat32Type, indices)
        }
        ArrowType::Float64 => {
            remove_primitive_array_indices!(arr, ArrowFloat64Type, indices)
        }
        ArrowType::Boolean => {
            remove_primitive_array_indices!(arr, ArrowBooleanType, indices)
        }
        ArrowType::Date32(_) => {
            remove_primitive_array_indices!(arr, ArrowDate32Type, indices)
        }
        ArrowType::Date64(_) => {
            remove_primitive_array_indices!(arr, ArrowDate64Type, indices)
        }
        ArrowType::Time32(ArrowTimeUnit::Second) => {
            remove_primitive_array_indices!(arr, ArrowTime32SecondType, indices)
        }
        ArrowType::Time32(ArrowTimeUnit::Millisecond) => {
            remove_primitive_array_indices!(arr, ArrowTime32MillisecondType, indices)
        }
        ArrowType::Time64(ArrowTimeUnit::Microsecond) => {
            remove_primitive_array_indices!(arr, ArrowTime64MicrosecondType, indices)
        }
        ArrowType::Time64(ArrowTimeUnit::Nanosecond) => {
            remove_primitive_array_indices!(arr, ArrowTime64NanosecondType, indices)
        }
        ArrowType::Duration(ArrowTimeUnit::Second) => {
            remove_primitive_array_indices!(arr, ArrowDurationSecondType, indices)
        }
        ArrowType::Duration(ArrowTimeUnit::Millisecond) => {
            remove_primitive_array_indices!(arr, ArrowDurationMillisecondType, indices)
        }
        ArrowType::Duration(ArrowTimeUnit::Microsecond) => {
            remove_primitive_array_indices!(arr, ArrowDurationMicrosecondType, indices)
        }
        ArrowType::Duration(ArrowTimeUnit::Nanosecond) => {
            remove_primitive_array_indices!(arr, ArrowDurationNanosecondType, indices)
        }
        ArrowType::Timestamp(ArrowTimeUnit::Second, time_unit) => {
            remove_timestamp_array_indices!(
                arr,
                ArrowTimestampSecondType,
                indices,
                time_unit
            )
        }
        ArrowType::Timestamp(ArrowTimeUnit::Millisecond, time_unit) => {
            remove_timestamp_array_indices!(
                arr,
                ArrowTimestampMillisecondType,
                indices,
                time_unit
            )
        }
        ArrowType::Timestamp(ArrowTimeUnit::Microsecond, time_unit) => {
            remove_timestamp_array_indices!(
                arr,
                ArrowTimestampMicrosecondType,
                indices,
                time_unit
            )
        }
        ArrowType::Timestamp(ArrowTimeUnit::Nanosecond, time_unit) => {
            remove_timestamp_array_indices!(
                arr,
                ArrowTimestampNanosecondType,
                indices,
                time_unit
            )
        }
        ArrowType::Utf8 => remove_string_array_indices!(arr, StringArray, indices),
        ArrowType::Binary => {
            remove_binary_array_indices!(arr, BinaryArray, BinaryBuilder, indices)
        }
        ArrowType::FixedSizeBinary(size) => remove_fixed_size_binary_array_indices!(
            arr,
            FixedSizeBinaryArray,
            FixedSizeBinaryBuilder,
            indices,
            size
        ),
        _ => Err(ParquetError::General(format!(
            "ListArray of type List({:?}) is not supported by array_reader",
            item_type
        ))),
    }
}

impl ArrayReader for ListArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a List.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        let next_batch_array = self.item_reader.next_batch(batch_size).unwrap();
        let item_type = self.item_reader.get_data_type().clone();

        if next_batch_array.len() == 0 {
            return build_empty_list_array(item_type);
        }
        let def_levels = self.item_reader.get_def_levels().unwrap();
        let rep_levels = self.item_reader.get_rep_levels().unwrap();

        if !((def_levels.len() == rep_levels.len())
            && (rep_levels.len() == next_batch_array.len()))
        {
            return Err(ArrowError(
                "Expected item_reader def_level and rep_level arrays to have the same length as batch array".to_string(),
            ));
        }

        // Need to remove from the values array the nulls that represent null lists rather than null items
        // null lists have def_level = 0
        let mut null_list_indices: Vec<usize> = Vec::new();
        for i in 0..def_levels.len() {
            if def_levels[i] == 0 {
                null_list_indices.push(i);
            }
        }
        let batch_values = match null_list_indices.len() {
            0 => next_batch_array.clone(),
            _ => remove_indices(next_batch_array.clone(), item_type, null_list_indices)?,
        };

        // null list has def_level = 0
        // empty list has def_level = 1
        // null item in a list has def_level = 2
        // non-null item has def_level = 3
        // first item in each list has rep_level = 0, subsequent items have rep_level = 1

        let mut offsets = Vec::new();
        let mut cur_offset = 0;
        for i in 0..rep_levels.len() {
            if rep_levels[i] == (0 as i16) {
                offsets.push(cur_offset)
            }
            if def_levels[i] > 0 {
                cur_offset = cur_offset + 1;
            }
        }
        offsets.push(cur_offset);

        let num_bytes = bit_util::ceil(offsets.len(), 8);
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
        let null_slice = null_buf.data_mut();
        let mut list_index = 0;
        for i in 0..rep_levels.len() {
            if rep_levels[i] == (0 as i16) && def_levels[i] != (0 as i16) {
                bit_util::set_bit(null_slice, list_index);
            }
            if rep_levels[i] == (0 as i16) {
                list_index = list_index + 1;
            }
        }
        let value_offsets = Buffer::from(&offsets.to_byte_slice());

        // null list has def_level = 0
        let null_count = def_levels.iter().filter(|x| x == &&(0 as i16)).count();

        let list_data = ArrayData::builder(self.get_data_type().clone())
            .len(offsets.len() - 1)
            .add_buffer(value_offsets.clone())
            .add_child_data(batch_values.data())
            .null_bit_buffer(null_buf.freeze())
            .null_count(null_count)
            .offset(next_batch_array.offset())
            .build();

        let result_array = ListArray::from(list_data);
        return Ok(Arc::new(result_array));
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Implementation of struct array reader.
pub struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: ArrowType,
    struct_def_level: i16,
    struct_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl StructArrayReader {
    /// Construct struct array reader.
    pub fn new(
        data_type: ArrowType,
        children: Vec<Box<dyn ArrayReader>>,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            struct_rep_level: rep_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns data type.
    /// This must be a struct.
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    /// Read `batch_size` struct records.
    ///
    /// Definition levels of struct array is calculated as following:
    /// ```ignore
    /// def_levels[i] = min(child1_def_levels[i], child2_def_levels[i], ...,
    /// childn_def_levels[i]);
    /// ```
    ///
    /// Repetition levels of struct array is calculated as following:
    /// ```ignore
    /// rep_levels[i] = child1_rep_levels[i];
    /// ```
    ///
    /// The null bitmap of struct array is calculated from def_levels:
    /// ```ignore
    /// null_bitmap[i] = (def_levels[i] >= self.def_level);
    /// ```
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        if self.children.len() == 0 {
            self.def_level_buffer = None;
            self.rep_level_buffer = None;
            return Ok(Arc::new(StructArray::from(Vec::new())));
        }

        let children_array = self
            .children
            .iter_mut()
            .map(|reader| reader.next_batch(batch_size))
            .map(|batch| {
                return batch;
            })
            .try_fold(
                Vec::new(),
                |mut result, child_array| -> Result<Vec<ArrayRef>> {
                    result.push(child_array?);
                    Ok(result)
                },
            )?;

        // check that array child data has same size
        let children_array_len =
            children_array.first().map(|arr| arr.len()).ok_or_else(|| {
                general_err!("Struct array reader should have at least one child!")
            })?;

        let all_children_len_eq = children_array
            .iter()
            .all(|arr| arr.len() == children_array_len);
        if !all_children_len_eq {
            return Err(general_err!("Not all children array length are the same!"));
        }

        // calculate struct def level data
        let buffer_size = children_array_len * size_of::<i16>();
        let mut def_level_data_buffer = MutableBuffer::new(buffer_size);
        def_level_data_buffer.resize(buffer_size)?;

        let def_level_data = unsafe {
            let ptr = transmute::<*const u8, *mut i16>(def_level_data_buffer.raw_data());
            from_raw_parts_mut(ptr, children_array_len)
        };

        def_level_data
            .iter_mut()
            .for_each(|v| *v = self.struct_def_level);

        for child in &self.children {
            if let Some(current_child_def_levels) = child.get_def_levels() {
                if current_child_def_levels.len() != children_array_len
                    && children_array.len() != 1
                {
                    return Err(general_err!("Child array length are not equal!"));
                } else {
                    for i in 0..children_array_len {
                        def_level_data[i] =
                            min(def_level_data[i], current_child_def_levels[i]);
                    }
                }
            }
        }

        // calculate bitmap for current array
        let mut bitmap_builder = BooleanBufferBuilder::new(children_array_len);
        let mut null_count = 0;
        for def_level in def_level_data {
            let not_null = *def_level >= self.struct_def_level;
            if !not_null {
                null_count += 1;
            }
            bitmap_builder.append(not_null)?;
        }

        // Now we can build array data
        let array_data = ArrayDataBuilder::new(self.data_type.clone())
            .len(children_array_len)
            .null_count(null_count)
            .null_bit_buffer(bitmap_builder.finish())
            .child_data(
                children_array
                    .iter()
                    .map(|x| x.data())
                    .collect::<Vec<ArrayDataRef>>(),
            )
            .build();

        // calculate struct rep level data, since struct doesn't add to repetition
        // levels, here we just need to keep repetition levels of first array
        // TODO: Verify that all children array reader has same repetition levels
        let rep_level_data = self
            .children
            .first()
            .ok_or_else(|| {
                general_err!("Struct array reader should have at least one child!")
            })?
            .get_rep_levels()
            .map(|data| -> Result<Buffer> {
                let mut buffer = Int16BufferBuilder::new(children_array_len);
                buffer.append_slice(data)?;
                Ok(buffer.finish())
            })
            .transpose()?;

        self.def_level_buffer = Some(def_level_data_buffer.freeze());
        self.rep_level_buffer = rep_level_data;
        let result_array = StructArray::from(array_data);

        return Ok(Arc::new(result_array));
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer
            .as_ref()
            .map(|buf| unsafe { buf.typed_data() })
    }
}

/// Create array reader from parquet schema, column indices, and parquet file reader.
pub fn build_array_reader<T>(
    parquet_schema: SchemaDescPtr,
    column_indices: T,
    file_reader: Rc<dyn FileReader>,
) -> Result<Box<dyn ArrayReader>>
where
    T: IntoIterator<Item = usize>,
{
    let mut base_nodes = Vec::new();
    let mut base_nodes_set = HashSet::new();
    let mut leaves = HashMap::<*const Type, usize>::new();

    for c in column_indices {
        let column = parquet_schema.column(c).self_type() as *const Type;
        let root = parquet_schema.get_column_root_ptr(c);
        let root_raw_ptr = root.clone().as_ref() as *const Type;

        leaves.insert(column, c);
        if !base_nodes_set.contains(&root_raw_ptr) {
            base_nodes.push(root);
            base_nodes_set.insert(root_raw_ptr);
        }
    }

    if leaves.is_empty() {
        return Err(general_err!("Can't build array reader without columns!"));
    }

    ArrayReaderBuilder::new(
        Rc::new(parquet_schema.root_schema().clone()),
        Rc::new(leaves),
        file_reader,
    )
    .build_array_reader()
}

/// Used to build array reader.
struct ArrayReaderBuilder {
    root_schema: TypePtr,
    // Key: columns that need to be included in final array builder
    // Value: column index in schema
    columns_included: Rc<HashMap<*const Type, usize>>,
    file_reader: Rc<dyn FileReader>,
}

/// Used in type visitor.
#[derive(Clone)]
struct ArrayReaderBuilderContext {
    def_level: i16,
    rep_level: i16,
    path: ColumnPath,
}

impl Default for ArrayReaderBuilderContext {
    fn default() -> Self {
        Self {
            def_level: 0i16,
            rep_level: 0i16,
            path: ColumnPath::new(Vec::new()),
        }
    }
}

/// Create array reader by visiting schema.
impl<'a> TypeVisitor<Option<Box<dyn ArrayReader>>, &'a ArrayReaderBuilderContext>
    for ArrayReaderBuilder
{
    /// Build array reader for primitive type.
    fn visit_primitive(
        &mut self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        if self.is_included(cur_type.as_ref()) {
            let mut new_context = context.clone();
            if cur_type.name().to_string() != "item" {
                new_context.path.append(vec![cur_type.name().to_string()]);
            }
            match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;
                }
                _ => (),
            }
            let reader =
                self.build_for_primitive_type_inner(cur_type.clone(), &new_context)?;
            if cur_type.get_basic_info().repetition() == Repetition::REPEATED {
                Err(ArrowError(
                    "Reading repeated field is not supported yet!".to_string(),
                ))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    /// Build array reader for struct type.
    fn visit_struct(
        &mut self,
        cur_type: Rc<Type>,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {
        let mut new_context = context.clone();
        new_context.path.append(vec![cur_type.name().to_string()]);

        if cur_type.get_basic_info().has_repetition() {
            match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;
                }
                _ => (),
            }
        }

        if let Some(reader) = self.build_for_struct_type_inner(&cur_type, &new_context)? {
            if cur_type.get_basic_info().has_repetition()
                && cur_type.get_basic_info().repetition() == Repetition::REPEATED
            {
                Err(ArrowError(
                    "Reading repeated field is not supported yet!".to_string(),
                ))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    /// Build array reader for map type.
    /// Currently this is not supported.
    fn visit_map(
        &mut self,
        _cur_type: Rc<Type>,
        _context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        Err(ArrowError(
            "Reading parquet map array into arrow is not supported yet!".to_string(),
        ))
    }

    /// Build array reader for list type.
    /// List type is a special case of struct type (see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
    fn visit_list_with_item(
        &mut self,
        list_type: Rc<Type>,
        _item_type: &Type,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        let mut new_context = context.clone();

        let list_child = &list_type.get_fields()[0];
        let item_child = &list_child.get_fields()[0];

        new_context.path.append(vec![list_type.name().to_string()]);

        match list_type.get_basic_info().repetition() {
            Repetition::REPEATED => {
                new_context.def_level += 1;
                new_context.rep_level += 1;
            }
            Repetition::OPTIONAL => {
                new_context.def_level += 1;
            }
            _ => (),
        }

        match list_child.get_basic_info().repetition() {
            Repetition::REPEATED => {
                new_context.def_level += 1;
                new_context.rep_level += 1;
            }
            Repetition::OPTIONAL => {
                new_context.def_level += 1;
            }
            _ => (),
        }

        let item_reader = self
            .dispatch(item_child.clone(), &new_context)
            .unwrap()
            .unwrap();
        let item_type = item_reader.get_data_type().clone();

        match item_type {
            ArrowType::List(_)
            | ArrowType::FixedSizeList(_, _)
            | ArrowType::Struct(_)
            | ArrowType::Dictionary(_, _) => Err(ArrowError(format!(
                "reading List({:?}) into arrow not supported yet",
                item_type
            ))),
            _ => {
                let arrow_type = ArrowType::List(Box::new(item_type.clone()));
                Ok(Some(Box::new(ListArrayReader::new(
                    item_reader,
                    arrow_type,
                    item_type,
                    new_context.def_level,
                    new_context.rep_level,
                ))))
            }
        }
    }
}

impl<'a> ArrayReaderBuilder {
    /// Construct array reader builder.
    fn new(
        root_schema: TypePtr,
        columns_included: Rc<HashMap<*const Type, usize>>,
        file_reader: Rc<dyn FileReader>,
    ) -> Self {
        Self {
            root_schema,
            columns_included,
            file_reader,
        }
    }

    /// Main entry point.
    fn build_array_reader(&mut self) -> Result<Box<dyn ArrayReader>> {
        let context = ArrayReaderBuilderContext::default();

        self.visit_struct(self.root_schema.clone(), &context)
            .and_then(|reader_opt| {
                reader_opt.ok_or_else(|| general_err!("Failed to build array reader!"))
            })
    }

    // Utility functions

    /// Check whether one column in included in this array reader builder.
    fn is_included(&self, t: &Type) -> bool {
        self.columns_included.contains_key(&(t as *const Type))
    }

    /// Creates primitive array reader for each primitive type.
    fn build_for_primitive_type_inner(
        &self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Box<dyn ArrayReader>> {
        let column_desc = Rc::new(ColumnDescriptor::new(
            cur_type.clone(),
            Some(self.root_schema.clone()),
            context.def_level,
            context.rep_level,
            context.path.clone(),
        ));
        let page_iterator = Box::new(FilePageIterator::new(
            self.columns_included[&(cur_type.as_ref() as *const Type)],
            self.file_reader.clone(),
        )?);

        match cur_type.get_physical_type() {
            PhysicalType::BOOLEAN => Ok(Box::new(PrimitiveArrayReader::<BoolType>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT32 => Ok(Box::new(PrimitiveArrayReader::<Int32Type>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT64 => Ok(Box::new(PrimitiveArrayReader::<Int64Type>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT96 => {
                Ok(Box::new(ComplexObjectArrayReader::<
                    Int96Type,
                    Int96Converter,
                >::new(page_iterator, column_desc)?))
            }
            PhysicalType::FLOAT => Ok(Box::new(PrimitiveArrayReader::<FloatType>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::DOUBLE => Ok(Box::new(
                PrimitiveArrayReader::<DoubleType>::new(page_iterator, column_desc)?,
            )),
            PhysicalType::BYTE_ARRAY => {
                if cur_type.get_basic_info().logical_type() == LogicalType::UTF8 {
                    Ok(Box::new(ComplexObjectArrayReader::<
                        ByteArrayType,
                        Utf8Converter,
                    >::new(
                        page_iterator, column_desc
                    )?))
                } else {
                    Ok(Box::new(ComplexObjectArrayReader::<
                        ByteArrayType,
                        BinaryConverter,
                    >::new(
                        page_iterator, column_desc
                    )?))
                }
            }
            other => Err(ArrowError(format!(
                "Unable to create primitive array reader for parquet physical type {}",
                other
            ))),
        }
    }

    /// Constructs struct array reader without considering repetition.
    fn build_for_struct_type_inner(
        &mut self,
        cur_type: &Type,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<dyn ArrayReader>>> {
        let mut fields = Vec::with_capacity(cur_type.get_fields().len());
        let mut children_reader = Vec::with_capacity(cur_type.get_fields().len());

        for child in cur_type.get_fields() {
            if let Some(child_reader) = self.dispatch(child.clone(), context)? {
                fields.push(Field::new(
                    child.name(),
                    child_reader.get_data_type().clone(),
                    child.is_optional(),
                ));
                children_reader.push(child_reader);
            }
        }

        if !fields.is_empty() {
            let arrow_type = ArrowType::Struct(fields);
            Ok(Some(Box::new(StructArrayReader::new(
                arrow_type,
                children_reader,
                context.def_level,
                context.rep_level,
            ))))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::array_reader::{
        build_array_reader, ArrayReader, PrimitiveArrayReader, StructArrayReader,
    };
    use crate::basic::{Encoding, Type as PhysicalType};
    use crate::column::page::Page;
    use crate::data_type::{DataType, Int32Type, Int64Type};
    use crate::errors::Result;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::{ColumnDescPtr, SchemaDescriptor};
    use crate::util::test_common::page_util::InMemoryPageIterator;
    use crate::util::test_common::{get_test_file, make_pages};
    use arrow::array::{Array, ArrayRef, ListArray, PrimitiveArray, StructArray};
    use arrow::datatypes::{
        DataType as ArrowType, Field, Int32Type as ArrowInt32, Int64Type as ArrowInt64,
        UInt32Type as ArrowUInt32, UInt64Type as ArrowUInt64,
    };
    use rand::distributions::uniform::SampleUniform;
    use std::any::Any;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::Arc;

    fn make_column_chuncks<T: DataType>(
        column_desc: ColumnDescPtr,
        encoding: Encoding,
        num_levels: usize,
        min_value: T::T,
        max_value: T::T,
        def_levels: &mut Vec<i16>,
        rep_levels: &mut Vec<i16>,
        values: &mut Vec<T::T>,
        page_lists: &mut Vec<Vec<Page>>,
        use_v2: bool,
        num_chuncks: usize,
    ) where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        for _i in 0..num_chuncks {
            let mut pages = VecDeque::new();
            let mut data = Vec::new();
            let mut page_def_levels = Vec::new();
            let mut page_rep_levels = Vec::new();

            make_pages::<T>(
                column_desc.clone(),
                encoding,
                1,
                num_levels,
                min_value,
                max_value,
                &mut page_def_levels,
                &mut page_rep_levels,
                &mut data,
                &mut pages,
                use_v2,
            );

            def_levels.append(&mut page_def_levels);
            rep_levels.append(&mut page_rep_levels);
            values.append(&mut data);
            page_lists.push(Vec::from(pages));
        }
    }

    #[test]
    fn test_primitive_array_reader_data() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Rc::new(SchemaDescriptor::new(Rc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chuncks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator = InMemoryPageIterator::new(
                schema.clone(),
                column_desc.clone(),
                page_lists,
            );

            let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
                Box::new(page_iterator),
                column_desc.clone(),
            )
            .unwrap();

            // Read first 50 values, which are all from the first column chunck
            let array = array_reader.next_batch(50).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(
                    data[0..50].iter().cloned().collect::<Vec<i32>>()
                ),
                array
            );

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(
                    data[50..150].iter().cloned().collect::<Vec<i32>>()
                ),
                array
            );

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(
                    data[150..200].iter().cloned().collect::<Vec<i32>>()
                ),
                array
            );
        }
    }

    macro_rules! test_primitive_array_reader_one_type {
        ($arrow_parquet_type:ty, $physical_type:expr, $logical_type_str:expr, $result_arrow_type:ty, $result_primitive_type:ty) => {{
            let message_type = format!(
                "
            message test_schema {{
              REQUIRED {:?} leaf ({});
          }}
            ",
                $physical_type, $logical_type_str
            );
            let schema = parse_message_type(&message_type)
                .map(|t| Rc::new(SchemaDescriptor::new(Rc::new(t))))
                .unwrap();

            let column_desc = schema.column(0);

            // Construct page iterator
            {
                let mut data = Vec::new();
                let mut page_lists = Vec::new();
                make_column_chuncks::<$arrow_parquet_type>(
                    column_desc.clone(),
                    Encoding::PLAIN,
                    100,
                    1,
                    200,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &mut data,
                    &mut page_lists,
                    true,
                    2,
                );
                let page_iterator = InMemoryPageIterator::new(
                    schema.clone(),
                    column_desc.clone(),
                    page_lists,
                );
                let mut array_reader = PrimitiveArrayReader::<$arrow_parquet_type>::new(
                    Box::new(page_iterator),
                    column_desc.clone(),
                )
                .unwrap();

                let array = array_reader.next_batch(50).unwrap();

                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$result_arrow_type>>()
                    .unwrap();

                assert_eq!(
                    &PrimitiveArray::<$result_arrow_type>::from(
                        data[0..50]
                            .iter()
                            .map(|x| *x as $result_primitive_type)
                            .collect::<Vec<$result_primitive_type>>()
                    ),
                    array
                );
            }
        }};
    }

    #[test]
    fn test_primitive_array_reader_temporal_types() {
        test_primitive_array_reader_one_type!(
            Int32Type,
            PhysicalType::INT32,
            "DATE",
            ArrowUInt32,
            u32
        );
        test_primitive_array_reader_one_type!(
            Int32Type,
            PhysicalType::INT32,
            "TIME_MILLIS",
            ArrowUInt32,
            u32
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIME_MICROS",
            ArrowUInt64,
            u64
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MILLIS",
            ArrowUInt64,
            u64
        );
        test_primitive_array_reader_one_type!(
            Int64Type,
            PhysicalType::INT64,
            "TIMESTAMP_MICROS",
            ArrowUInt64,
            u64
        );
    }

    #[test]
    fn test_primitive_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL INT32 leaf;
            }
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Rc::new(SchemaDescriptor::new(Rc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chuncks::<Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut def_levels,
                &mut rep_levels,
                &mut Vec::new(),
                &mut page_lists,
                true,
                2,
            );

            let page_iterator = InMemoryPageIterator::new(
                schema.clone(),
                column_desc.clone(),
                page_lists,
            );

            let mut array_reader = PrimitiveArrayReader::<Int32Type>::new(
                Box::new(page_iterator),
                column_desc.clone(),
            )
            .unwrap();

            let mut accu_len: usize = 0;

            // Read first 50 values, which are all from the first column chunck
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
        }
    }

    /// Array reader for test.
    struct InMemoryArrayReader {
        data_type: ArrowType,
        array: ArrayRef,
        def_levels: Option<Vec<i16>>,
        rep_levels: Option<Vec<i16>>,
    }

    impl InMemoryArrayReader {
        pub fn new(
            data_type: ArrowType,
            array: ArrayRef,
            def_levels: Option<Vec<i16>>,
            rep_levels: Option<Vec<i16>>,
        ) -> Self {
            Self {
                data_type,
                array,
                def_levels,
                rep_levels,
            }
        }
    }

    impl ArrayReader for InMemoryArrayReader {
        fn as_any(&self) -> &Any {
            self
        }

        fn get_data_type(&self) -> &ArrowType {
            &self.data_type
        }

        fn next_batch(&mut self, _batch_size: usize) -> Result<ArrayRef> {
            Ok(self.array.clone())
        }

        fn get_def_levels(&self) -> Option<&[i16]> {
            self.def_levels.as_ref().map(|v| v.as_slice())
        }

        fn get_rep_levels(&self) -> Option<&[i16]> {
            self.rep_levels.as_ref().map(|v| v.as_slice())
        }
    }

    #[test]
    fn test_struct_array_reader() {
        let array_1 = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![1, 2, 3, 4, 5]));
        let array_reader_1 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_1.clone(),
            Some(vec![0, 1, 2, 3, 1]),
            Some(vec![1, 1, 1, 1, 1]),
        );

        let array_2 = Arc::new(PrimitiveArray::<ArrowInt32>::from(vec![5, 4, 3, 2, 1]));
        let array_reader_2 = InMemoryArrayReader::new(
            ArrowType::Int32,
            array_2.clone(),
            Some(vec![0, 1, 3, 1, 2]),
            Some(vec![1, 1, 1, 1, 1]),
        );

        let struct_type = ArrowType::Struct(vec![
            Field::new("f1", array_1.data_type().clone(), true),
            Field::new("f2", array_2.data_type().clone(), true),
        ]);

        let mut struct_array_reader = StructArrayReader::new(
            struct_type,
            vec![Box::new(array_reader_1), Box::new(array_reader_2)],
            1,
            1,
        );

        let struct_array = struct_array_reader.next_batch(1024).unwrap();
        let struct_array = struct_array.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(5, struct_array.len());
        assert_eq!(
            vec![true, false, false, false, false],
            (0..5)
                .map(|idx| struct_array.data_ref().is_null(idx))
                .collect::<Vec<bool>>()
        );
        assert_eq!(
            Some(vec![0, 1, 1, 1, 1].as_slice()),
            struct_array_reader.get_def_levels()
        );
        assert_eq!(
            Some(vec![1, 1, 1, 1, 1].as_slice()),
            struct_array_reader.get_rep_levels()
        );
    }

    #[test] //TODO: this test file is not in the testing submodule yet (Morgan 03-18-2020)
    fn test_create_list_array_reader() {
        let file = get_test_file("tiny_int_array.parquet");
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap());
        let mut array_reader = build_array_reader(
            file_reader.metadata().file_metadata().schema_descr_ptr(),
            vec![0usize].into_iter(),
            file_reader,
        )
        .unwrap();

        let arrow_type = ArrowType::Struct(vec![Field::new(
            "int_array",
            ArrowType::List(Box::new(ArrowType::Int64)),
            true,
        )]);
        assert_eq!(array_reader.get_data_type(), &arrow_type);

        let next_batch = array_reader.next_batch(1024).unwrap();

        // parquets with primitive columns also produce a StructArray -- each struct is a field
        let struct_array = next_batch.as_any().downcast_ref::<StructArray>().unwrap();
        let array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(8, array.len());

        let sublist_0 = array.value(0);
        let sublist_0 = sublist_0
            .as_any()
            .downcast_ref::<PrimitiveArray<ArrowInt64>>()
            .unwrap();

        assert_eq!(2, sublist_0.len());
        assert_eq!(false, sublist_0.is_null(0));
        assert_eq!(false, sublist_0.is_null(1));
        assert_eq!(7000, sublist_0.value(0));
        assert_eq!(7001, sublist_0.value(1));
    }
}
