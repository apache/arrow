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

use crate::arrow::record_reader::RecordReader;
use crate::data_type::DataType;
use arrow::array::Array;
use std::convert::From;
use std::slice::from_raw_parts;
use std::sync::Arc;

use crate::errors::Result;
use arrow::array::BufferBuilder;
use arrow::array::BufferBuilderTrait;
use arrow::datatypes::ArrowPrimitiveType;

use arrow::array::ArrayDataBuilder;
use arrow::array::PrimitiveArray;
use std::marker::PhantomData;
use std::mem::transmute;

use crate::data_type::BoolType;
use crate::data_type::DoubleType as ParquetDoubleType;
use crate::data_type::FloatType as ParquetFloatType;
use crate::data_type::Int32Type as ParquetInt32Type;
use crate::data_type::Int64Type as ParquetInt64Type;
use arrow::datatypes::BooleanType;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::Int8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;
use arrow::datatypes::UInt8Type;

/// A converter is used to consume record reader's content and convert it to arrow
/// primitive array.
pub trait Converter<T: DataType> {
    /// This method converts record reader's buffered content into arrow array.
    fn convert(record_reader: &mut RecordReader<T>) -> Result<Arc<Array>>;
}

/// Trait for value to value conversion. This is similar to `std::convert::Into` in std
/// lib. Due to the limitation of rust compiler, we can't implement trait
/// `std::convert::Into` for primitives types.
pub trait ConvertAs<T> {
    /// This method does value to value conversion.
    fn convert_as(self) -> T;
}

impl<T> ConvertAs<T> for T {
    fn convert_as(self) -> T {
        self
    }
}

macro_rules! convert_as {
    ($src_type: ty, $dest_type: ty) => {
        impl ConvertAs<$dest_type> for $src_type {
            fn convert_as(self) -> $dest_type {
                self as $dest_type
            }
        }
    };
}

convert_as!(i32, i8);
convert_as!(i32, i16);
convert_as!(i32, u8);
convert_as!(i32, u16);
convert_as!(i32, u32);

/// Builder converter is used when parquet's data type is different from
/// arrow's native type. In this case, we need to iterate over values in record reader
/// and convert parquet value into arrow value one by one.
pub struct BuilderConverter<ParquetType, ArrowType> {
    _parquet_marker: PhantomData<ParquetType>,
    _arrow_marker: PhantomData<ArrowType>,
}

impl<ParquetType, ArrowType> Converter<ParquetType>
    for BuilderConverter<ParquetType, ArrowType>
where
    ParquetType: DataType,
    ArrowType: ArrowPrimitiveType,
    <ParquetType as DataType>::T: ConvertAs<<ArrowType as ArrowPrimitiveType>::Native>,
{
    fn convert(record_reader: &mut RecordReader<ParquetType>) -> Result<Arc<Array>> {
        let num_values = record_reader.num_values();
        let mut builder = BufferBuilder::<ArrowType>::new(num_values);

        let records_data = record_reader.consume_record_data();
        let data_slice = unsafe {
            from_raw_parts(
                transmute::<*const u8, *mut ParquetType::T>(records_data.raw_data()),
                num_values,
            )
        };
        for d in data_slice {
            builder.append(d.clone().convert_as())?;
        }
        std::mem::drop(records_data);

        let mut array_data = ArrayDataBuilder::new(ArrowType::get_data_type())
            .len(num_values)
            .add_buffer(builder.finish());

        if let Some(b) = record_reader.consume_bitmap_buffer() {
            array_data = array_data.null_bit_buffer(b);
        }

        Ok(Arc::new(PrimitiveArray::<ArrowType>::from(
            array_data.build(),
        )))
    }
}

/// Direct converter is used when parquet's data type is same as arrow's native type.
/// In this case we can reuse `RecordReader`'s record data buffer.
pub struct DirectConverter<ParquetType, ArrowType> {
    _parquet_marker: PhantomData<ParquetType>,
    _arrow_marker: PhantomData<ArrowType>,
}

impl<ParquetType, ArrowType> Converter<ParquetType>
    for DirectConverter<ParquetType, ArrowType>
where
    ParquetType: DataType,
    ArrowType: ArrowPrimitiveType,
{
    fn convert(record_reader: &mut RecordReader<ParquetType>) -> Result<Arc<Array>> {
        let record_data = record_reader.consume_record_data();

        let mut array_data = ArrayDataBuilder::new(ArrowType::get_data_type())
            .len(record_reader.num_values())
            .add_buffer(record_data);

        if let Some(b) = record_reader.consume_bitmap_buffer() {
            array_data = array_data.null_bit_buffer(b);
        }

        Ok(Arc::new(PrimitiveArray::<ArrowType>::from(
            array_data.build(),
        )))
    }
}

pub type BooleanConverter = DirectConverter<BoolType, BooleanType>;
pub type Int8Converter = BuilderConverter<ParquetInt32Type, Int8Type>;
pub type UInt8Converter = BuilderConverter<ParquetInt32Type, UInt8Type>;
pub type Int16Converter = BuilderConverter<ParquetInt32Type, Int16Type>;
pub type UInt16Converter = BuilderConverter<ParquetInt32Type, UInt16Type>;
pub type Int32Converter = DirectConverter<ParquetInt32Type, Int32Type>;
pub type UInt32Converter = DirectConverter<ParquetInt32Type, UInt32Type>;
pub type Int64Converter = DirectConverter<ParquetInt64Type, Int64Type>;
pub type UInt64Converter = DirectConverter<ParquetInt64Type, UInt64Type>;
pub type Float32Converter = DirectConverter<ParquetFloatType, Float32Type>;
pub type Float64Converter = DirectConverter<ParquetDoubleType, Float64Type>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::converter::Int16Converter;
    use crate::arrow::record_reader::RecordReader;
    use crate::basic::Encoding;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use crate::util::test_common::page_util::InMemoryPageReader;
    use crate::util::test_common::page_util::{DataPageBuilder, DataPageBuilderImpl};
    use arrow::array::ArrayEqual;
    use arrow::array::PrimitiveArray;
    use arrow::datatypes::{Int16Type, Int32Type};
    use std::rc::Rc;

    #[test]
    fn test_builder_converter() {
        let raw_data = vec![Some(1i16), None, Some(2i16), Some(3i16)];

        // Construct record reader
        let mut record_reader = {
            // Construct column schema
            let message_type = "
            message test_schema {
              OPTIONAL INT32 leaf;
            }
            ";

            let def_levels = [1i16, 0i16, 1i16, 1i16];
            build_record_reader(
                message_type,
                &[1, 2, 3],
                0i16,
                None,
                1i16,
                Some(&def_levels),
                10,
            )
        };

        let array = Int16Converter::convert(&mut record_reader).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<Int16Type>>()
            .unwrap();

        assert!(array.equals(&PrimitiveArray::<Int16Type>::from(raw_data)));
    }

    #[test]
    fn test_direct_converter() {
        let raw_data = vec![Some(1), None, Some(2), Some(3)];

        // Construct record reader
        let mut record_reader = {
            // Construct column schema
            let message_type = "
            message test_schema {
              OPTIONAL INT32 leaf;
            }
            ";

            let def_levels = [1i16, 0i16, 1i16, 1i16];
            build_record_reader(
                message_type,
                &[1, 2, 3],
                0i16,
                None,
                1i16,
                Some(&def_levels),
                10,
            )
        };

        let array = Int32Converter::convert(&mut record_reader).unwrap();
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .unwrap();

        assert!(array.equals(&PrimitiveArray::<Int32Type>::from(raw_data)));
    }

    fn build_record_reader<T: DataType>(
        message_type: &str,
        values: &[T::T],
        max_rep_level: i16,
        rep_levels: Option<&[i16]>,
        max_def_level: i16,
        def_levels: Option<&[i16]>,
        num_records: usize,
    ) -> RecordReader<T> {
        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Rc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        let mut record_reader = RecordReader::<T>::new(desc.clone());

        // Prepare record reader
        let mut pb = DataPageBuilderImpl::new(desc.clone(), 4, true);
        if rep_levels.is_some() {
            pb.add_rep_levels(max_rep_level, rep_levels.unwrap());
        }
        if def_levels.is_some() {
            pb.add_def_levels(max_def_level, def_levels.unwrap());
        }
        pb.add_values::<T>(Encoding::PLAIN, &values);
        let page = pb.consume();

        let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
        record_reader.set_page_reader(page_reader).unwrap();

        record_reader.read_records(num_records).unwrap();

        record_reader
    }
}
