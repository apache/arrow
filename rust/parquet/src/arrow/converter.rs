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
use arrow::array::ArrayRef;
use arrow::compute::cast;
use std::convert::From;
use std::sync::Arc;

use crate::errors::Result;
use arrow::datatypes::ArrowPrimitiveType;

use arrow::array::ArrayDataBuilder;
use arrow::array::PrimitiveArray;
use std::marker::PhantomData;

use crate::data_type::{
    BoolType, DoubleType as ParquetDoubleType, FloatType as ParquetFloatType,
    Int32Type as ParquetInt32Type, Int64Type as ParquetInt64Type,
};
use arrow::datatypes::{
    BooleanType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};

/// A converter is used to consume record reader's content and convert it to arrow
/// primitive array.
pub trait Converter<T: DataType> {
    /// This method converts record reader's buffered content into arrow array.
    /// It will consume record reader's data, but will not reset record reader's
    /// state.
    fn convert(record_reader: &mut RecordReader<T>) -> Result<ArrayRef>;
}

/// Cast converter first converts record reader's buffer to arrow's
/// `PrimitiveArray<ArrowSourceType>`, then casts it to `PrimitiveArray<ArrowTargetType>`.
pub struct CastConverter<ParquetType, ArrowSourceType, ArrowTargetType> {
    _parquet_marker: PhantomData<ParquetType>,
    _arrow_source_marker: PhantomData<ArrowSourceType>,
    _arrow_target_marker: PhantomData<ArrowTargetType>,
}

impl<ParquetType, ArrowSourceType, ArrowTargetType> Converter<ParquetType>
    for CastConverter<ParquetType, ArrowSourceType, ArrowTargetType>
where
    ParquetType: DataType,
    ArrowSourceType: ArrowPrimitiveType,
    ArrowTargetType: ArrowPrimitiveType,
{
    fn convert(record_reader: &mut RecordReader<ParquetType>) -> Result<ArrayRef> {
        let record_data = record_reader.consume_record_data();

        let mut array_data = ArrayDataBuilder::new(ArrowSourceType::get_data_type())
            .len(record_reader.num_values())
            .add_buffer(record_data);

        if let Some(b) = record_reader.consume_bitmap_buffer() {
            array_data = array_data.null_bit_buffer(b);
        }

        let primitive_array: ArrayRef =
            Arc::new(PrimitiveArray::<ArrowSourceType>::from(array_data.build()));

        Ok(cast(&primitive_array, &ArrowTargetType::get_data_type())?)
    }
}

pub type BooleanConverter = CastConverter<BoolType, BooleanType, BooleanType>;
pub type Int8Converter = CastConverter<ParquetInt32Type, Int32Type, Int8Type>;
pub type UInt8Converter = CastConverter<ParquetInt32Type, Int32Type, UInt8Type>;
pub type Int16Converter = CastConverter<ParquetInt32Type, Int32Type, Int16Type>;
pub type UInt16Converter = CastConverter<ParquetInt32Type, Int32Type, UInt16Type>;
pub type Int32Converter = CastConverter<ParquetInt32Type, Int32Type, Int32Type>;
pub type UInt32Converter = CastConverter<ParquetInt32Type, UInt32Type, UInt32Type>;
pub type Int64Converter = CastConverter<ParquetInt64Type, Int64Type, Int64Type>;
pub type UInt64Converter = CastConverter<ParquetInt64Type, UInt64Type, UInt64Type>;
pub type Float32Converter = CastConverter<ParquetFloatType, Float32Type, Float32Type>;
pub type Float64Converter = CastConverter<ParquetDoubleType, Float64Type, Float64Type>;

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
    fn test_converter_arrow_source_target_different() {
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
    fn test_converter_arrow_source_target_same() {
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
