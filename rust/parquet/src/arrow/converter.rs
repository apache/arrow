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

use crate::data_type::{ByteArray, DataType, FixedLenByteArray, Int96};
// TODO: clean up imports (best done when there are few moving parts)
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, DecimalBuilder, FixedSizeBinaryBuilder,
    IntervalDayTimeArray, IntervalDayTimeBuilder, IntervalYearMonthArray,
    IntervalYearMonthBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder,
    PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder,
};
use arrow::compute::cast;
use std::convert::{From, TryInto};
use std::sync::Arc;

use crate::errors::Result;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowPrimitiveType};

use arrow::array::{
    BinaryArray, DecimalArray, DictionaryArray, FixedSizeBinaryArray, LargeBinaryArray,
    LargeStringArray, PrimitiveArray, StringArray, TimestampNanosecondArray,
};
use std::marker::PhantomData;

use crate::data_type::Int32Type as ParquetInt32Type;
use arrow::datatypes::Int32Type;

/// A converter is used to consume record reader's content and convert it to arrow
/// primitive array.
pub trait Converter<S, T> {
    /// This method converts record reader's buffered content into arrow array.
    /// It will consume record reader's data, but will not reset record reader's
    /// state.
    fn convert(&self, source: S) -> Result<T>;
}

pub struct FixedSizeArrayConverter {
    byte_width: i32,
}

impl FixedSizeArrayConverter {
    pub fn new(byte_width: i32) -> Self {
        Self { byte_width }
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, FixedSizeBinaryArray>
    for FixedSizeArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<FixedSizeBinaryArray> {
        let mut builder = FixedSizeBinaryBuilder::new(source.len(), self.byte_width);
        for v in source {
            match v {
                Some(array) => builder.append_value(array.data()),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct DecimalArrayConverter {
    precision: i32,
    scale: i32,
}

impl DecimalArrayConverter {
    pub fn new(precision: i32, scale: i32) -> Self {
        Self { precision, scale }
    }

    fn from_bytes_to_i128(b: &[u8]) -> i128 {
        assert!(b.len() <= 16, "DecimalArray supports only up to size 16");
        let first_bit = b[0] & 128u8 == 128u8;
        let mut result = if first_bit { [255u8; 16] } else { [0u8; 16] };
        for (i, v) in b.iter().enumerate() {
            result[i + (16 - b.len())] = *v;
        }
        i128::from_be_bytes(result)
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, DecimalArray> for DecimalArrayConverter {
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<DecimalArray> {
        let mut builder = DecimalBuilder::new(
            source.len(),
            self.precision as usize,
            self.scale as usize,
        );
        for v in source {
            match v {
                Some(array) => {
                    builder.append_value(Self::from_bytes_to_i128(array.data()))
                }
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}
/// An Arrow Interval converter, which reads the first 4 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow YearMonth value
pub struct IntervalYearMonthArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, IntervalYearMonthArray>
    for IntervalYearMonthArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<IntervalYearMonthArray> {
        let mut builder = IntervalYearMonthBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(i32::from_le_bytes(
                    array.data()[0..4].try_into().unwrap(),
                )),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

/// An Arrow Interval converter, which reads the last 8 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow DayTime value
pub struct IntervalDayTimeArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, IntervalDayTimeArray>
    for IntervalDayTimeArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<IntervalDayTimeArray> {
        let mut builder = IntervalDayTimeBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(i64::from_le_bytes(
                    array.data()[4..12].try_into().unwrap(),
                )),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct Int96ArrayConverter {
    pub timezone: Option<String>,
}

impl Converter<Vec<Option<Int96>>, TimestampNanosecondArray> for Int96ArrayConverter {
    fn convert(&self, source: Vec<Option<Int96>>) -> Result<TimestampNanosecondArray> {
        Ok(TimestampNanosecondArray::from_opt_vec(
            source
                .into_iter()
                .map(|int96| int96.map(|val| val.to_i64() * 1_000_000))
                .collect(),
            self.timezone.clone(),
        ))
    }
}

pub struct Utf8ArrayConverter {}

impl Converter<Vec<Option<ByteArray>>, StringArray> for Utf8ArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<StringArray> {
        let data_size = source
            .iter()
            .map(|x| x.as_ref().map(|b| b.len()).unwrap_or(0))
            .sum();

        let mut builder = StringBuilder::with_capacity(source.len(), data_size);
        for v in source {
            match v {
                Some(array) => builder.append_value(array.as_utf8()?),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct LargeUtf8ArrayConverter {}

impl Converter<Vec<Option<ByteArray>>, LargeStringArray> for LargeUtf8ArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<LargeStringArray> {
        let data_size = source
            .iter()
            .map(|x| x.as_ref().map(|b| b.len()).unwrap_or(0))
            .sum();

        let mut builder = LargeStringBuilder::with_capacity(source.len(), data_size);
        for v in source {
            match v {
                Some(array) => builder.append_value(array.as_utf8()?),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct BinaryArrayConverter {}

impl Converter<Vec<Option<ByteArray>>, BinaryArray> for BinaryArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<BinaryArray> {
        let mut builder = BinaryBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(array.data()),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct LargeBinaryArrayConverter {}

impl Converter<Vec<Option<ByteArray>>, LargeBinaryArray> for LargeBinaryArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<LargeBinaryArray> {
        let mut builder = LargeBinaryBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(array.data()),
                None => builder.append_null(),
            }?
        }

        Ok(builder.finish())
    }
}

pub struct StringDictionaryArrayConverter {}

impl<K: ArrowDictionaryKeyType> Converter<Vec<Option<ByteArray>>, DictionaryArray<K>>
    for StringDictionaryArrayConverter
{
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<DictionaryArray<K>> {
        let data_size = source
            .iter()
            .map(|x| x.as_ref().map(|b| b.len()).unwrap_or(0))
            .sum();

        let keys_builder = PrimitiveBuilder::<K>::new(source.len());
        let values_builder = StringBuilder::with_capacity(source.len(), data_size);

        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);
        for v in source {
            match v {
                Some(array) => {
                    let _ = builder.append(array.as_utf8()?)?;
                }
                None => builder.append_null()?,
            }
        }

        Ok(builder.finish())
    }
}

pub struct DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, ParquetType>
{
    _dict_value_source_marker: PhantomData<DictValueSourceType>,
    _dict_value_target_marker: PhantomData<DictValueTargetType>,
    _parquet_marker: PhantomData<ParquetType>,
}

impl<DictValueSourceType, DictValueTargetType, ParquetType>
    DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, ParquetType>
{
    pub fn new() -> Self {
        Self {
            _dict_value_source_marker: PhantomData,
            _dict_value_target_marker: PhantomData,
            _parquet_marker: PhantomData,
        }
    }
}

impl<K, DictValueSourceType, DictValueTargetType, ParquetType>
    Converter<Vec<Option<<ParquetType as DataType>::T>>, DictionaryArray<K>>
    for DictionaryArrayConverter<DictValueSourceType, DictValueTargetType, ParquetType>
where
    K: ArrowPrimitiveType,
    DictValueSourceType: ArrowPrimitiveType,
    DictValueTargetType: ArrowPrimitiveType,
    ParquetType: DataType,
    PrimitiveArray<DictValueSourceType>: From<Vec<Option<<ParquetType as DataType>::T>>>,
{
    fn convert(
        &self,
        source: Vec<Option<<ParquetType as DataType>::T>>,
    ) -> Result<DictionaryArray<K>> {
        let keys_builder = PrimitiveBuilder::<K>::new(source.len());
        let values_builder = PrimitiveBuilder::<DictValueTargetType>::new(source.len());

        let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);

        let source_array: Arc<dyn Array> =
            Arc::new(PrimitiveArray::<DictValueSourceType>::from(source));
        let target_array = cast(&source_array, &DictValueTargetType::DATA_TYPE)?;
        let target = target_array
            .as_any()
            .downcast_ref::<PrimitiveArray<DictValueTargetType>>()
            .unwrap();

        for i in 0..target.len() {
            if target.is_null(i) {
                builder.append_null()?;
            } else {
                let _ = builder.append(target.value(i))?;
            }
        }

        Ok(builder.finish())
    }
}

pub type Utf8Converter =
    ArrayRefConverter<Vec<Option<ByteArray>>, StringArray, Utf8ArrayConverter>;
pub type LargeUtf8Converter =
    ArrayRefConverter<Vec<Option<ByteArray>>, LargeStringArray, LargeUtf8ArrayConverter>;
pub type BinaryConverter =
    ArrayRefConverter<Vec<Option<ByteArray>>, BinaryArray, BinaryArrayConverter>;
pub type LargeBinaryConverter = ArrayRefConverter<
    Vec<Option<ByteArray>>,
    LargeBinaryArray,
    LargeBinaryArrayConverter,
>;
pub type StringDictionaryConverter<T> = ArrayRefConverter<
    Vec<Option<ByteArray>>,
    DictionaryArray<T>,
    StringDictionaryArrayConverter,
>;
pub type DictionaryConverter<K, SV, TV, P> = ArrayRefConverter<
    Vec<Option<<P as DataType>::T>>,
    DictionaryArray<K>,
    DictionaryArrayConverter<SV, TV, P>,
>;
pub type PrimitiveDictionaryConverter<K, V> = ArrayRefConverter<
    Vec<Option<<ParquetInt32Type as DataType>::T>>,
    DictionaryArray<K>,
    DictionaryArrayConverter<Int32Type, V, ParquetInt32Type>,
>;

pub type Int96Converter =
    ArrayRefConverter<Vec<Option<Int96>>, TimestampNanosecondArray, Int96ArrayConverter>;

pub type FixedLenBinaryConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    FixedSizeBinaryArray,
    FixedSizeArrayConverter,
>;
pub type IntervalYearMonthConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    IntervalYearMonthArray,
    IntervalYearMonthArrayConverter,
>;
pub type IntervalDayTimeConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    IntervalDayTimeArray,
    IntervalDayTimeArrayConverter,
>;

pub type DecimalConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    DecimalArray,
    DecimalArrayConverter,
>;

pub struct FromConverter<S, T> {
    _source: PhantomData<S>,
    _dest: PhantomData<T>,
}

impl<S, T> FromConverter<S, T>
where
    T: From<S>,
{
    pub fn new() -> Self {
        Self {
            _source: PhantomData,
            _dest: PhantomData,
        }
    }
}

impl<S, T> Converter<S, T> for FromConverter<S, T>
where
    T: From<S>,
{
    fn convert(&self, source: S) -> Result<T> {
        Ok(T::from(source))
    }
}

pub struct ArrayRefConverter<S, A, C> {
    _source: PhantomData<S>,
    _array: PhantomData<A>,
    converter: C,
}

impl<S, A, C> ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    pub fn new(converter: C) -> Self {
        Self {
            _source: PhantomData,
            _array: PhantomData,
            converter,
        }
    }
}

impl<S, A, C> Converter<S, ArrayRef> for ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    fn convert(&self, source: S) -> Result<ArrayRef> {
        self.converter
            .convert(source)
            .map(|array| Arc::new(array) as ArrayRef)
    }
}
