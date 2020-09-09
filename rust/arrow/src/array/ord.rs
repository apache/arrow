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

//! Defines trait for array element comparison

use std::cmp::Ordering;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use TimeUnit::*;

/// Trait for Arrays that can be sorted
///
/// Example:
/// ```
/// use std::cmp::Ordering;
/// use arrow::array::*;
/// use arrow::datatypes::*;
///
/// let arr: Box<dyn OrdArray> = Box::new(PrimitiveArray::<Int64Type>::from(vec![
///     Some(-2),
///     Some(89),
///     Some(-64),
///     Some(101),
/// ]));
///
/// assert_eq!(arr.cmp_value(1, 2), Ordering::Greater);
/// ```
pub trait OrdArray {
    /// Return ordering between array element at index i and j
    fn cmp_value(&self, i: usize, j: usize) -> Ordering;
}

impl<T: OrdArray> OrdArray for Box<T> {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        T::cmp_value(self, i, j)
    }
}

impl<T: OrdArray> OrdArray for &T {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        T::cmp_value(self, i, j)
    }
}

impl<T: ArrowPrimitiveType> OrdArray for PrimitiveArray<T>
where
    T::Native: std::cmp::Ord,
{
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        self.value(i).cmp(&self.value(j))
    }
}

impl OrdArray for StringArray {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        self.value(i).cmp(self.value(j))
    }
}

impl OrdArray for NullArray {
    fn cmp_value(&self, _i: usize, _j: usize) -> Ordering {
        Ordering::Equal
    }
}

macro_rules! float_ord_cmp {
    ($NAME: ident, $T: ty) => {
        #[inline]
        fn $NAME(a: $T, b: $T) -> Ordering {
            if a < b {
                return Ordering::Less;
            }
            if a > b {
                return Ordering::Greater;
            }

            // convert to bits with canonical pattern for NaN
            let a = if a.is_nan() {
                <$T>::NAN.to_bits()
            } else {
                a.to_bits()
            };
            let b = if b.is_nan() {
                <$T>::NAN.to_bits()
            } else {
                b.to_bits()
            };

            if a == b {
                // Equal or both NaN
                Ordering::Equal
            } else if a < b {
                // (-0.0, 0.0) or (!NaN, NaN)
                Ordering::Less
            } else {
                // (0.0, -0.0) or (NaN, !NaN)
                Ordering::Greater
            }
        }
    };
}

float_ord_cmp!(cmp_f64, f64);
float_ord_cmp!(cmp_f32, f32);

#[repr(transparent)]
struct Float64ArrayAsOrdArray<'a>(&'a Float64Array);
#[repr(transparent)]
struct Float32ArrayAsOrdArray<'a>(&'a Float32Array);

impl OrdArray for Float64ArrayAsOrdArray<'_> {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        let a: f64 = self.0.value(i);
        let b: f64 = self.0.value(j);

        cmp_f64(a, b)
    }
}

impl OrdArray for Float32ArrayAsOrdArray<'_> {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        let a: f32 = self.0.value(i);
        let b: f32 = self.0.value(j);

        cmp_f32(a, b)
    }
}

fn float32_as_ord_array<'a>(array: &'a ArrayRef) -> Box<dyn OrdArray + 'a> {
    let float_array: &Float32Array = as_primitive_array::<Float32Type>(array);
    Box::new(Float32ArrayAsOrdArray(float_array))
}

fn float64_as_ord_array<'a>(array: &'a ArrayRef) -> Box<dyn OrdArray + 'a> {
    let float_array: &Float64Array = as_primitive_array::<Float64Type>(array);
    Box::new(Float64ArrayAsOrdArray(float_array))
}

struct StringDictionaryArrayAsOrdArray<'a, T: ArrowDictionaryKeyType> {
    dict_array: &'a DictionaryArray<T>,
    values: StringArray,
    keys: PrimitiveArray<T>,
}

impl<T: ArrowDictionaryKeyType> OrdArray for StringDictionaryArrayAsOrdArray<'_, T> {
    fn cmp_value(&self, i: usize, j: usize) -> Ordering {
        let keys = &self.keys;
        let dict = &self.values;

        let key_a: T::Native = keys.value(i);
        let key_b: T::Native = keys.value(j);

        let str_a = dict.value(key_a.to_usize().unwrap());
        let str_b = dict.value(key_b.to_usize().unwrap());

        str_a.cmp(str_b)
    }
}

fn string_dict_as_ord_array<'a, T: ArrowDictionaryKeyType>(
    array: &'a ArrayRef,
) -> Box<dyn OrdArray + 'a>
where
    T::Native: std::cmp::Ord,
{
    let dict_array = as_dictionary_array::<T>(array);
    let keys = dict_array.keys_array();

    let values = &dict_array.values();
    let values = StringArray::from(values.data());

    Box::new(StringDictionaryArrayAsOrdArray {
        dict_array,
        values,
        keys,
    })
}

/// Convert ArrayRef to OrdArray trait object
pub fn as_ordarray<'a>(values: &'a ArrayRef) -> Result<Box<OrdArray + 'a>> {
    match values.data_type() {
        DataType::Boolean => Ok(Box::new(as_boolean_array(&values))),
        DataType::Utf8 => Ok(Box::new(as_string_array(&values))),
        DataType::Null => Ok(Box::new(as_null_array(&values))),
        DataType::Int8 => Ok(Box::new(as_primitive_array::<Int8Type>(&values))),
        DataType::Int16 => Ok(Box::new(as_primitive_array::<Int16Type>(&values))),
        DataType::Int32 => Ok(Box::new(as_primitive_array::<Int32Type>(&values))),
        DataType::Int64 => Ok(Box::new(as_primitive_array::<Int64Type>(&values))),
        DataType::UInt8 => Ok(Box::new(as_primitive_array::<UInt8Type>(&values))),
        DataType::UInt16 => Ok(Box::new(as_primitive_array::<UInt16Type>(&values))),
        DataType::UInt32 => Ok(Box::new(as_primitive_array::<UInt32Type>(&values))),
        DataType::UInt64 => Ok(Box::new(as_primitive_array::<UInt64Type>(&values))),
        DataType::Date32(_) => Ok(Box::new(as_primitive_array::<Date32Type>(&values))),
        DataType::Date64(_) => Ok(Box::new(as_primitive_array::<Date64Type>(&values))),
        DataType::Time32(Second) => {
            Ok(Box::new(as_primitive_array::<Time32SecondType>(&values)))
        }
        DataType::Time32(Millisecond) => Ok(Box::new(as_primitive_array::<
            Time32MillisecondType,
        >(&values))),
        DataType::Time64(Microsecond) => Ok(Box::new(as_primitive_array::<
            Time64MicrosecondType,
        >(&values))),
        DataType::Time64(Nanosecond) => Ok(Box::new(as_primitive_array::<
            Time64NanosecondType,
        >(&values))),
        DataType::Timestamp(Second, _) => {
            Ok(Box::new(as_primitive_array::<TimestampSecondType>(&values)))
        }
        DataType::Timestamp(Millisecond, _) => Ok(Box::new(as_primitive_array::<
            TimestampMillisecondType,
        >(&values))),
        DataType::Timestamp(Microsecond, _) => Ok(Box::new(as_primitive_array::<
            TimestampMicrosecondType,
        >(&values))),
        DataType::Timestamp(Nanosecond, _) => Ok(Box::new(as_primitive_array::<
            TimestampNanosecondType,
        >(&values))),
        DataType::Interval(IntervalUnit::YearMonth) => Ok(Box::new(
            as_primitive_array::<IntervalYearMonthType>(&values),
        )),
        DataType::Interval(IntervalUnit::DayTime) => {
            Ok(Box::new(as_primitive_array::<IntervalDayTimeType>(&values)))
        }
        DataType::Duration(TimeUnit::Second) => {
            Ok(Box::new(as_primitive_array::<DurationSecondType>(&values)))
        }
        DataType::Duration(TimeUnit::Millisecond) => Ok(Box::new(as_primitive_array::<
            DurationMillisecondType,
        >(&values))),
        DataType::Duration(TimeUnit::Microsecond) => Ok(Box::new(as_primitive_array::<
            DurationMicrosecondType,
        >(&values))),
        DataType::Duration(TimeUnit::Nanosecond) => Ok(Box::new(as_primitive_array::<
            DurationNanosecondType,
        >(&values))),
        DataType::Float32 => Ok(float32_as_ord_array(&values)),
        DataType::Float64 => Ok(float64_as_ord_array(&values)),
        DataType::Dictionary(key_type, value_type)
            if *value_type.as_ref() == DataType::Utf8 =>
        {
            match key_type.as_ref() {
                DataType::Int8 => Ok(string_dict_as_ord_array::<Int8Type>(values)),
                DataType::Int16 => Ok(string_dict_as_ord_array::<Int16Type>(values)),
                DataType::Int32 => Ok(string_dict_as_ord_array::<Int32Type>(values)),
                DataType::Int64 => Ok(string_dict_as_ord_array::<Int64Type>(values)),
                DataType::UInt8 => Ok(string_dict_as_ord_array::<UInt8Type>(values)),
                DataType::UInt16 => Ok(string_dict_as_ord_array::<UInt16Type>(values)),
                DataType::UInt32 => Ok(string_dict_as_ord_array::<UInt32Type>(values)),
                DataType::UInt64 => Ok(string_dict_as_ord_array::<UInt64Type>(values)),
                t => Err(ArrowError::ComputeError(format!(
                    "Lexical Sort not supported for dictionary key type {:?}",
                    t
                ))),
            }
        }
        t => Err(ArrowError::ComputeError(format!(
            "Lexical Sort not supported for data type {:?}",
            t
        ))),
    }
}

#[cfg(test)]
pub mod tests {
    use crate::array::{as_ordarray, ArrayRef, DictionaryArray, Float64Array};
    use crate::datatypes::Int16Type;
    use std::cmp::Ordering;
    use std::iter::FromIterator;
    use std::sync::Arc;

    #[test]
    fn test_float64_as_ord_array() {
        let array = Float64Array::from(vec![1.0, 2.0, 3.0, f64::NAN]);
        let array_ref: ArrayRef = Arc::new(array);

        let ord_array = as_ordarray(&array_ref).unwrap();

        assert_eq!(Ordering::Less, ord_array.cmp_value(0, 1));
    }

    #[test]
    fn test_dict_as_ord_array() {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];
        let array = DictionaryArray::<Int16Type>::from_iter(data.into_iter());
        let array_ref: ArrayRef = Arc::new(array);

        let ord_array = as_ordarray(&array_ref).unwrap();

        assert_eq!(Ordering::Less, ord_array.cmp_value(0, 1));
        assert_eq!(Ordering::Equal, ord_array.cmp_value(3, 4));
        assert_eq!(Ordering::Greater, ord_array.cmp_value(2, 3));
    }
}
