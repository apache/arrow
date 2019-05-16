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

//! Defines take kernel for `ArrayRef`

use std::sync::Arc;

use crate::array::*;
use crate::builder::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

pub fn take(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef> {
    use TimeUnit::*;
    match array.data_type() {
        DataType::Boolean => panic!(),
        DataType::Int8 => take_numeric::<Int8Type>(array, index),
        DataType::Int16 => take_numeric::<Int16Type>(array, index),
        DataType::Int32 => take_numeric::<Int32Type>(array, index),
        DataType::Int64 => take_numeric::<Int64Type>(array, index),
        DataType::UInt8 => take_numeric::<UInt8Type>(array, index),
        DataType::UInt16 => take_numeric::<UInt16Type>(array, index),
        DataType::UInt32 => take_numeric::<UInt32Type>(array, index),
        DataType::UInt64 => take_numeric::<UInt64Type>(array, index),
        DataType::Float32 => take_numeric::<Float32Type>(array, index),
        DataType::Float64 => take_numeric::<Float64Type>(array, index),
        DataType::Date32(_) => take_numeric::<Date32Type>(array, index),
        DataType::Date64(_) => take_numeric::<Date64Type>(array, index),
        DataType::Time32(Second) => take_numeric::<Time32SecondType>(array, index),
        DataType::Time32(Millisecond) => {
            take_numeric::<Time32MillisecondType>(array, index)
        }
        DataType::Time64(Microsecond) => {
            take_numeric::<Time64MicrosecondType>(array, index)
        }
        DataType::Time64(Nanosecond) => {
            take_numeric::<Time64NanosecondType>(array, index)
        }
        DataType::Timestamp(Second) => take_numeric::<TimestampSecondType>(array, index),
        DataType::Timestamp(Millisecond) => {
            take_numeric::<TimestampMillisecondType>(array, index)
        }
        DataType::Timestamp(Microsecond) => {
            take_numeric::<TimestampMicrosecondType>(array, index)
        }
        DataType::Timestamp(Nanosecond) => {
            take_numeric::<TimestampNanosecondType>(array, index)
        }
        DataType::Utf8 => panic!(),
        DataType::List(_) => unimplemented!(),
        DataType::Struct(fields) => {
            let struct_: &StructArray =
                array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays: Result<Vec<ArrayRef>> =
                struct_.columns().iter().map(|a| take(a, index)).collect();
            let arrays = arrays?;
            let pairs: Vec<(Field, ArrayRef)> =
                fields.clone().into_iter().zip(arrays).collect();
            Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
        }
        t @ _ => unimplemented!("Sort not supported for data type {:?}", t),
    }
}

/// `take` implementation for numeric arrays
fn take_numeric<T>(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef>
where
    T: ArrowNumericType,
{
    let mut builder = PrimitiveBuilder::<T>::new(index.len());
    let a = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let len = a.len();
    for i in 0..index.len() {
        if index.is_null(i) {
            builder.append_null()?;
        } else {
            let ix = index.value(i) as usize;
            if ix >= len {
                return Err(ArrowError::ComputeError(
                    format!("Array index out of bounds, cannot get item at index {} from {} length", ix, len))
                );
            } else {
                if a.is_null(ix) {
                    builder.append_null()?;
                } else {
                    builder.append_value(a.value(ix))?;
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take_test_numeric<'a, T>(
        data: Vec<Option<T::Native>>,
        index: &UInt32Array,
    ) -> ArrayRef
    where
        T: ArrowNumericType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let a = PrimitiveArray::<T>::from(data);
        take(&(Arc::new(a) as ArrayRef), index).unwrap()
    }

    #[test]
    fn take_primitive() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(3)]);

        // uint8
        let a = take_test_numeric::<UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
        );
        assert_eq!(index.len(), a.len());
        let b = UInt8Array::from(vec![Some(3), None, None, Some(3), Some(3)]);
        let a = a.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(b.data(), a.data());

        // uint16
        let a = take_test_numeric::<UInt16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
        );
        assert_eq!(index.len(), a.len());
        let b = UInt16Array::from(vec![Some(3), None, None, Some(3), Some(3)]);
        let a = a.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(b.data(), a.data());

        // uint32
        let a = take_test_numeric::<UInt32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
        );
        assert_eq!(index.len(), a.len());
        let b = UInt32Array::from(vec![Some(3), None, None, Some(3), Some(3)]);
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(b.data(), a.data());

        // uint64
        let a = take_test_numeric::<UInt64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
        );
        assert_eq!(index.len(), a.len());
        let b = UInt64Array::from(vec![Some(3), None, None, Some(3), Some(3)]);
        let a = a.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(b.data(), a.data());

        // int8
        let a = take_test_numeric::<Int8Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
        );
        assert_eq!(index.len(), a.len());
        let b = Int8Array::from(vec![Some(-15), None, None, Some(-15), Some(-15)]);
        let a = a.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(b.data(), a.data());
    }

    // #[test]
    // fn take_bool() {}

    // #[test]
    // fn take_binary() {}

    // #[test]
    // fn take_list() {}

    // #[test]
    // fn take_struct() {}

    // #[test]
    // fn take_out_of_bounds() {}
}
