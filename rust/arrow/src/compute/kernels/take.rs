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
use crate::array_data::ArrayData;
use crate::buffer::Buffer;
use crate::builder::*;
use crate::compute::util::take_index_from_list;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Take elements from `ArrayRef` by supplying an array of indices.
///
/// Supports:
///  * null indices, returning a null value for the index
///  * checking for overflowing indices
pub fn take(
    array: &ArrayRef,
    index: &UInt32Array,
    options: Option<&TakeOptions>,
) -> Result<ArrayRef> {
    use TimeUnit::*;

    let options = options.map(|opt| opt.clone()).unwrap_or(Default::default());
    if options.check_bounds {
        let len = array.len();
        for i in 0..index.len() {
            if index.is_valid(i) {
                let ix = index.value(i) as usize;
                if ix >= len {
                    return Err(ArrowError::ComputeError(
                    format!("Array index out of bounds, cannot get item at index {} from {} entries", ix, len))
                );
                }
            }
        }
    }
    match array.data_type() {
        DataType::Boolean => take_bool(array, index),
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
        DataType::Utf8 => take_binary(array, index),
        DataType::List(_) => take_list(array, index),
        DataType::Struct(fields) => {
            let struct_: &StructArray =
                array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays: Result<Vec<ArrayRef>> = struct_
                .columns()
                .iter()
                .map(|a| take(a, index, Some(&options)))
                .collect();
            let arrays = arrays?;
            let pairs: Vec<(Field, ArrayRef)> =
                fields.clone().into_iter().zip(arrays).collect();
            Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
        }
        t @ _ => unimplemented!("Sort not supported for data type {:?}", t),
    }
}

/// Options that define how `take` should behave
#[derive(Clone)]
pub struct TakeOptions {
    /// perform bounds check before taking
    pub check_bounds: bool,
}

impl Default for TakeOptions {
    fn default() -> Self {
        Self {
            check_bounds: false,
        }
    }
}

/// `take` implementation for numeric arrays
fn take_numeric<T>(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef>
where
    T: ArrowNumericType,
{
    let mut builder = PrimitiveBuilder::<T>::new(index.len());
    let a = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for i in 0..index.len() {
        if index.is_null(i) {
            builder.append_null()?;
        } else {
            let ix = index.value(i) as usize;
            if a.is_valid(ix) {
                builder.append_value(a.value(ix))?;
            } else {
                builder.append_null()?;
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// `take` implementation for binary arrays
fn take_binary(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::new(index.len());
    let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    for i in 0..index.len() {
        if index.is_null(i) {
            builder.append(false)?;
        } else {
            let ix = index.value(i) as usize;
            if a.is_null(ix) {
                builder.append(false)?;
            } else {
                builder.append_values(a.value(ix))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// `take` implementation for boolean arrays
fn take_bool(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new(index.len());
    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    for i in 0..index.len() {
        if index.is_null(i) {
            builder.append_null()?;
        } else {
            let ix = index.value(i) as usize;
            if a.is_null(ix) {
                builder.append_null()?;
            } else {
                builder.append_value(a.value(ix))?;
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// `take` implementation for list arrays
///
/// Calculates the index and indexed offset for the inner array,
/// applying `take` on the inner array, then reconstructing a list array
/// with the indexed offsets
fn take_list(array: &ArrayRef, index: &UInt32Array) -> Result<ArrayRef> {
    let list: &ListArray = array.as_any().downcast_ref::<ListArray>().unwrap();
    let (indices, offsets) = take_index_from_list(array, index);
    let taken = take(&list.values(), &indices, None)?;
    let value_offsets = Buffer::from(offsets[..].to_byte_slice());
    let list_data = ArrayData::new(
        list.data_type().clone(),
        index.len(),
        Some(index.null_count()),
        taken.data().null_bitmap().clone().map(|bitmap| bitmap.bits),
        0,
        vec![value_offsets],
        vec![taken.data()],
    );
    let list_array = Arc::new(ListArray::from(Arc::new(list_data))) as ArrayRef;
    Ok(list_array)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take_test_numeric<'a, T>(
        data: Vec<Option<T::Native>>,
        index: &UInt32Array,
        options: Option<&TakeOptions>,
    ) -> ArrayRef
    where
        T: ArrowNumericType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let a = PrimitiveArray::<T>::from(data);
        take(&(Arc::new(a) as ArrayRef), index, options).unwrap()
    }

    // create a simple struct for testing purposes
    fn create_test_struct() -> ArrayRef {
        let boolean_data = BooleanArray::from(vec![true, false, false, true]).data();
        let int_data = Int32Array::from(vec![42, 28, 19, 31]).data();
        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, true));
        field_types.push(Field::new("b", DataType::Int32, true));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(4)
            .null_count(0)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .build();
        let struct_array = StructArray::from(struct_array_data);
        Arc::new(struct_array) as ArrayRef
    }

    #[test]
    fn test_take_primitive() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // uint8
        let a = take_test_numeric::<UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
        );
        assert_eq!(index.len(), a.len());
        let a = a.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(2, a.null_count());
        assert_eq!(3, a.value(0));
        assert_eq!(true, a.is_null(1));
        assert_eq!(true, a.is_null(2));
        assert_eq!(3, a.value(3));
        assert_eq!(2, a.value(4));

        // uint16
        let a = take_test_numeric::<UInt16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
        );
        assert_eq!(index.len(), a.len());
        let a = a.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(2, a.null_count());
        assert_eq!(3, a.value(0));
        assert_eq!(true, a.is_null(1));
        assert_eq!(true, a.is_null(2));
        assert_eq!(3, a.value(3));
        assert_eq!(2, a.value(4));

        // uint32
        let a = take_test_numeric::<UInt32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
        );
        assert_eq!(index.len(), a.len());
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(2, a.null_count());
        assert_eq!(3, a.value(0));
        assert_eq!(true, a.is_null(1));
        assert_eq!(true, a.is_null(2));
        assert_eq!(3, a.value(3));
        assert_eq!(2, a.value(4));

        // int64
        let a = take_test_numeric::<Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
        );
        assert_eq!(index.len(), a.len());
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(2, a.null_count());
        assert_eq!(-15, a.value(0));
        assert_eq!(true, a.is_null(1));
        assert_eq!(true, a.is_null(2));
        assert_eq!(-15, a.value(3));
        assert_eq!(2, a.value(4));

        // float32
        let a = take_test_numeric::<Float64Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
        );
        assert_eq!(index.len(), a.len());
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(2, a.null_count());
        assert_eq!(-3.1, a.value(0));
        assert_eq!(true, a.is_null(1));
        assert_eq!(true, a.is_null(2));
        assert_eq!(-3.1, a.value(3));
        assert_eq!(2.21, a.value(4));
    }

    #[test]
    fn test_take_bool() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(4)]);
        let array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(true),
            None,
        ]);
        let array = Arc::new(array) as ArrayRef;
        let a = take(&array, &index, None).unwrap();
        assert_eq!(a.len(), index.len());
        let b = BooleanArray::from(vec![
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(true),
        ]);
        assert_eq!(a.data(), b.data());
    }

    #[test]
    fn test_take_binary() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(4)]);
        let mut builder: BinaryBuilder = BinaryBuilder::new(6);
        builder.append_string("one").unwrap();
        builder.append_null().unwrap();
        builder.append_string("three").unwrap();
        builder.append_string("four").unwrap();
        builder.append_string("five").unwrap();
        let array = Arc::new(builder.finish()) as ArrayRef;
        let a = take(&array, &index, None).unwrap();
        assert_eq!(a.len(), index.len());
        builder.append_string("four").unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_string("four").unwrap();
        builder.append_string("five").unwrap();
        let b = builder.finish();
        assert_eq!(a.data(), b.data());
    }

    #[test]
    fn test_take_list() {
        // Construct a value array, [[0,0,0], [-1,-2,-1], [2,3]]
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 3]).data();
        // Construct offsets
        let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());
        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        // index returns: [[2,3], null, [-1,-2,-1], [2,3], [0,0,0]]
        let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(2), Some(0)]);

        let a = take(&list_array, &index, None).unwrap();
        let a: &ListArray = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(5, a.len());
        let b = a.values();
        let b = Int32Array::from(b.data());

        let taken_offsets = Buffer::from(&[0, 2, 2, 5, 7, 10].to_byte_slice());
        assert_eq!(1, b.null_count());
        assert_eq!(11, b.len());
        assert_eq!(2, b.value(0));
        assert_eq!(3, b.value(1));
        assert_eq!(true, b.is_null(2));
        assert_eq!(-1, b.value(3));
        assert_eq!(-2, b.value(4));
        assert_eq!(-1, b.value(5));
        assert_eq!(2, b.value(6));
        assert_eq!(3, b.value(7));
        assert_eq!(0, b.value(8));
        assert_eq!(0, b.value(9));
        assert_eq!(0, b.value(9));
        // list offsets should be the same
        assert_eq!(a.data_ref().buffers(), &[taken_offsets]);
    }

    #[test]
    fn test_take_list_with_nulls() {
        // Construct a value array, [[0,null,0], [-1,-2,3], null, [2,null]]
        let value_data = Int32Array::from(vec![
            Some(0),
            None,
            Some(0),
            Some(-1),
            Some(-2),
            Some(3),
            None,
            Some(5),
            None,
        ])
        .data();
        // Construct offsets
        let value_offsets = Buffer::from(&[0, 3, 6, 7, 9].to_byte_slice());
        // Construct a list array from the above two
        let list_data_type = DataType::List(Box::new(DataType::Int32));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(4)
            .add_buffer(value_offsets.clone())
            .null_count(1)
            .null_bit_buffer(Buffer::from([0b10111101, 0b00000000]))
            .add_child_data(value_data.clone())
            .build();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        // index returns: [null, null, [-1,-2,-1], [2,null], [0,null,0]]
        let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(3), Some(0)]);

        let a = take(&list_array, &index, None).unwrap();
        let a: &ListArray = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(5, a.len());
        let b = a.values();
        let b = Int32Array::from(b.data());

        let taken_offsets = Buffer::from(&[0, 1, 1, 4, 6, 9].to_byte_slice());
        assert_eq!(4, b.null_count());
        assert_eq!(10, b.len());
        assert_eq!(true, b.is_null(0));
        assert_eq!(true, b.is_null(1));
        assert_eq!(-1, b.value(2));
        assert_eq!(-2, b.value(3));
        assert_eq!(3, b.value(4));
        assert_eq!(5, b.value(5));
        assert_eq!(true, b.is_null(6));
        assert_eq!(0, b.value(7));
        assert_eq!(true, b.is_null(8));
        assert_eq!(0, b.value(9));
        // list offsets should be the same
        assert_eq!(a.data_ref().buffers(), &[taken_offsets]);
    }

    #[test]
    fn take_struct() {
        let array = create_test_struct();

        let index = UInt32Array::from(vec![0, 3, 1, 0, 2]);
        let a = take(&array, &index, None).unwrap();
        let a: &StructArray = a.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), a.len());
        assert_eq!(0, a.null_count());

        let b = BooleanArray::from(vec![true, true, false, true, false]);
        let c = Int32Array::from(vec![42, 31, 28, 42, 19]);
        let bools = a.column(0);
        let bools = bools.as_any().downcast_ref::<BooleanArray>().unwrap();
        let ints = a.column(1);
        let ints = ints.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(format!("{:?}", bools), format!("{:?}", b));
        assert_eq!(format!("{:?}", ints), format!("{:?}", c));
        assert_eq!(b.data(), bools.data());
        assert_eq!(c.data(), ints.data());
    }

    #[test]
    fn take_struct_with_nulls() {
        let array = create_test_struct();

        let index = UInt32Array::from(vec![None, Some(3), Some(1), None, Some(0)]);
        let a = take(&array, &index, None).unwrap();
        let a: &StructArray = a.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), a.len());
        assert_eq!(0, a.null_count());

        let b = BooleanArray::from(vec![None, Some(true), Some(false), None, Some(true)]);
        let c = Int32Array::from(vec![None, Some(31), Some(28), None, Some(42)]);
        let bools = a.column(0);
        let bools = bools.as_any().downcast_ref::<BooleanArray>().unwrap();
        let ints = a.column(1);
        let ints = ints.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(format!("{:?}", bools), format!("{:?}", b));
        assert_eq!(format!("{:?}", ints), format!("{:?}", c));
    }

    #[test]
    #[should_panic(
        expected = "Array index out of bounds, cannot get item at index 6 from 5 entries"
    )]
    fn take_out_of_bounds() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(6)]);
        let take_opt = TakeOptions { check_bounds: true };

        // int64
        take_test_numeric::<Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            Some(&take_opt),
        );
    }
}
