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

//! Defines take kernel for [Array]

use std::{ops::AddAssign, sync::Arc};

use crate::buffer::{Buffer, MutableBuffer};
use crate::compute::util::{
    take_value_indices_from_fixed_size_list, take_value_indices_from_list,
};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;
use crate::{array::*, buffer::buffer_bin_and};

use num::{ToPrimitive, Zero};
use TimeUnit::*;

macro_rules! downcast_take {
    ($type: ty, $values: expr, $indices: expr) => {{
        let values = $values
            .as_any()
            .downcast_ref::<PrimitiveArray<$type>>()
            .expect("Unable to downcast to a primitive array");
        Ok(Arc::new(take_primitive::<$type, _>(&values, $indices)?))
    }};
}

macro_rules! downcast_dict_take {
    ($type: ty, $values: expr, $indices: expr) => {{
        let values = $values
            .as_any()
            .downcast_ref::<DictionaryArray<$type>>()
            .expect("Unable to downcast to a dictionary array");
        Ok(Arc::new(take_dict::<$type, _>(values, $indices)?))
    }};
}

/// Take elements by index from [Array], creating a new [Array] from those indexes.
///
/// # Errors
/// This function errors whenever:
/// * An index cannot be casted to `usize` (typically 32 bit architectures)
/// * An index is out of bounds and `options` is set to check bounds.
/// # Safety
/// When `options` is not set to check bounds (default), taking indexes after `len` is undefined behavior.
/// # Examples
/// ```
/// use arrow::array::{StringArray, UInt32Array};
/// use arrow::error::Result;
/// use arrow::compute::take;
/// # fn main() -> Result<()> {
/// let values = StringArray::from(vec!["zero", "one", "two"]);
///
/// // Take items at index 2, and 1:
/// let indices = UInt32Array::from(vec![2, 1]);
/// let taken = take(&values, &indices, None)?;
/// let taken = taken.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(*taken, StringArray::from(vec!["two", "one"]));
/// # Ok(())
/// # }
/// ```
pub fn take<IndexType>(
    values: &Array,
    indices: &PrimitiveArray<IndexType>,
    options: Option<TakeOptions>,
) -> Result<ArrayRef>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    take_impl(values, indices, options)
}

fn take_impl<IndexType>(
    values: &Array,
    indices: &PrimitiveArray<IndexType>,
    options: Option<TakeOptions>,
) -> Result<ArrayRef>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    let options = options.unwrap_or_default();
    if options.check_bounds {
        let len = values.len();
        for i in 0..indices.len() {
            if indices.is_valid(i) {
                let ix = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                    ArrowError::ComputeError("Cast to usize failed".to_string())
                })?;
                if ix >= len {
                    return Err(ArrowError::ComputeError(
                    format!("Array index out of bounds, cannot get item at index {} from {} entries", ix, len))
                );
                }
            }
        }
    }
    match values.data_type() {
        DataType::Boolean => {
            let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Arc::new(take_boolean(values, indices)?))
        }
        DataType::Int8 => downcast_take!(Int8Type, values, indices),
        DataType::Int16 => downcast_take!(Int16Type, values, indices),
        DataType::Int32 => downcast_take!(Int32Type, values, indices),
        DataType::Int64 => downcast_take!(Int64Type, values, indices),
        DataType::UInt8 => downcast_take!(UInt8Type, values, indices),
        DataType::UInt16 => downcast_take!(UInt16Type, values, indices),
        DataType::UInt32 => downcast_take!(UInt32Type, values, indices),
        DataType::UInt64 => downcast_take!(UInt64Type, values, indices),
        DataType::Float32 => downcast_take!(Float32Type, values, indices),
        DataType::Float64 => downcast_take!(Float64Type, values, indices),
        DataType::Date32(_) => downcast_take!(Date32Type, values, indices),
        DataType::Date64(_) => downcast_take!(Date64Type, values, indices),
        DataType::Time32(Second) => downcast_take!(Time32SecondType, values, indices),
        DataType::Time32(Millisecond) => {
            downcast_take!(Time32MillisecondType, values, indices)
        }
        DataType::Time64(Microsecond) => {
            downcast_take!(Time64MicrosecondType, values, indices)
        }
        DataType::Time64(Nanosecond) => {
            downcast_take!(Time64NanosecondType, values, indices)
        }
        DataType::Timestamp(Second, _) => {
            downcast_take!(TimestampSecondType, values, indices)
        }
        DataType::Timestamp(Millisecond, _) => {
            downcast_take!(TimestampMillisecondType, values, indices)
        }
        DataType::Timestamp(Microsecond, _) => {
            downcast_take!(TimestampMicrosecondType, values, indices)
        }
        DataType::Timestamp(Nanosecond, _) => {
            downcast_take!(TimestampNanosecondType, values, indices)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            downcast_take!(IntervalYearMonthType, values, indices)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            downcast_take!(IntervalDayTimeType, values, indices)
        }
        DataType::Duration(TimeUnit::Second) => {
            downcast_take!(DurationSecondType, values, indices)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            downcast_take!(DurationMillisecondType, values, indices)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            downcast_take!(DurationMicrosecondType, values, indices)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            downcast_take!(DurationNanosecondType, values, indices)
        }
        DataType::Utf8 => {
            let values = values
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap();
            Ok(Arc::new(take_string::<i32, _>(values, indices)?))
        }
        DataType::LargeUtf8 => {
            let values = values
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .unwrap();
            Ok(Arc::new(take_string::<i64, _>(values, indices)?))
        }
        DataType::List(_) => {
            let values = values
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            Ok(Arc::new(take_list::<_, Int32Type>(values, indices)?))
        }
        DataType::LargeList(_) => {
            let values = values
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();
            Ok(Arc::new(take_list::<_, Int64Type>(values, indices)?))
        }
        DataType::FixedSizeList(_, length) => {
            let values = values
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            Ok(Arc::new(take_fixed_size_list(
                values,
                indices,
                *length as u32,
            )?))
        }
        DataType::Struct(fields) => {
            let struct_: &StructArray =
                values.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays: Result<Vec<ArrayRef>> = struct_
                .columns()
                .iter()
                .map(|a| take_impl(a.as_ref(), indices, Some(options.clone())))
                .collect();
            let arrays = arrays?;
            let pairs: Vec<(Field, ArrayRef)> =
                fields.clone().into_iter().zip(arrays).collect();
            Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => downcast_dict_take!(Int8Type, values, indices),
            DataType::Int16 => downcast_dict_take!(Int16Type, values, indices),
            DataType::Int32 => downcast_dict_take!(Int32Type, values, indices),
            DataType::Int64 => downcast_dict_take!(Int64Type, values, indices),
            DataType::UInt8 => downcast_dict_take!(UInt8Type, values, indices),
            DataType::UInt16 => downcast_dict_take!(UInt16Type, values, indices),
            DataType::UInt32 => downcast_dict_take!(UInt32Type, values, indices),
            DataType::UInt64 => downcast_dict_take!(UInt64Type, values, indices),
            t => unimplemented!("Take not supported for dictionary key type {:?}", t),
        },
        t => unimplemented!("Take not supported for data type {:?}", t),
    }
}

/// Options that define how `take` should behave
#[derive(Clone, Debug)]
pub struct TakeOptions {
    /// Perform bounds check before taking indices from values.
    /// If enabled, an `ArrowError` is returned if the indices are out of bounds.
    /// If not enabled, and indices exceed bounds, the kernel will panic.
    pub check_bounds: bool,
}

impl Default for TakeOptions {
    fn default() -> Self {
        Self {
            check_bounds: false,
        }
    }
}

/// `take` implementation for all primitive arrays
///
/// This checks if an `indices` slot is populated, and gets the value from `values`
///  as the populated index.
/// If the `indices` slot is null, a null value is returned.
/// For example, given:
///     values:  [1, 2, 3, null, 5]
///     indices: [0, null, 4, 3]
/// The result is: [1 (slot 0), null (null slot), 5 (slot 4), null (slot 3)]
fn take_primitive<T, I>(
    values: &PrimitiveArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: num::Num,
    I: ArrowNumericType,
    I::Native: ToPrimitive,
{
    let data_len = indices.len();

    let mut buffer =
        MutableBuffer::from_len_zeroed(data_len * std::mem::size_of::<T::Native>());
    let data = buffer.typed_data_mut();

    let nulls;

    if values.null_count() == 0 {
        // Take indices without null checking
        for (i, elem) in data.iter_mut().enumerate() {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            *elem = values.value(index);
        }
        nulls = indices.data_ref().null_buffer().cloned();
    } else {
        let num_bytes = bit_util::ceil(data_len, 8);
        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);

        let null_slice = null_buf.as_slice_mut();

        for (i, elem) in data.iter_mut().enumerate() {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            if values.is_null(index) {
                bit_util::unset_bit(null_slice, i);
            }

            *elem = values.value(index);
        }
        nulls = match indices.data_ref().null_buffer() {
            Some(buffer) => Some(buffer_bin_and(
                buffer,
                0,
                &null_buf.into(),
                0,
                indices.len(),
            )),
            None => Some(null_buf.into()),
        };
    }

    let data = ArrayData::new(
        T::DATA_TYPE,
        indices.len(),
        None,
        nulls,
        0,
        vec![buffer.into()],
        vec![],
    );
    Ok(PrimitiveArray::<T>::from(Arc::new(data)))
}

/// `take` implementation for boolean arrays
fn take_boolean<IndexType>(
    values: &BooleanArray,
    indices: &PrimitiveArray<IndexType>,
) -> Result<BooleanArray>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    let data_len = indices.len();

    let num_byte = bit_util::ceil(data_len, 8);
    let mut val_buf = MutableBuffer::from_len_zeroed(num_byte);

    let val_slice = val_buf.as_slice_mut();

    let null_count = values.null_count();

    let nulls;
    if null_count == 0 {
        (0..data_len).try_for_each::<_, Result<()>>(|i| {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            if values.value(index) {
                bit_util::set_bit(val_slice, i);
            }

            Ok(())
        })?;

        nulls = indices.data_ref().null_buffer().cloned();
    } else {
        let mut null_buf = MutableBuffer::new(num_byte).with_bitset(num_byte, true);
        let null_slice = null_buf.as_slice_mut();

        (0..data_len).try_for_each::<_, Result<()>>(|i| {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            if values.is_null(index) {
                bit_util::unset_bit(null_slice, i);
            } else if values.value(index) {
                bit_util::set_bit(val_slice, i);
            }

            Ok(())
        })?;

        nulls = match indices.data_ref().null_buffer() {
            Some(buffer) => Some(buffer_bin_and(
                buffer,
                0,
                &null_buf.into(),
                0,
                indices.len(),
            )),
            None => Some(null_buf.into()),
        };
    }

    let data = ArrayData::new(
        DataType::Boolean,
        indices.len(),
        None,
        nulls,
        0,
        vec![val_buf.into()],
        vec![],
    );
    Ok(BooleanArray::from(Arc::new(data)))
}

/// `take` implementation for string arrays
fn take_string<OffsetSize, IndexType>(
    array: &GenericStringArray<OffsetSize>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<GenericStringArray<OffsetSize>>
where
    OffsetSize: Zero + AddAssign + StringOffsetSizeTrait,
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    let data_len = indices.len();

    let bytes_offset = (data_len + 1) * std::mem::size_of::<OffsetSize>();
    let mut offsets_buffer = MutableBuffer::from_len_zeroed(bytes_offset);

    let offsets = offsets_buffer.typed_data_mut();
    let mut values = MutableBuffer::new(0);
    let mut length_so_far = OffsetSize::zero();
    offsets[0] = length_so_far;

    let nulls;
    if array.null_count() == 0 && indices.null_count() == 0 {
        for (i, offset) in offsets.iter_mut().skip(1).enumerate() {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            let s = array.value(index);

            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            values.extend_from_slice(s.as_bytes());
            *offset = length_so_far;
        }
        nulls = None
    } else if indices.null_count() == 0 {
        let num_bytes = bit_util::ceil(data_len, 8);

        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();

        for (i, offset) in offsets.iter_mut().skip(1).enumerate() {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            if array.is_valid(index) {
                let s = array.value(index);

                length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                values.extend_from_slice(s.as_bytes());
            } else {
                bit_util::unset_bit(null_slice, i);
            }
            *offset = length_so_far;
        }
        nulls = Some(null_buf.into());
    } else if array.null_count() == 0 {
        for (i, offset) in offsets.iter_mut().skip(1).enumerate() {
            if indices.is_valid(i) {
                let index =
                    ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                        ArrowError::ComputeError("Cast to usize failed".to_string())
                    })?;

                let s = array.value(index);

                length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                values.extend_from_slice(s.as_bytes());
            }
            *offset = length_so_far;
        }
        nulls = indices.data_ref().null_buffer().cloned();
    } else {
        let num_bytes = bit_util::ceil(data_len, 8);

        let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
        let null_slice = null_buf.as_slice_mut();

        for (i, offset) in offsets.iter_mut().skip(1).enumerate() {
            let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
                ArrowError::ComputeError("Cast to usize failed".to_string())
            })?;

            if array.is_valid(index) && indices.is_valid(i) {
                let s = array.value(index);

                length_so_far += OffsetSize::from_usize(s.len()).unwrap();
                values.extend_from_slice(s.as_bytes());
            } else {
                // set null bit
                bit_util::unset_bit(null_slice, i);
            }
            *offset = length_so_far;
        }

        nulls = match indices.data_ref().null_buffer() {
            Some(buffer) => {
                Some(buffer_bin_and(buffer, 0, &null_buf.into(), 0, data_len))
            }
            None => Some(null_buf.into()),
        };
    }

    let mut data = ArrayData::builder(<OffsetSize as StringOffsetSizeTrait>::DATA_TYPE)
        .len(data_len)
        .add_buffer(offsets_buffer.into())
        .add_buffer(values.into());
    if let Some(null_buffer) = nulls {
        data = data.null_bit_buffer(null_buffer);
    }
    Ok(GenericStringArray::<OffsetSize>::from(data.build()))
}

/// `take` implementation for list arrays
///
/// Calculates the index and indexed offset for the inner array,
/// applying `take` on the inner array, then reconstructing a list array
/// with the indexed offsets
fn take_list<IndexType, OffsetType>(
    values: &GenericListArray<OffsetType::Native>,
    indices: &PrimitiveArray<IndexType>,
) -> Result<GenericListArray<OffsetType::Native>>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
    OffsetType: ArrowNumericType,
    OffsetType::Native: ToPrimitive + OffsetSizeTrait,
    PrimitiveArray<OffsetType>: From<Vec<Option<OffsetType::Native>>>,
{
    // TODO: Some optimizations can be done here such as if it is
    // taking the whole list or a contiguous sublist
    let (list_indices, offsets) =
        take_value_indices_from_list::<IndexType, OffsetType>(values, indices)?;

    let taken = take_impl::<OffsetType>(values.values().as_ref(), &list_indices, None)?;
    // determine null count and null buffer, which are a function of `values` and `indices`
    let mut null_count = 0;
    let num_bytes = bit_util::ceil(indices.len(), 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    {
        let null_slice = null_buf.as_slice_mut();
        offsets[..].windows(2).enumerate().for_each(
            |(i, window): (usize, &[OffsetType::Native])| {
                if window[0] == window[1] {
                    // offsets are equal, slot is null
                    bit_util::unset_bit(null_slice, i);
                    null_count += 1;
                }
            },
        );
    }
    let value_offsets = Buffer::from_slice_ref(&offsets);
    // create a new list with taken data and computed null information
    let list_data = ArrayDataBuilder::new(values.data_type().clone())
        .len(indices.len())
        .null_bit_buffer(null_buf.into())
        .offset(0)
        .add_child_data(taken.data())
        .add_buffer(value_offsets)
        .build();
    Ok(GenericListArray::<OffsetType::Native>::from(list_data))
}

/// `take` implementation for `FixedSizeListArray`
///
/// Calculates the index and indexed offset for the inner array,
/// applying `take` on the inner array, then reconstructing a list array
/// with the indexed offsets
fn take_fixed_size_list<IndexType>(
    values: &FixedSizeListArray,
    indices: &PrimitiveArray<IndexType>,
    length: <UInt32Type as ArrowPrimitiveType>::Native,
) -> Result<FixedSizeListArray>
where
    IndexType: ArrowNumericType,
    IndexType::Native: ToPrimitive,
{
    let list_indices = take_value_indices_from_fixed_size_list(values, indices, length)?;
    let taken = take_impl::<UInt32Type>(values.values().as_ref(), &list_indices, None)?;

    // determine null count and null buffer, which are a function of `values` and `indices`
    let num_bytes = bit_util::ceil(indices.len(), 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
    let null_slice = null_buf.as_slice_mut();

    for i in 0..indices.len() {
        let index = ToPrimitive::to_usize(&indices.value(i)).ok_or_else(|| {
            ArrowError::ComputeError("Cast to usize failed".to_string())
        })?;
        if !indices.is_valid(i) || values.is_null(index) {
            bit_util::unset_bit(null_slice, i);
        }
    }

    let list_data = ArrayDataBuilder::new(values.data_type().clone())
        .len(indices.len())
        .null_bit_buffer(null_buf.into())
        .offset(0)
        .add_child_data(taken.data())
        .build();

    Ok(FixedSizeListArray::from(list_data))
}

/// `take` implementation for dictionary arrays
///
/// applies `take` to the keys of the dictionary array and returns a new dictionary array
/// with the same dictionary values and reordered keys
fn take_dict<T, I>(
    values: &DictionaryArray<T>,
    indices: &PrimitiveArray<I>,
) -> Result<DictionaryArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: num::Num,
    I: ArrowNumericType,
    I::Native: ToPrimitive,
{
    let new_keys = take_primitive::<T, I>(&values.keys_array(), indices)?;
    let new_keys_data = new_keys.data_ref();

    let data = Arc::new(ArrayData::new(
        values.data_type().clone(),
        new_keys.len(),
        Some(new_keys_data.null_count()),
        new_keys_data.null_buffer().cloned(),
        0,
        new_keys_data.buffers().to_vec(),
        values.data().child_data().to_vec(),
    ));

    Ok(DictionaryArray::<T>::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::util::tests::build_fixed_size_list_nullable;

    fn test_take_boolean_arrays(
        data: Vec<Option<bool>>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<bool>>,
    ) {
        let output = BooleanArray::from(data);
        let expected = Arc::new(BooleanArray::from(expected_data)) as ArrayRef;
        let output = take(&output, index, options).unwrap();
        assert_eq!(&output, &expected)
    }

    fn test_take_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        index: &UInt32Array,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = Arc::new(PrimitiveArray::<T>::from(expected_data)) as ArrayRef;
        let output = take(&output, index, options).unwrap();
        assert_eq!(&output, &expected)
    }

    fn test_take_impl_primitive_arrays<T, I>(
        data: Vec<Option<T::Native>>,
        index: &PrimitiveArray<I>,
        options: Option<TakeOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
        I: ArrowNumericType,
        I::Native: ToPrimitive,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = PrimitiveArray::<T>::from(expected_data);
        let output = take_impl(&output, index, options).unwrap();
        let output = output.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        assert_eq!(output, &expected)
    }

    // create a simple struct for testing purposes
    fn create_test_struct() -> StructArray {
        let boolean_data = BooleanArray::from(vec![true, false, false, true]).data();
        let int_data = Int32Array::from(vec![42, 28, 19, 31]).data();
        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, true));
        field_types.push(Field::new("b", DataType::Int32, true));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(4)
            .add_child_data(boolean_data)
            .add_child_data(int_data)
            .build();
        StructArray::from(struct_array_data)
    }

    #[test]
    fn test_take_primitive() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int8
        test_take_primitive_arrays::<Int8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int16
        test_take_primitive_arrays::<Int16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int32
        test_take_primitive_arrays::<Int32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int64
        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // uint8
        test_take_primitive_arrays::<UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // uint16
        test_take_primitive_arrays::<UInt16Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // uint32
        test_take_primitive_arrays::<UInt32Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int64
        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // interval_year_month
        test_take_primitive_arrays::<IntervalYearMonthType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // interval_day_time
        test_take_primitive_arrays::<IntervalDayTimeType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // duration_second
        test_take_primitive_arrays::<DurationSecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // duration_millisecond
        test_take_primitive_arrays::<DurationMillisecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // duration_microsecond
        test_take_primitive_arrays::<DurationMicrosecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // duration_nanosecond
        test_take_primitive_arrays::<DurationNanosecondType>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // float32
        test_take_primitive_arrays::<Float32Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );

        // float64
        test_take_primitive_arrays::<Float64Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );
    }

    #[test]
    fn test_take_impl_primitive_with_int64_indices() {
        let index = Int64Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int16
        test_take_impl_primitive_arrays::<Int16Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // int64
        test_take_impl_primitive_arrays::<Int64Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // uint64
        test_take_impl_primitive_arrays::<UInt64Type, Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // duration_millisecond
        test_take_impl_primitive_arrays::<DurationMillisecondType, Int64Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // float32
        test_take_impl_primitive_arrays::<Float32Type, Int64Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );
    }

    #[test]
    fn test_take_impl_primitive_with_uint8_indices() {
        let index = UInt8Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);

        // int16
        test_take_impl_primitive_arrays::<Int16Type, UInt8Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            None,
            vec![Some(3), None, None, Some(3), Some(2)],
        );

        // duration_millisecond
        test_take_impl_primitive_arrays::<DurationMillisecondType, UInt8Type>(
            vec![Some(0), None, Some(2), Some(-15), None],
            &index,
            None,
            vec![Some(-15), None, None, Some(-15), Some(2)],
        );

        // float32
        test_take_impl_primitive_arrays::<Float32Type, UInt8Type>(
            vec![Some(0.0), None, Some(2.21), Some(-3.1), None],
            &index,
            None,
            vec![Some(-3.1), None, None, Some(-3.1), Some(2.21)],
        );
    }

    #[test]
    fn test_take_primitive_bool() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(2)]);
        // boolean
        test_take_boolean_arrays(
            vec![Some(false), None, Some(true), Some(false), None],
            &index,
            None,
            vec![Some(false), None, None, Some(false), Some(true)],
        );
    }

    fn _test_take_string<'a, K: 'static>()
    where
        K: Array + PartialEq + From<Vec<Option<&'a str>>>,
    {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(4)]);

        let array = K::from(vec![
            Some("one"),
            None,
            Some("three"),
            Some("four"),
            Some("five"),
        ]);
        let actual = take(&array, &index, None).unwrap();
        assert_eq!(actual.len(), index.len());

        let actual = actual.as_any().downcast_ref::<K>().unwrap();

        let expected =
            K::from(vec![Some("four"), None, None, Some("four"), Some("five")]);

        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_take_string() {
        _test_take_string::<StringArray>()
    }

    #[test]
    fn test_take_large_string() {
        _test_take_string::<LargeStringArray>()
    }

    macro_rules! test_take_list {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,0,0], [-1,-2,-1], [2,3]]
            let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 3]).data();
            // Construct offsets
            let value_offsets: [$offset_type; 4] = [0, 3, 6, 8];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type = DataType::$list_data_type(Box::new(Field::new(
                "item",
                DataType::Int32,
                false,
            )));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(3)
                .add_buffer(value_offsets)
                .add_child_data(value_data)
                .build();
            let list_array = $list_array_type::from(list_data);

            // index returns: [[2,3], null, [-1,-2,-1], [2,3], [0,0,0]]
            let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(2), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type =
                a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [[2,3], null, [-1,-2,-1], [2,3], [0,0,0]]
            let expected_data = Int32Array::from(vec![
                Some(2),
                Some(3),
                Some(-1),
                Some(-2),
                Some(-1),
                Some(2),
                Some(3),
                Some(0),
                Some(0),
                Some(0),
            ])
            .data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 2, 2, 5, 7, 10];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer remains the same as only the indices have nulls
                .null_bit_buffer(
                    index.data().null_bitmap().as_ref().unwrap().bits.clone(),
                )
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    macro_rules! test_take_list_with_value_nulls {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,null,0], [-1,-2,3], [null], [5,null]]
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
            let value_offsets: [$offset_type; 5] = [0, 3, 6, 7, 9];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type = DataType::$list_data_type(Box::new(Field::new(
                "item",
                DataType::Int32,
                false,
            )));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(4)
                .add_buffer(value_offsets)
                .null_bit_buffer(Buffer::from([0b10111101, 0b00000000]))
                .add_child_data(value_data)
                .build();
            let list_array = $list_array_type::from(list_data);

            // index returns: [[null], null, [-1,-2,3], [2,null], [0,null,0]]
            let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(3), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type =
                a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [[null], null, [-1,-2,3], [5,null], [0,null,0]]
            let expected_data = Int32Array::from(vec![
                None,
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
                Some(0),
                None,
                Some(0),
            ])
            .data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 1, 1, 4, 6, 9];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer remains the same as only the indices have nulls
                .null_bit_buffer(
                    index.data().null_bitmap().as_ref().unwrap().bits.clone(),
                )
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    macro_rules! test_take_list_with_nulls {
        ($offset_type:ty, $list_data_type:ident, $list_array_type:ident) => {{
            // Construct a value array, [[0,null,0], [-1,-2,3], null, [5,null]]
            let value_data = Int32Array::from(vec![
                Some(0),
                None,
                Some(0),
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
            ])
            .data();
            // Construct offsets
            let value_offsets: [$offset_type; 5] = [0, 3, 6, 6, 8];
            let value_offsets = Buffer::from_slice_ref(&value_offsets);
            // Construct a list array from the above two
            let list_data_type = DataType::$list_data_type(Box::new(Field::new(
                "item",
                DataType::Int32,
                false,
            )));
            let list_data = ArrayData::builder(list_data_type.clone())
                .len(4)
                .add_buffer(value_offsets)
                .null_bit_buffer(Buffer::from([0b01111101]))
                .add_child_data(value_data)
                .build();
            let list_array = $list_array_type::from(list_data);

            // index returns: [null, null, [-1,-2,3], [5,null], [0,null,0]]
            let index = UInt32Array::from(vec![Some(2), None, Some(1), Some(3), Some(0)]);

            let a = take(&list_array, &index, None).unwrap();
            let a: &$list_array_type =
                a.as_any().downcast_ref::<$list_array_type>().unwrap();

            // construct a value array with expected results:
            // [null, null, [-1,-2,3], [5,null], [0,null,0]]
            let expected_data = Int32Array::from(vec![
                Some(-1),
                Some(-2),
                Some(3),
                Some(5),
                None,
                Some(0),
                None,
                Some(0),
            ])
            .data();
            // construct offsets
            let expected_offsets: [$offset_type; 6] = [0, 0, 0, 3, 5, 8];
            let expected_offsets = Buffer::from_slice_ref(&expected_offsets);
            // construct list array from the two
            let mut null_bits: [u8; 1] = [0; 1];
            bit_util::set_bit(&mut null_bits, 2);
            bit_util::set_bit(&mut null_bits, 3);
            bit_util::set_bit(&mut null_bits, 4);
            let expected_list_data = ArrayData::builder(list_data_type)
                .len(5)
                // null buffer must be recalculated as both values and indices have nulls
                .null_bit_buffer(Buffer::from(null_bits))
                .add_buffer(expected_offsets)
                .add_child_data(expected_data)
                .build();
            let expected_list_array = $list_array_type::from(expected_list_data);

            assert_eq!(a, &expected_list_array);
        }};
    }

    fn do_take_fixed_size_list_test<T>(
        length: <Int32Type as ArrowPrimitiveType>::Native,
        input_data: Vec<Option<Vec<Option<T::Native>>>>,
        indices: Vec<<UInt32Type as ArrowPrimitiveType>::Native>,
        expected_data: Vec<Option<Vec<Option<T::Native>>>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
    {
        let indices = UInt32Array::from(indices);

        let input_array = build_fixed_size_list_nullable::<T>(input_data, length);

        let output = take_fixed_size_list(&input_array, &indices, length as u32).unwrap();

        let expected = build_fixed_size_list_nullable::<T>(expected_data, length);

        assert_eq!(&output, &expected)
    }

    #[test]
    fn test_take_list() {
        test_take_list!(i32, List, ListArray);
    }

    #[test]
    fn test_take_large_list() {
        test_take_list!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_take_list_with_value_nulls() {
        test_take_list_with_value_nulls!(i32, List, ListArray);
    }

    #[test]
    fn test_take_large_list_with_value_nulls() {
        test_take_list_with_value_nulls!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_test_take_list_with_nulls() {
        test_take_list_with_nulls!(i32, List, ListArray);
    }

    #[test]
    fn test_test_take_large_list_with_nulls() {
        test_take_list_with_nulls!(i64, LargeList, LargeListArray);
    }

    #[test]
    fn test_take_fixed_size_list() {
        do_take_fixed_size_list_test::<Int32Type>(
            3,
            vec![
                Some(vec![None, Some(1), Some(2)]),
                Some(vec![Some(3), Some(4), None]),
                Some(vec![Some(6), Some(7), Some(8)]),
            ],
            vec![2, 1, 0],
            vec![
                Some(vec![Some(6), Some(7), Some(8)]),
                Some(vec![Some(3), Some(4), None]),
                Some(vec![None, Some(1), Some(2)]),
            ],
        );

        do_take_fixed_size_list_test::<UInt8Type>(
            1,
            vec![
                Some(vec![Some(1)]),
                Some(vec![Some(2)]),
                Some(vec![Some(3)]),
                Some(vec![Some(4)]),
                Some(vec![Some(5)]),
                Some(vec![Some(6)]),
                Some(vec![Some(7)]),
                Some(vec![Some(8)]),
            ],
            vec![2, 7, 0],
            vec![
                Some(vec![Some(3)]),
                Some(vec![Some(8)]),
                Some(vec![Some(1)]),
            ],
        );

        do_take_fixed_size_list_test::<UInt64Type>(
            3,
            vec![
                Some(vec![Some(10), Some(11), Some(12)]),
                Some(vec![Some(13), Some(14), Some(15)]),
                None,
                Some(vec![Some(16), Some(17), Some(18)]),
            ],
            vec![3, 2, 1, 2, 0],
            vec![
                Some(vec![Some(16), Some(17), Some(18)]),
                None,
                Some(vec![Some(13), Some(14), Some(15)]),
                None,
                Some(vec![Some(10), Some(11), Some(12)]),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 4 but the index is 1000")]
    fn test_take_list_out_of_bounds() {
        // Construct a value array, [[0,0,0], [-1,-2,-1], [2,3]]
        let value_data = Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 3]).data();
        // Construct offsets
        let value_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8]);
        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build();
        let list_array = ListArray::from(list_data);

        let index = UInt32Array::from(vec![1000]);

        // A panic is expected here since we have not supplied the check_bounds
        // option.
        take(&list_array, &index, None).unwrap();
    }

    #[test]
    fn test_take_struct() {
        let array = create_test_struct();

        let index = UInt32Array::from(vec![0, 3, 1, 0, 2]);
        let a = take(&array, &index, None).unwrap();
        let a: &StructArray = a.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), a.len());
        assert_eq!(0, a.null_count());

        let expected_bool_data =
            BooleanArray::from(vec![true, true, false, true, false]).data();
        let expected_int_data = Int32Array::from(vec![42, 31, 28, 42, 19]).data();
        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, true));
        field_types.push(Field::new("b", DataType::Int32, true));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(5)
            .add_child_data(expected_bool_data)
            .add_child_data(expected_int_data)
            .build();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(a, &struct_array);
    }

    #[test]
    fn test_take_struct_with_nulls() {
        let array = create_test_struct();

        let index = UInt32Array::from(vec![None, Some(3), Some(1), None, Some(0)]);
        let a = take(&array, &index, None).unwrap();
        let a: &StructArray = a.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(index.len(), a.len());
        assert_eq!(0, a.null_count());

        let expected_bool_data =
            BooleanArray::from(vec![None, Some(true), Some(false), None, Some(true)])
                .data();
        let expected_int_data =
            Int32Array::from(vec![None, Some(31), Some(28), None, Some(42)]).data();

        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, true));
        field_types.push(Field::new("b", DataType::Int32, true));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(5)
            // TODO: see https://issues.apache.org/jira/browse/ARROW-5408 for why count != 2
            .add_child_data(expected_bool_data)
            .add_child_data(expected_int_data)
            .build();
        let struct_array = StructArray::from(struct_array_data);
        assert_eq!(a, &struct_array);
    }

    #[test]
    #[should_panic(
        expected = "Array index out of bounds, cannot get item at index 6 from 5 entries"
    )]
    fn test_take_out_of_bounds() {
        let index = UInt32Array::from(vec![Some(3), None, Some(1), Some(3), Some(6)]);
        let take_opt = TakeOptions { check_bounds: true };

        // int64
        test_take_primitive_arrays::<Int64Type>(
            vec![Some(0), None, Some(2), Some(3), None],
            &index,
            Some(take_opt),
            vec![None],
        );
    }

    #[test]
    fn test_take_dict() {
        let keys_builder = Int16Builder::new(8);
        let values_builder = StringBuilder::new(4);

        let mut dict_builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        dict_builder.append("foo").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("").unwrap();
        dict_builder.append_null().unwrap();
        dict_builder.append("foo").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("bar").unwrap();
        dict_builder.append("foo").unwrap();

        let array = dict_builder.finish();
        let dict_values = array.values().clone();
        let dict_values = dict_values.as_any().downcast_ref::<StringArray>().unwrap();

        let indices = UInt32Array::from(vec![
            Some(0), // first "foo"
            Some(7), // last "foo"
            None,    // null index should return null
            Some(5), // second "bar"
            Some(6), // another "bar"
            Some(2), // empty string
            Some(3), // input is null at this index
        ]);

        let result = take(&array, &indices, None).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .unwrap();

        let result_values: StringArray = result.values().data().into();

        // dictionary values should stay the same
        let expected_values = StringArray::from(vec!["foo", "bar", ""]);
        assert_eq!(&expected_values, dict_values);
        assert_eq!(&expected_values, &result_values);

        let expected_keys = Int16Array::from(vec![
            Some(0),
            Some(0),
            None,
            Some(1),
            Some(1),
            Some(2),
            None,
        ]);
        assert_eq!(result.keys(), &expected_keys);
    }
}
