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

use std::fmt;
use std::sync::Arc;
use std::{any::Any, convert::TryFrom};

use super::*;
use crate::array::equal_json::JsonEqual;
use crate::buffer::{Buffer, MutableBuffer};
use crate::error::Result;
use crate::ffi;

/// Trait for dealing with different types of array at runtime when the type of the
/// array is not known in advance.
pub trait Array: fmt::Debug + Send + Sync + JsonEqual {
    /// Returns the array as [`Any`](std::any::Any) so that it can be
    /// downcasted to a specific implementation.
    ///
    /// # Example:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
    ///     vec![Arc::new(id)]
    /// )?;
    ///
    /// let int32array = batch
    ///     .column(0)
    ///     .as_any()
    ///     .downcast_ref::<Int32Array>()
    ///     .expect("Failed to downcast");
    /// # Ok(())
    /// # }
    /// ```
    fn as_any(&self) -> &Any;

    /// Returns a reference to the underlying data of this array.
    fn data(&self) -> &ArrayData;

    /// Returns a reference-counted pointer to the underlying data of this array.
    fn data_ref(&self) -> &ArrayData {
        self.data()
    }

    /// Returns a reference to the [`DataType`](crate::datatypes::DataType) of this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::datatypes::DataType;
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(*array.data_type(), DataType::Int32);
    /// ```
    fn data_type(&self) -> &DataType {
        self.data_ref().data_type()
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// // Make slice over the values [2, 3, 4]
    /// let array_slice = array.slice(1, 3);
    ///
    /// assert_eq!(array_slice.as_ref(), &Int32Array::from(vec![2, 3, 4]));
    /// ```
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        make_array(self.data_ref().slice(offset, length))
    }

    /// Returns the length (i.e., number of elements) of this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(array.len(), 5);
    /// ```
    fn len(&self) -> usize {
        self.data_ref().len()
    }

    /// Returns whether this array is empty.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(array.is_empty(), false);
    /// ```
    fn is_empty(&self) -> bool {
        self.data_ref().is_empty()
    }

    /// Returns the offset into the underlying data used by this array(-slice).
    /// Note that the underlying data can be shared by many arrays.
    /// This defaults to `0`.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// // Make slice over the values [2, 3, 4]
    /// let array_slice = array.slice(1, 3);
    ///
    /// assert_eq!(array.offset(), 0);
    /// assert_eq!(array_slice.offset(), 1);
    /// ```
    fn offset(&self) -> usize {
        self.data_ref().offset()
    }

    /// Returns whether the element at `index` is null.
    /// When using this function on a slice, the index is relative to the slice.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![Some(1), None]);
    ///
    /// assert_eq!(array.is_null(0), false);
    /// assert_eq!(array.is_null(1), true);
    /// ```
    fn is_null(&self, index: usize) -> bool {
        self.data_ref().is_null(index)
    }

    /// Returns whether the element at `index` is not null.
    /// When using this function on a slice, the index is relative to the slice.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![Some(1), None]);
    ///
    /// assert_eq!(array.is_valid(0), true);
    /// assert_eq!(array.is_valid(1), false);
    /// ```
    fn is_valid(&self, index: usize) -> bool {
        self.data_ref().is_valid(index)
    }

    /// Returns the total number of null values in this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow::array::{Array, Int32Array};
    ///
    /// // Construct an array with values [1, NULL, NULL]
    /// let array = Int32Array::from(vec![Some(1), None, None]);
    ///
    /// assert_eq!(array.null_count(), 2);
    /// ```
    fn null_count(&self) -> usize {
        self.data_ref().null_count()
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this array.
    fn get_buffer_memory_size(&self) -> usize;

    /// Returns the total number of bytes of memory occupied physically by this array.
    fn get_array_memory_size(&self) -> usize;

    /// returns two pointers that represent this array in the C Data Interface (FFI)
    fn to_raw(
        &self,
    ) -> Result<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)> {
        let data = self.data().clone();
        let array = ffi::ArrowArray::try_from(data)?;
        Ok(ffi::ArrowArray::into_raw(array))
    }
}

/// A reference-counted reference to a generic `Array`.
pub type ArrayRef = Arc<Array>;

/// Constructs an array using the input `data`.
/// Returns a reference-counted `Array` instance.
pub fn make_array(data: ArrayData) -> ArrayRef {
    match data.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float16 => panic!("Float16 datatype not supported"),
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Date32 => Arc::new(Date32Array::from(data)) as ArrayRef,
        DataType::Date64 => Arc::new(Date64Array::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(Time32SecondArray::from(data)) as ArrayRef
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Binary => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(data)) as ArrayRef,
        DataType::FixedSizeBinary(_) => {
            Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef
        }
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::LargeList(_) => Arc::new(LargeListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        DataType::Union(_) => Arc::new(UnionArray::from(data)) as ArrayRef,
        DataType::FixedSizeList(_, _) => {
            Arc::new(FixedSizeListArray::from(data)) as ArrayRef
        }
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => {
                Arc::new(DictionaryArray::<Int8Type>::from(data)) as ArrayRef
            }
            DataType::Int16 => {
                Arc::new(DictionaryArray::<Int16Type>::from(data)) as ArrayRef
            }
            DataType::Int32 => {
                Arc::new(DictionaryArray::<Int32Type>::from(data)) as ArrayRef
            }
            DataType::Int64 => {
                Arc::new(DictionaryArray::<Int64Type>::from(data)) as ArrayRef
            }
            DataType::UInt8 => {
                Arc::new(DictionaryArray::<UInt8Type>::from(data)) as ArrayRef
            }
            DataType::UInt16 => {
                Arc::new(DictionaryArray::<UInt16Type>::from(data)) as ArrayRef
            }
            DataType::UInt32 => {
                Arc::new(DictionaryArray::<UInt32Type>::from(data)) as ArrayRef
            }
            DataType::UInt64 => {
                Arc::new(DictionaryArray::<UInt64Type>::from(data)) as ArrayRef
            }
            dt => panic!("Unexpected dictionary key type {:?}", dt),
        },
        DataType::Null => Arc::new(NullArray::from(data)) as ArrayRef,
        DataType::Decimal(_, _) => Arc::new(DecimalArray::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
    }
}

/// Creates a new empty array
pub fn new_empty_array(data_type: &DataType) -> ArrayRef {
    let data = ArrayData::new_empty(data_type);
    make_array(data)
}
/// Creates a new array of `data_type` of length `length` filled entirely of `NULL` values
pub fn new_null_array(data_type: &DataType, length: usize) -> ArrayRef {
    // context: https://github.com/apache/arrow/pull/9469#discussion_r574761687
    match data_type {
        DataType::Null => Arc::new(NullArray::new(length)),
        DataType::Boolean => {
            let null_buf: Buffer = MutableBuffer::new_null(length).into();
            make_array(ArrayData::new(
                data_type.clone(),
                length,
                Some(length),
                Some(null_buf.clone()),
                0,
                vec![null_buf],
                vec![],
            ))
        }
        DataType::Int8 => new_null_sized_array::<Int8Type>(data_type, length),
        DataType::UInt8 => new_null_sized_array::<UInt8Type>(data_type, length),
        DataType::Int16 => new_null_sized_array::<Int16Type>(data_type, length),
        DataType::UInt16 => new_null_sized_array::<UInt16Type>(data_type, length),
        DataType::Float16 => unreachable!(),
        DataType::Int32 => new_null_sized_array::<Int32Type>(data_type, length),
        DataType::UInt32 => new_null_sized_array::<UInt32Type>(data_type, length),
        DataType::Float32 => new_null_sized_array::<Float32Type>(data_type, length),
        DataType::Date32 => new_null_sized_array::<Date32Type>(data_type, length),
        // expanding this into Date23{unit}Type results in needless branching
        DataType::Time32(_) => new_null_sized_array::<Int32Type>(data_type, length),
        DataType::Int64 => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::UInt64 => new_null_sized_array::<UInt64Type>(data_type, length),
        DataType::Float64 => new_null_sized_array::<Float64Type>(data_type, length),
        DataType::Date64 => new_null_sized_array::<Date64Type>(data_type, length),
        // expanding this into Timestamp{unit}Type results in needless branching
        DataType::Timestamp(_, _) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Time64(_) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Duration(_) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => {
                new_null_sized_array::<IntervalYearMonthType>(data_type, length)
            }
            IntervalUnit::DayTime => {
                new_null_sized_array::<IntervalDayTimeType>(data_type, length)
            }
        },
        DataType::FixedSizeBinary(value_len) => make_array(ArrayData::new(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![Buffer::from(vec![0u8; *value_len as usize * length])],
            vec![],
        )),
        DataType::Binary | DataType::Utf8 => {
            new_null_binary_array::<i32>(data_type, length)
        }
        DataType::LargeBinary | DataType::LargeUtf8 => {
            new_null_binary_array::<i64>(data_type, length)
        }
        DataType::List(field) => {
            new_null_list_array::<i32>(data_type, field.data_type(), length)
        }
        DataType::LargeList(field) => {
            new_null_list_array::<i64>(data_type, field.data_type(), length)
        }
        DataType::FixedSizeList(field, value_len) => make_array(ArrayData::new(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![],
            vec![
                new_null_array(field.data_type(), *value_len as usize * length)
                    .data()
                    .clone(),
            ],
        )),
        DataType::Struct(fields) => make_array(ArrayData::new(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![],
            fields
                .iter()
                .map(|field| ArrayData::new_empty(field.data_type()))
                .collect(),
        )),
        DataType::Union(_) => {
            unimplemented!("Creating null Union array not yet supported")
        }
        DataType::Dictionary(_, value) => {
            make_array(ArrayData::new(
                data_type.clone(),
                length,
                Some(length),
                Some(MutableBuffer::new_null(length).into()),
                0,
                vec![MutableBuffer::new(0).into()], // values are empty
                vec![new_empty_array(value.as_ref()).data().clone()],
            ))
        }
        DataType::Decimal(_, _) => {
            unimplemented!("Creating null Decimal array not yet supported")
        }
    }
}

#[inline]
fn new_null_list_array<OffsetSize: OffsetSizeTrait>(
    data_type: &DataType,
    child_data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(ArrayData::new(
        data_type.clone(),
        length,
        Some(length),
        Some(MutableBuffer::new_null(length).into()),
        0,
        vec![Buffer::from(
            vec![OffsetSize::zero(); length + 1].to_byte_slice(),
        )],
        vec![ArrayData::new_empty(child_data_type)],
    ))
}

#[inline]
fn new_null_binary_array<OffsetSize: OffsetSizeTrait>(
    data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(ArrayData::new(
        data_type.clone(),
        length,
        Some(length),
        Some(MutableBuffer::new_null(length).into()),
        0,
        vec![
            Buffer::from(vec![OffsetSize::zero(); length + 1].to_byte_slice()),
            MutableBuffer::new(0).into(),
        ],
        vec![],
    ))
}

#[inline]
fn new_null_sized_array<T: ArrowPrimitiveType>(
    data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(ArrayData::new(
        data_type.clone(),
        length,
        Some(length),
        Some(MutableBuffer::new_null(length).into()),
        0,
        vec![Buffer::from(vec![0u8; length * T::get_byte_width()])],
        vec![],
    ))
}

/// Creates a new array from two FFI pointers. Used to import arrays from the C Data Interface
/// # Safety
/// Assumes that these pointers represent valid C Data Interfaces, both in memory
/// representation and lifetime via the `release` mechanism.
pub unsafe fn make_array_from_raw(
    array: *const ffi::FFI_ArrowArray,
    schema: *const ffi::FFI_ArrowSchema,
) -> Result<ArrayRef> {
    let array = ffi::ArrowArray::try_from_raw(array, schema)?;
    let data = ArrayData::try_from(array)?;
    Ok(make_array(data))
}
// Helper function for printing potentially long arrays.
pub(super) fn print_long_array<A, F>(
    array: &A,
    f: &mut fmt::Formatter,
    print_item: F,
) -> fmt::Result
where
    A: Array,
    F: Fn(&A, usize, &mut fmt::Formatter) -> fmt::Result,
{
    let head = std::cmp::min(10, array.len());

    for i in 0..head {
        if array.is_null(i) {
            writeln!(f, "  null,")?;
        } else {
            write!(f, "  ")?;
            print_item(&array, i, f)?;
            writeln!(f, ",")?;
        }
    }
    if array.len() > 10 {
        if array.len() > 20 {
            writeln!(f, "  ...{} elements...,", array.len() - 20)?;
        }

        let tail = std::cmp::max(head, array.len() - 10);

        for i in tail..array.len() {
            if array.is_null(i) {
                writeln!(f, "  null,")?;
            } else {
                write!(f, "  ")?;
                print_item(&array, i, f)?;
                writeln!(f, ",")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_empty_primitive() {
        let array = new_empty_array(&DataType::Int32);
        let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 0);
        let expected: &[i32] = &[];
        assert_eq!(a.values(), expected);
    }

    #[test]
    fn test_empty_variable_sized() {
        let array = new_empty_array(&DataType::Utf8);
        let a = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.len(), 0);
        assert_eq!(a.value_offsets()[0], 0i32);
    }

    #[test]
    fn test_empty_list_primitive() {
        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
        let array = new_empty_array(&data_type);
        let a = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(a.len(), 0);
        assert_eq!(a.value_offsets()[0], 0i32);
    }

    #[test]
    fn test_null_boolean() {
        let array = new_null_array(&DataType::Boolean, 9);
        let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(a.len(), 9);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_primitive() {
        let array = new_null_array(&DataType::Int32, 9);
        let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 9);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_variable_sized() {
        let array = new_null_array(&DataType::Utf8, 9);
        let a = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.value_offsets()[9], 0i32);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_list_primitive() {
        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let array = new_null_array(&data_type, 9);
        let a = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.value_offsets()[9], 0i32);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_dictionary() {
        let values = vec![None, None, None, None, None, None, None, None, None]
            as Vec<Option<&str>>;

        let array: DictionaryArray<Int8Type> = values.into_iter().collect();
        let array = Arc::new(array) as ArrayRef;

        let null_array = new_null_array(array.data_type(), 9);
        assert_eq!(&array, &null_array);
    }
}
