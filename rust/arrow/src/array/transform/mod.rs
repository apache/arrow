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

use std::{mem::size_of, sync::Arc};

use crate::{buffer::MutableBuffer, datatypes::DataType, util::bit_util};

use super::{ArrayData, ArrayDataRef};

mod boolean;
mod list;
mod primitive;
mod utils;
mod variable_size;

type ExtendNullBits<'a> = Box<Fn(&mut _MutableArrayData, usize, usize) + 'a>;
// function that extends `[start..start+len]` to the mutable array.
// this is dynamic because different data_types influence how buffers and childs are extended.
type Extend<'a> = Box<Fn(&mut _MutableArrayData, usize, usize, usize) + 'a>;

/// A mutable [ArrayData] that knows how to freeze itself into an [ArrayData].
/// This is just a data container.
#[derive(Debug)]
struct _MutableArrayData<'a> {
    pub data_type: DataType,
    pub null_count: usize,

    pub len: usize,
    pub null_buffer: MutableBuffer,

    pub buffers: Vec<MutableBuffer>,
    pub child_data: Vec<MutableArrayData<'a>>,
}

impl<'a> _MutableArrayData<'a> {
    fn freeze(self, dictionary: Option<ArrayDataRef>) -> ArrayData {
        let mut buffers = Vec::with_capacity(self.buffers.len());
        for buffer in self.buffers {
            buffers.push(buffer.freeze());
        }

        let child_data = match self.data_type {
            DataType::Dictionary(_, _) => vec![dictionary.unwrap()],
            _ => {
                let mut child_data = Vec::with_capacity(self.child_data.len());
                for child in self.child_data {
                    child_data.push(Arc::new(child.freeze()));
                }
                child_data
            }
        };
        ArrayData::new(
            self.data_type,
            self.len,
            Some(self.null_count),
            if self.null_count > 0 {
                Some(self.null_buffer.freeze())
            } else {
                None
            },
            0,
            buffers,
            child_data,
        )
    }

    /// Returns the buffer `buffer` as a slice of type `T`. When the expected buffer is bit-packed,
    /// the slice is not offset.
    #[inline]
    pub(super) fn buffer<T>(&self, buffer: usize) -> &[T] {
        let values = unsafe { self.buffers[buffer].data().align_to::<T>() };
        if !values.0.is_empty() || !values.2.is_empty() {
            // this is unreachable because
            unreachable!("The buffer is not byte-aligned with its interpretation")
        };
        &values.1
    }
}

fn build_extend_nulls(array: &ArrayData) -> ExtendNullBits {
    if let Some(bitmap) = array.null_bitmap() {
        let bytes = bitmap.bits.data();
        Box::new(move |mutable, start, len| {
            utils::reserve_for_bits(&mut mutable.null_buffer, mutable.len + len);
            mutable.null_count += utils::set_bits(
                mutable.null_buffer.data_mut(),
                bytes,
                mutable.len,
                array.offset() + start,
                len,
            );
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

/// Struct to efficiently and interactively create an [ArrayData] from an existing [ArrayData] by
/// copying chunks.
/// The main use case of this struct is to perform unary operations to arrays of arbitrary types, such as `filter` and `take`.
/// # Example:
///
/// ```
/// use std::sync::Arc;
/// use arrow::{array::{Int32Array, Array, MutableArrayData}};
///
/// let array = Int32Array::from(vec![1, 2, 3, 4, 5]).data();
/// // Create a new `MutableArrayData` from an array and with a capacity.
/// // Capacity here is equivalent to `Vec::with_capacity`
/// let arrays = vec![array.as_ref()];
/// let mut mutable = MutableArrayData::new(arrays, 4);
/// mutable.extend(0, 1, 3); // extend from the slice [1..3], [2,3]
/// mutable.extend(0, 0, 3); // extend from the slice [0..3], [1,2,3]
/// // `.freeze()` to convert `MutableArrayData` into a `ArrayData`.
/// let new_array = Int32Array::from(Arc::new(mutable.freeze()));
/// assert_eq!(Int32Array::from(vec![2, 3, 1, 2, 3]), new_array);
/// ```
pub struct MutableArrayData<'a> {
    arrays: Vec<&'a ArrayData>,
    // The attributes in [_MutableArrayData] cannot be in [MutableArrayData] due to
    // mutability invariants (interior mutability):
    // [MutableArrayData] contains a function that can only mutate [_MutableArrayData], not
    // [MutableArrayData] itself
    data: _MutableArrayData<'a>,

    // the child data of the `Array` in Dictionary arrays.
    // This is not stored in `MutableArrayData` because these values constant and only needed
    // at the end, when freezing [_MutableArrayData].
    dictionary: Option<ArrayDataRef>,

    // the function used to extend values. This function's lifetime is bound to the array
    // because it reads values from it.
    extend_values: Vec<Extend<'a>>,
    // the function used to extend nulls. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_nulls: Vec<ExtendNullBits<'a>>,
}

impl<'a> std::fmt::Debug for MutableArrayData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ignores the closures.
        f.debug_struct("MutableArrayData")
            .field("data", &self.data)
            .finish()
    }
}

fn build_extend<'a>(array: &'a ArrayData) -> Extend<'a> {
    use crate::datatypes::*;
    match array.data_type() {
        DataType::Boolean => boolean::build_extend(array),
        DataType::UInt8 => primitive::build_extend::<u8>(array),
        DataType::UInt16 => primitive::build_extend::<u16>(array),
        DataType::UInt32 => primitive::build_extend::<u32>(array),
        DataType::UInt64 => primitive::build_extend::<u64>(array),
        DataType::Int8 => primitive::build_extend::<i8>(array),
        DataType::Int16 => primitive::build_extend::<i16>(array),
        DataType::Int32 => primitive::build_extend::<i32>(array),
        DataType::Int64 => primitive::build_extend::<i64>(array),
        DataType::Float32 => primitive::build_extend::<f32>(array),
        DataType::Float64 => primitive::build_extend::<f64>(array),
        DataType::Date32(_)
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::build_extend::<i32>(array)
        }
        DataType::Date64(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => {
            primitive::build_extend::<i64>(array)
        }
        DataType::Utf8 | DataType::Binary => variable_size::build_extend::<i32>(array),
        DataType::LargeUtf8 | DataType::LargeBinary => {
            variable_size::build_extend::<i64>(array)
        }
        DataType::List(_) => list::build_extend::<i32>(array),
        DataType::LargeList(_) => list::build_extend::<i64>(array),
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => primitive::build_extend::<u8>(array),
            DataType::UInt16 => primitive::build_extend::<u16>(array),
            DataType::UInt32 => primitive::build_extend::<u32>(array),
            DataType::UInt64 => primitive::build_extend::<u64>(array),
            DataType::Int8 => primitive::build_extend::<i8>(array),
            DataType::Int16 => primitive::build_extend::<i16>(array),
            DataType::Int32 => primitive::build_extend::<i32>(array),
            DataType::Int64 => primitive::build_extend::<i64>(array),
            _ => unreachable!(),
        },
        DataType::Float16 => unreachable!(),
        /*
        DataType::Null => {}
        DataType::FixedSizeBinary(_) => {}
        DataType::FixedSizeList(_, _) => {}
        DataType::Struct(_) => {}
        DataType::Union(_) => {}
        */
        _ => todo!("Take and filter operations still not supported for this datatype"),
    }
}

impl<'a> MutableArrayData<'a> {
    /// returns a new [MutableArrayData] with capacity to `capacity` slots and specialized to create an
    /// [ArrayData] from `array`
    pub fn new(arrays: Vec<&'a ArrayData>, capacity: usize) -> Self {
        let data_type = arrays[0].data_type();
        use crate::datatypes::*;

        let buffers = match &data_type {
            DataType::Boolean => {
                let bytes = bit_util::ceil(capacity, 8);
                let buffer = MutableBuffer::new(bytes).with_bitset(bytes, false);
                vec![buffer]
            }
            DataType::UInt8 => vec![MutableBuffer::new(capacity * size_of::<u8>())],
            DataType::UInt16 => vec![MutableBuffer::new(capacity * size_of::<u16>())],
            DataType::UInt32 => vec![MutableBuffer::new(capacity * size_of::<u32>())],
            DataType::UInt64 => vec![MutableBuffer::new(capacity * size_of::<u64>())],
            DataType::Int8 => vec![MutableBuffer::new(capacity * size_of::<i8>())],
            DataType::Int16 => vec![MutableBuffer::new(capacity * size_of::<i16>())],
            DataType::Int32 => vec![MutableBuffer::new(capacity * size_of::<i32>())],
            DataType::Int64 => vec![MutableBuffer::new(capacity * size_of::<i64>())],
            DataType::Float32 => vec![MutableBuffer::new(capacity * size_of::<f32>())],
            DataType::Float64 => vec![MutableBuffer::new(capacity * size_of::<f64>())],
            DataType::Date32(_) | DataType::Time32(_) => {
                vec![MutableBuffer::new(capacity * size_of::<i32>())]
            }
            DataType::Date64(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _) => {
                vec![MutableBuffer::new(capacity * size_of::<i64>())]
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                vec![MutableBuffer::new(capacity * size_of::<i32>())]
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                vec![MutableBuffer::new(capacity * size_of::<i64>())]
            }
            DataType::Utf8 | DataType::Binary => {
                let mut buffer = MutableBuffer::new((1 + capacity) * size_of::<i32>());
                buffer.extend_from_slice(&[0i32].to_byte_slice());
                vec![buffer, MutableBuffer::new(capacity * size_of::<u8>())]
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                let mut buffer = MutableBuffer::new((1 + capacity) * size_of::<i64>());
                buffer.extend_from_slice(&[0i64].to_byte_slice());
                vec![buffer, MutableBuffer::new(capacity * size_of::<u8>())]
            }
            DataType::List(_) => {
                // offset buffer always starts with a zero
                let mut buffer = MutableBuffer::new((1 + capacity) * size_of::<i32>());
                buffer.extend_from_slice(0i32.to_byte_slice());
                vec![buffer]
            }
            DataType::LargeList(_) => {
                // offset buffer always starts with a zero
                let mut buffer = MutableBuffer::new((1 + capacity) * size_of::<i64>());
                buffer.extend_from_slice(&[0i64].to_byte_slice());
                vec![buffer]
            }
            DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
                DataType::UInt8 => vec![MutableBuffer::new(capacity * size_of::<u8>())],
                DataType::UInt16 => vec![MutableBuffer::new(capacity * size_of::<u16>())],
                DataType::UInt32 => vec![MutableBuffer::new(capacity * size_of::<u32>())],
                DataType::UInt64 => vec![MutableBuffer::new(capacity * size_of::<u64>())],
                DataType::Int8 => vec![MutableBuffer::new(capacity * size_of::<i8>())],
                DataType::Int16 => vec![MutableBuffer::new(capacity * size_of::<i16>())],
                DataType::Int32 => vec![MutableBuffer::new(capacity * size_of::<i32>())],
                DataType::Int64 => vec![MutableBuffer::new(capacity * size_of::<i64>())],
                _ => unreachable!(),
            },
            DataType::Float16 => unreachable!(),
            _ => {
                todo!("Take and filter operations still not supported for this datatype")
            }
        };

        let child_data = match &data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(_) => vec![],
            DataType::List(_) | DataType::LargeList(_) => {
                let childs = arrays
                    .iter()
                    .map(|array| array.child_data()[0].as_ref())
                    .collect::<Vec<_>>();
                vec![MutableArrayData::new(childs, capacity)]
            }
            // the dictionary type just appends keys and clones the values.
            DataType::Dictionary(_, _) => vec![],
            DataType::Float16 => unreachable!(),
            _ => {
                todo!("Take and filter operations still not supported for this datatype")
            }
        };

        let dictionary = match &data_type {
            DataType::Dictionary(_, _) => Some(arrays[0].child_data()[0].clone()),
            _ => None,
        };

        let extend_nulls = arrays
            .iter()
            .map(|array| build_extend_nulls(array))
            .collect();

        let null_bytes = bit_util::ceil(capacity, 8);
        let null_buffer = MutableBuffer::new(null_bytes).with_bitset(null_bytes, false);

        let extend_values = arrays.iter().map(|array| build_extend(array)).collect();

        let data = _MutableArrayData {
            data_type: data_type.clone(),
            len: 0,
            null_count: 0,
            null_buffer,
            buffers,
            child_data,
        };
        Self {
            arrays: arrays.to_vec(),
            data,
            dictionary,
            extend_values,
            extend_nulls,
        }
    }

    /// Extends this [MutableArrayData] with elements from the bounded [ArrayData] at `start`
    /// and for a size of `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    pub fn extend(&mut self, index: usize, start: usize, end: usize) {
        let len = end - start;
        (self.extend_nulls[index])(&mut self.data, start, len);
        (self.extend_values[index])(&mut self.data, index, start, len);
        self.data.len += len;
    }

    /// Creates a [ArrayData] from the pushed regions up to this point, consuming `self`.
    pub fn freeze(self) -> ArrayData {
        self.data.freeze(self.dictionary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::{
        Array, ArrayDataRef, BooleanArray, DictionaryArray, Int16Array, Int16Type,
        Int64Builder, ListBuilder, PrimitiveBuilder, StringArray,
        StringDictionaryBuilder, UInt8Array,
    };
    use crate::{array::ListArray, error::Result};

    /// tests extending from a primitive array w/ offset nor nulls
    #[test]
    fn test_primitive() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]).data();
        let arrays = vec![b.as_ref()];
        let mut a = MutableArrayData::new(arrays, 3);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(Arc::new(result));
        let expected = UInt8Array::from(vec![Some(1), Some(2)]);
        assert_eq!(array, expected);
    }

    /// tests extending from a primitive array with offset w/ nulls
    #[test]
    fn test_primitive_offset() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
        let b = b.slice(1, 2).data();
        let arrays = vec![b.as_ref()];
        let mut a = MutableArrayData::new(arrays, 2);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(Arc::new(result));
        let expected = UInt8Array::from(vec![Some(2), Some(3)]);
        assert_eq!(array, expected);
    }

    /// tests extending from a primitive array with offset and nulls
    #[test]
    fn test_primitive_null_offset() {
        let b = UInt8Array::from(vec![Some(1), None, Some(3)]);
        let b = b.slice(1, 2).data();
        let arrays = vec![b.as_ref()];
        let mut a = MutableArrayData::new(arrays, 2);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(Arc::new(result));
        let expected = UInt8Array::from(vec![None, Some(3)]);
        assert_eq!(array, expected);
    }

    #[test]
    fn test_list_null_offset() -> Result<()> {
        let int_builder = Int64Builder::new(24);
        let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
        builder.values().append_slice(&[1, 2, 3])?;
        builder.append(true)?;
        builder.values().append_slice(&[4, 5])?;
        builder.append(true)?;
        builder.values().append_slice(&[6, 7, 8])?;
        builder.append(true)?;
        let array = builder.finish().data();
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, 0);
        mutable.extend(0, 0, 1);

        let result = mutable.freeze();
        let array = ListArray::from(Arc::new(result));

        let int_builder = Int64Builder::new(24);
        let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
        builder.values().append_slice(&[1, 2, 3])?;
        builder.append(true)?;
        let expected = builder.finish();

        assert_eq!(array, expected);

        Ok(())
    }

    /// tests extending from a variable-sized (strings and binary) array w/ offset with nulls
    #[test]
    fn test_variable_sized_nulls() {
        let array =
            StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]).data();
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected = StringArray::from(vec![Some("bc"), None]);
        assert_eq!(result, expected);
    }

    /// tests extending from a variable-sized (strings and binary) array
    /// with an offset and nulls
    #[test]
    fn test_variable_sized_offsets() {
        let array =
            StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]).data();
        let array = array.slice(1, 3);

        let arrays = vec![&array];

        let mut mutable = MutableArrayData::new(arrays, 0);

        mutable.extend(0, 0, 3);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bool() {
        let array =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]).data();
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = BooleanArray::from(Arc::new(result));

        let expected = BooleanArray::from(vec![Some(true), None]);
        assert_eq!(result, expected);
    }

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayDataRef {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::new_with_dictionary(
            PrimitiveBuilder::<Int16Type>::new(3),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null().unwrap()
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_dictionary() {
        // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
        let array = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), Some("b"), None, Some("c")],
        );
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = DictionaryArray::from(Arc::new(result));

        let expected = Int16Array::from(vec![Some(1), None]);
        assert_eq!(result.keys(), &expected);
    }
}
