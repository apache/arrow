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

use std::sync::Arc;

use crate::{buffer::MutableBuffer, datatypes::DataType, util::bit_util};

use super::{
    data::{into_buffers, new_buffers},
    ArrayData, ArrayDataRef,
};

mod boolean;
mod fixed_binary;
mod list;
mod null;
mod primitive;
mod structure;
mod utils;
mod variable_size;

type ExtendNullBits<'a> = Box<Fn(&mut _MutableArrayData, usize, usize) + 'a>;
// function that extends `[start..start+len]` to the mutable array.
// this is dynamic because different data_types influence how buffers and childs are extended.
type Extend<'a> = Box<Fn(&mut _MutableArrayData, usize, usize, usize) + 'a>;

type ExtendNulls = Box<Fn(&mut _MutableArrayData, usize) -> ()>;

/// A mutable [ArrayData] that knows how to freeze itself into an [ArrayData].
/// This is just a data container.
#[derive(Debug)]
struct _MutableArrayData<'a> {
    pub data_type: DataType,
    pub null_count: usize,

    pub len: usize,
    pub null_buffer: MutableBuffer,

    // arrow specification only allows up to 3 buffers (2 ignoring the nulls above).
    // Thus, we place them in the stack to avoid bound checks and greater data locality.
    pub buffer1: MutableBuffer,
    pub buffer2: MutableBuffer,
    pub child_data: Vec<MutableArrayData<'a>>,
}

impl<'a> _MutableArrayData<'a> {
    fn freeze(self, dictionary: Option<ArrayDataRef>) -> ArrayData {
        let buffers = into_buffers(&self.data_type, self.buffer1, self.buffer2);

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
                Some(self.null_buffer.into())
            } else {
                None
            },
            0,
            buffers,
            child_data,
        )
    }
}

fn build_extend_null_bits(array: &ArrayData, use_nulls: bool) -> ExtendNullBits {
    if let Some(bitmap) = array.null_bitmap() {
        let bytes = bitmap.bits.as_slice();
        Box::new(move |mutable, start, len| {
            utils::resize_for_bits(&mut mutable.null_buffer, mutable.len + len);
            mutable.null_count += utils::set_bits(
                mutable.null_buffer.as_slice_mut(),
                bytes,
                mutable.len,
                array.offset() + start,
                len,
            );
        })
    } else if use_nulls {
        Box::new(|mutable, _, len| {
            utils::resize_for_bits(&mut mutable.null_buffer, mutable.len + len);
            let write_data = mutable.null_buffer.as_slice_mut();
            let offset = mutable.len;
            (0..len).for_each(|i| {
                bit_util::set_bit(write_data, offset + i);
            });
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
/// // Create a new `MutableArrayData` from an array and with a capacity of 4.
/// // Capacity here is equivalent to `Vec::with_capacity`
/// let arrays = vec![array.as_ref()];
/// let mut mutable = MutableArrayData::new(arrays, false, 4);
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

    // function used to extend values from arrays. This function's lifetime is bound to the array
    // because it reads values from it.
    extend_values: Vec<Extend<'a>>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,

    // function used to extend nulls.
    // this is independent of the arrays and therefore has no lifetime.
    extend_nulls: ExtendNulls,
}

impl<'a> std::fmt::Debug for MutableArrayData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ignores the closures.
        f.debug_struct("MutableArrayData")
            .field("data", &self.data)
            .finish()
    }
}

fn build_extend(array: &ArrayData) -> Extend {
    use crate::datatypes::*;
    match array.data_type() {
        DataType::Null => null::build_extend(array),
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
        DataType::Struct(_) => structure::build_extend(array),
        DataType::FixedSizeBinary(_) => fixed_binary::build_extend(array),
        DataType::Float16 => unreachable!(),
        /*
        DataType::FixedSizeList(_, _) => {}
        DataType::Union(_) => {}
        */
        _ => todo!("Take and filter operations still not supported for this datatype"),
    }
}

fn build_extend_nulls(data_type: &DataType) -> ExtendNulls {
    use crate::datatypes::*;
    Box::new(match data_type {
        DataType::Null => null::extend_nulls,
        DataType::Boolean => boolean::extend_nulls,
        DataType::UInt8 => primitive::extend_nulls::<u8>,
        DataType::UInt16 => primitive::extend_nulls::<u16>,
        DataType::UInt32 => primitive::extend_nulls::<u32>,
        DataType::UInt64 => primitive::extend_nulls::<u64>,
        DataType::Int8 => primitive::extend_nulls::<i8>,
        DataType::Int16 => primitive::extend_nulls::<i16>,
        DataType::Int32 => primitive::extend_nulls::<i32>,
        DataType::Int64 => primitive::extend_nulls::<i64>,
        DataType::Float32 => primitive::extend_nulls::<f32>,
        DataType::Float64 => primitive::extend_nulls::<f64>,
        DataType::Date32(_)
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => primitive::extend_nulls::<i32>,
        DataType::Date64(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::extend_nulls::<i64>,
        DataType::Utf8 | DataType::Binary => variable_size::extend_nulls::<i32>,
        DataType::LargeUtf8 | DataType::LargeBinary => variable_size::extend_nulls::<i64>,
        DataType::List(_) => list::extend_nulls::<i32>,
        DataType::LargeList(_) => list::extend_nulls::<i64>,
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => primitive::extend_nulls::<u8>,
            DataType::UInt16 => primitive::extend_nulls::<u16>,
            DataType::UInt32 => primitive::extend_nulls::<u32>,
            DataType::UInt64 => primitive::extend_nulls::<u64>,
            DataType::Int8 => primitive::extend_nulls::<i8>,
            DataType::Int16 => primitive::extend_nulls::<i16>,
            DataType::Int32 => primitive::extend_nulls::<i32>,
            DataType::Int64 => primitive::extend_nulls::<i64>,
            _ => unreachable!(),
        },
        DataType::Struct(_) => structure::extend_nulls,
        DataType::FixedSizeBinary(_) => fixed_binary::extend_nulls,
        DataType::Float16 => unreachable!(),
        /*
        DataType::FixedSizeList(_, _) => {}
        DataType::Union(_) => {}
        */
        _ => todo!("Take and filter operations still not supported for this datatype"),
    })
}

impl<'a> MutableArrayData<'a> {
    /// returns a new [MutableArrayData] with capacity to `capacity` slots and specialized to create an
    /// [ArrayData] from multiple `arrays`.
    ///
    /// `use_nulls` is a flag used to optimize insertions. It should be `false` if the only source of nulls
    /// are the arrays themselves and `true` if the user plans to call [MutableArrayData::extend_nulls].
    /// In other words, if `use_nulls` is `false`, calling [MutableArrayData::extend_nulls] should not be used.
    pub fn new(arrays: Vec<&'a ArrayData>, mut use_nulls: bool, capacity: usize) -> Self {
        let data_type = arrays[0].data_type();
        use crate::datatypes::*;

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_nulls = true;
        };

        let [buffer1, buffer2] = new_buffers(data_type, capacity);

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
                vec![MutableArrayData::new(childs, use_nulls, capacity)]
            }
            // the dictionary type just appends keys and clones the values.
            DataType::Dictionary(_, _) => vec![],
            DataType::Float16 => unreachable!(),
            DataType::Struct(fields) => (0..fields.len())
                .map(|i| {
                    let child_arrays = arrays
                        .iter()
                        .map(|array| array.child_data()[i].as_ref())
                        .collect::<Vec<_>>();
                    MutableArrayData::new(child_arrays, use_nulls, capacity)
                })
                .collect::<Vec<_>>(),
            _ => {
                todo!("Take and filter operations still not supported for this datatype")
            }
        };

        let dictionary = match &data_type {
            DataType::Dictionary(_, _) => Some(arrays[0].child_data()[0].clone()),
            _ => None,
        };

        let extend_nulls = build_extend_nulls(data_type);

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(array, use_nulls))
            .collect();

        let null_bytes = bit_util::ceil(capacity, 8);
        let null_buffer = MutableBuffer::from_len_zeroed(null_bytes);

        let extend_values = arrays.iter().map(|array| build_extend(array)).collect();

        let data = _MutableArrayData {
            data_type: data_type.clone(),
            len: 0,
            null_count: 0,
            null_buffer,
            buffer1,
            buffer2,
            child_data,
        };
        Self {
            arrays,
            data,
            dictionary,
            extend_values,
            extend_null_bits,
            extend_nulls,
        }
    }

    /// Extends this [MutableArrayData] with elements from the bounded [ArrayData] at `start`
    /// and for a size of `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    pub fn extend(&mut self, index: usize, start: usize, end: usize) {
        let len = end - start;
        (self.extend_null_bits[index])(&mut self.data, start, len);
        (self.extend_values[index])(&mut self.data, index, start, len);
        self.data.len += len;
    }

    /// Extends this [MutableArrayData] with null elements, disregarding the bound arrays
    pub fn extend_nulls(&mut self, len: usize) {
        self.data.null_count += len;
        (self.extend_nulls)(&mut self.data, len);
        self.data.len += len;
    }

    /// Creates a [ArrayData] from the pushed regions up to this point, consuming `self`.
    pub fn freeze(self) -> ArrayData {
        self.data.freeze(self.dictionary)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;

    use crate::{
        array::{
            Array, ArrayDataRef, ArrayRef, BooleanArray, DictionaryArray,
            FixedSizeBinaryArray, Int16Array, Int16Type, Int32Array, Int64Array,
            Int64Builder, ListBuilder, NullArray, PrimitiveBuilder, StringArray,
            StringDictionaryBuilder, StructArray, UInt8Array,
        },
        buffer::Buffer,
        datatypes::Field,
    };
    use crate::{
        array::{ListArray, StringBuilder},
        error::Result,
    };

    /// tests extending from a primitive array w/ offset nor nulls
    #[test]
    fn test_primitive() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]).data();
        let arrays = vec![b.as_ref()];
        let mut a = MutableArrayData::new(arrays, false, 3);
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
        let mut a = MutableArrayData::new(arrays, false, 2);
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
        let mut a = MutableArrayData::new(arrays, false, 2);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(Arc::new(result));
        let expected = UInt8Array::from(vec![None, Some(3)]);
        assert_eq!(array, expected);
    }

    #[test]
    fn test_primitive_null_offset_nulls() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
        let b = b.slice(1, 2).data();
        let arrays = vec![b.as_ref()];
        let mut a = MutableArrayData::new(arrays, true, 2);
        a.extend(0, 0, 2);
        a.extend_nulls(3);
        a.extend(0, 1, 2);
        let result = a.freeze();
        let array = UInt8Array::from(Arc::new(result));
        let expected =
            UInt8Array::from(vec![Some(2), Some(3), None, None, None, Some(3)]);
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

        let mut mutable = MutableArrayData::new(arrays, false, 0);
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

        let mut mutable = MutableArrayData::new(arrays, false, 0);

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

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 0, 3);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_offsets() {
        let array =
            StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]).data();
        let array = array.slice(1, 3);

        let arrays = vec![&array];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 0, 3);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_multiple_with_nulls() {
        let array1 = StringArray::from(vec!["hello", "world"]).data();
        let array2 = StringArray::from(vec![Some("1"), None]).data();

        let arrays = vec![array1.as_ref(), array2.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, false, 5);

        mutable.extend(0, 0, 2);
        mutable.extend(1, 0, 2);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected =
            StringArray::from(vec![Some("hello"), Some("world"), Some("1"), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_null_offset_nulls() {
        let array =
            StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]).data();
        let array = array.slice(1, 3);

        let arrays = vec![&array];

        let mut mutable = MutableArrayData::new(arrays, true, 0);

        mutable.extend(0, 1, 3);
        mutable.extend_nulls(1);

        let result = mutable.freeze();
        let result = StringArray::from(Arc::new(result));

        let expected = StringArray::from(vec![None, Some("defh"), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bool() {
        let array =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]).data();
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = BooleanArray::from(Arc::new(result));

        let expected = BooleanArray::from(vec![Some(true), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_null() {
        let array1 = NullArray::new(10).data();
        let array2 = NullArray::new(5).data();
        let arrays = vec![array1.as_ref(), array2.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        mutable.extend(1, 0, 1);

        let result = mutable.freeze();
        let result = NullArray::from(Arc::new(result));

        let expected = NullArray::new(3);
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

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = DictionaryArray::from(Arc::new(result));

        let expected = Int16Array::from(vec![Some(1), None]);
        assert_eq!(result.keys(), &expected);
    }

    #[test]
    fn test_struct() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap()
                .data();
        let arrays = vec![array.as_ref()];
        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        let data = mutable.freeze();
        let array = StructArray::from(Arc::new(data));

        let expected = StructArray::try_from(vec![
            ("f1", strings.slice(1, 2)),
            ("f2", ints.slice(1, 2)),
        ])
        .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_struct_nulls() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap()
                .data();
        let arrays = vec![array.as_ref()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        let data = mutable.freeze();
        let array = StructArray::from(Arc::new(data));

        let expected_string = Arc::new(StringArray::from(vec![None, None])) as ArrayRef;
        let expected_int = Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef;

        let expected =
            StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)])
                .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_struct_many() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap()
                .data();
        let arrays = vec![array.as_ref(), array.as_ref()];
        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        mutable.extend(1, 0, 2);
        let data = mutable.freeze();
        let array = StructArray::from(Arc::new(data));

        let expected_string =
            Arc::new(StringArray::from(vec![None, None, Some("joe"), None])) as ArrayRef;
        let expected_int =
            Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)])) as ArrayRef;

        let expected =
            StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)])
                .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_binary_fixed_sized_offsets() {
        let array =
            FixedSizeBinaryArray::from(vec![vec![0, 0], vec![0, 1], vec![0, 2]]).data();
        let array = array.slice(1, 2);
        // = [[0, 1], [0, 2]] due to the offset = 1

        let arrays = vec![&array];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 2);
        mutable.extend(0, 0, 1);

        let result = mutable.freeze();
        let result = FixedSizeBinaryArray::from(Arc::new(result));

        let expected = FixedSizeBinaryArray::from(vec![vec![0, 2], vec![0, 1]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_list_append() -> Result<()> {
        let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::new(24));
        builder.values().append_slice(&[1, 2, 3])?;
        builder.append(true)?;
        builder.values().append_slice(&[4, 5])?;
        builder.append(true)?;
        builder.values().append_slice(&[6, 7, 8])?;
        builder.values().append_slice(&[9, 10, 11])?;
        builder.append(true)?;
        let a = builder.finish().data();

        let a_builder = Int64Builder::new(24);
        let mut a_builder = ListBuilder::<Int64Builder>::new(a_builder);
        a_builder.values().append_slice(&[12, 13])?;
        a_builder.append(true)?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[14, 15])?;
        a_builder.append(true)?;
        let b = a_builder.finish();

        let b = b.data();
        let c = b.slice(1, 2);

        let mut mutable =
            MutableArrayData::new(vec![a.as_ref(), b.as_ref(), &c], false, 1);
        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(2, 0, c.len());

        let finished = mutable.freeze();

        let expected_int_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            // append first array
            Some(12),
            Some(13),
            Some(14),
            Some(15),
            // append second array
            Some(14),
            Some(15),
        ]);
        let list_value_offsets =
            Buffer::from_slice_ref(&[0i32, 3, 5, 11, 13, 13, 15, 15, 17]);
        let expected_list_data = ArrayData::new(
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            8,
            None,
            None,
            0,
            vec![list_value_offsets],
            vec![expected_int_array.data()],
        );
        assert_eq!(finished, expected_list_data);

        Ok(())
    }

    #[test]
    fn test_list_nulls_append() -> Result<()> {
        let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::new(32));
        builder.values().append_slice(&[1, 2, 3])?;
        builder.append(true)?;
        builder.values().append_slice(&[4, 5])?;
        builder.append(true)?;
        builder.append(false)?;
        builder.values().append_slice(&[6, 7, 8])?;
        builder.values().append_null()?;
        builder.values().append_null()?;
        builder.values().append_slice(&[9, 10, 11])?;
        builder.append(true)?;
        let a = builder.finish();
        let a = a.data();

        let mut builder = ListBuilder::<Int64Builder>::new(Int64Builder::new(32));
        builder.values().append_slice(&[12, 13])?;
        builder.append(true)?;
        builder.append(false)?;
        builder.append(true)?;
        builder.values().append_null()?;
        builder.values().append_null()?;
        builder.values().append_slice(&[14, 15])?;
        builder.append(true)?;
        let b = builder.finish();
        let b = b.data();
        let c = b.slice(1, 2);
        let d = b.slice(2, 2);

        let mut mutable =
            MutableArrayData::new(vec![a.as_ref(), b.as_ref(), &c, &d], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(2, 0, c.len());
        mutable.extend(3, 0, d.len());
        let result = mutable.freeze();

        let expected_int_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            None,
            Some(9),
            Some(10),
            Some(11),
            // second array
            Some(12),
            Some(13),
            None,
            None,
            Some(14),
            Some(15),
            // slice(1, 2) results in no values added
            None,
            None,
            Some(14),
            Some(15),
        ]);
        let list_value_offsets =
            Buffer::from_slice_ref(&[0, 3, 5, 5, 13, 15, 15, 15, 19, 19, 19, 19, 23]);
        let expected_list_data = ArrayData::new(
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            12,
            None,
            Some(Buffer::from(&[0b11011011, 0b1110])),
            0,
            vec![list_value_offsets],
            vec![expected_int_array.data()],
        );
        assert_eq!(result, expected_list_data);

        Ok(())
    }

    #[test]
    fn test_list_of_strings_append() -> Result<()> {
        // [["alpha", "beta", None]]
        let mut builder = ListBuilder::new(StringBuilder::new(32));
        builder.values().append_value("Hello")?;
        builder.values().append_value("Arrow")?;
        builder.values().append_null()?;
        builder.append(true)?;
        let a = builder.finish().data();

        // [["alpha", "beta"], [None], ["gamma", "delta", None]]
        let mut builder = ListBuilder::new(StringBuilder::new(32));
        builder.values().append_value("alpha")?;
        builder.values().append_value("beta")?;
        builder.append(true)?;
        builder.values().append_null()?;
        builder.append(true)?;
        builder.values().append_value("gamma")?;
        builder.values().append_value("delta")?;
        builder.values().append_null()?;
        builder.append(true)?;
        let b = builder.finish().data();

        let mut mutable = MutableArrayData::new(vec![a.as_ref(), b.as_ref()], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(1, 1, 3);
        mutable.extend(1, 0, 0);
        let result = mutable.freeze();

        let expected_string_array = StringArray::from(vec![
            // extend a[0..a.len()]
            // a[0]
            Some("Hello"),
            Some("Arrow"),
            None,
            // extend b[0..b.len()]
            // b[0]
            Some("alpha"),
            Some("beta"),
            // b[1]
            None,
            // b[2]
            Some("gamma"),
            Some("delta"),
            None,
            // extend b[1..3]
            // b[1]
            None,
            // b[2]
            Some("gamma"),
            Some("delta"),
            None,
            // extend b[0..0]
        ]);
        let list_value_offsets = Buffer::from_slice_ref(&[0, 3, 5, 6, 9, 10, 13]);
        let expected_list_data = ArrayData::new(
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            6,
            None,
            None,
            0,
            vec![list_value_offsets],
            vec![expected_string_array.data()],
        );
        assert_eq!(result, expected_list_data);
        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_append() -> Result<()> {
        let a = vec![Some(vec![1, 2]), Some(vec![3, 4]), Some(vec![5, 6])];
        let a = FixedSizeBinaryArray::from(a).data();

        let b = vec![
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
            None,
        ];
        let b = FixedSizeBinaryArray::from(b).data();

        let mut mutable = MutableArrayData::new(vec![a.as_ref(), b.as_ref()], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(1, 1, 4);
        mutable.extend(1, 2, 3);
        mutable.extend(1, 5, 5);
        let result = mutable.freeze();

        let expected = vec![
            // a
            Some(vec![1, 2]),
            Some(vec![3, 4]),
            Some(vec![5, 6]),
            // b
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
            None,
            // b[1..4]
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
            // b[2..3]
            None,
            // b[4..4]
        ];
        let expected = FixedSizeBinaryArray::from(expected).data();
        assert_eq!(&result, expected.as_ref());
        Ok(())
    }

    /*
    // this is an old test used on a meanwhile removed dead code
    // that is still useful when `MutableArrayData` supports fixed-size lists.
    #[test]
    fn test_fixed_size_list_append() -> Result<()> {
        let int_builder = UInt16Builder::new(64);
        let mut builder = FixedSizeListBuilder::<UInt16Builder>::new(int_builder, 2);
        builder.values().append_slice(&[1, 2])?;
        builder.append(true)?;
        builder.values().append_slice(&[3, 4])?;
        builder.append(false)?;
        builder.values().append_slice(&[5, 6])?;
        builder.append(true)?;

        let a_builder = UInt16Builder::new(64);
        let mut a_builder = FixedSizeListBuilder::<UInt16Builder>::new(a_builder, 2);
        a_builder.values().append_slice(&[7, 8])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[9, 10])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[11, 12])?;
        a_builder.append(false)?;
        a_builder.values().append_slice(&[13, 14])?;
        a_builder.append(true)?;
        a_builder.values().append_null()?;
        a_builder.values().append_null()?;
        a_builder.append(true)?;
        let a = a_builder.finish();

        // append array
        builder.append_data(&[
            a.data(),
            a.slice(1, 3).data(),
            a.slice(2, 1).data(),
            a.slice(5, 0).data(),
        ])?;
        let finished = builder.finish();

        let expected_int_array = UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            // append first array
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            None,
            None,
            // append slice(1, 3)
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            // append slice(2, 1)
            Some(11),
            Some(12),
        ]);
        let expected_list_data = ArrayData::new(
            DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt16, true)),
                2,
            ),
            12,
            None,
            None,
            0,
            vec![],
            vec![expected_int_array.data()],
        );
        let expected_list =
            FixedSizeListArray::from(Arc::new(expected_list_data) as ArrayDataRef);
        assert_eq!(&expected_list.values(), &finished.values());
        assert_eq!(expected_list.len(), finished.len());

        Ok(())
    }
    */
}
