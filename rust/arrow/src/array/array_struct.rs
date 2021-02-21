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

use std::convert::{From, TryFrom};
use std::fmt;
use std::iter::IntoIterator;
use std::mem;
use std::{any::Any, sync::Arc};

use super::{make_array, Array, ArrayData, ArrayDataRef, ArrayRef};
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::{
    buffer::{buffer_bin_or, Buffer},
    datatypes::Field,
};

/// A nested array type where each child (called *field*) is represented by a separate
/// array.
pub struct StructArray {
    data: ArrayDataRef,
    pub(crate) boxed_fields: Vec<ArrayRef>,
}

impl StructArray {
    /// Returns the field at `pos`.
    pub fn column(&self, pos: usize) -> &ArrayRef {
        &self.boxed_fields[pos]
    }

    /// Return the number of fields in this struct array
    pub fn num_columns(&self) -> usize {
        self.boxed_fields.len()
    }

    /// Returns the fields of the struct array
    pub fn columns(&self) -> Vec<&ArrayRef> {
        self.boxed_fields.iter().collect()
    }

    /// Returns child array refs of the struct array
    pub fn columns_ref(&self) -> Vec<ArrayRef> {
        self.boxed_fields.clone()
    }

    /// Return field names in this struct array
    pub fn column_names(&self) -> Vec<&str> {
        match self.data.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>(),
            _ => unreachable!("Struct array's data type is not struct!"),
        }
    }

    /// Return child array whose field name equals to column_name
    ///
    /// Note: A schema can currently have duplicate field names, in which case
    /// the first field will always be selected.
    /// This issue will be addressed in [ARROW-11178](https://issues.apache.org/jira/browse/ARROW-11178)
    pub fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef> {
        self.column_names()
            .iter()
            .position(|c| c == &column_name)
            .map(|pos| self.column(pos))
    }
}

impl From<ArrayDataRef> for StructArray {
    fn from(data: ArrayDataRef) -> Self {
        let mut boxed_fields = vec![];
        for cd in data.child_data() {
            let child_data = if data.offset() != 0 || data.len() != cd.len() {
                Arc::new(cd.slice(data.offset(), data.len()))
            } else {
                cd.clone()
            };
            boxed_fields.push(make_array(child_data));
        }
        Self { data, boxed_fields }
    }
}

impl TryFrom<Vec<(&str, ArrayRef)>> for StructArray {
    type Error = ArrowError;

    /// builds a StructArray from a vector of names and arrays.
    /// This errors if the values have a different length.
    /// An entry is set to Null when all values are null.
    fn try_from(values: Vec<(&str, ArrayRef)>) -> Result<Self> {
        let values_len = values.len();

        // these will be populated
        let mut fields = Vec::with_capacity(values_len);
        let mut child_data = Vec::with_capacity(values_len);

        // len: the size of the arrays.
        let mut len: Option<usize> = None;
        // null: the null mask of the arrays.
        let mut null: Option<Buffer> = None;
        for (field_name, array) in values {
            let child_datum = array.data();
            let child_datum_len = child_datum.len();
            if let Some(len) = len {
                if len != child_datum_len {
                    return Err(ArrowError::InvalidArgumentError(
                        format!("Array of field \"{}\" has length {}, but previous elements have length {}.
                        All arrays in every entry in a struct array must have the same length.", field_name, child_datum_len, len)
                    ));
                }
            } else {
                len = Some(child_datum_len)
            }
            child_data.push(child_datum.clone());
            fields.push(Field::new(
                field_name,
                array.data_type().clone(),
                child_datum.null_buffer().is_some(),
            ));

            if let Some(child_null_buffer) = child_datum.null_buffer() {
                let child_datum_offset = child_datum.offset();

                null = Some(if let Some(null_buffer) = &null {
                    buffer_bin_or(
                        null_buffer,
                        0,
                        child_null_buffer,
                        child_datum_offset,
                        child_datum_len,
                    )
                } else {
                    child_null_buffer.bit_slice(child_datum_offset, child_datum_len)
                });
            } else if null.is_some() {
                // when one of the fields has no nulls, them there is no null in the array
                null = None;
            }
        }
        let len = len.unwrap();

        let mut builder = ArrayData::builder(DataType::Struct(fields))
            .len(len)
            .child_data(child_data);
        if let Some(null_buffer) = null {
            builder = builder.null_bit_buffer(null_buffer);
        }

        Ok(StructArray::from(builder.build()))
    }
}

impl Array for StructArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the length (i.e., number of elements) of this array
    fn len(&self) -> usize {
        self.data_ref().len()
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [StructArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [StructArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

impl From<Vec<(Field, ArrayRef)>> for StructArray {
    fn from(v: Vec<(Field, ArrayRef)>) -> Self {
        let (field_types, field_values): (Vec<_>, Vec<_>) = v.into_iter().unzip();

        // Check the length of the child arrays
        let length = field_values[0].len();
        for i in 1..field_values.len() {
            assert_eq!(
                length,
                field_values[i].len(),
                "all child arrays of a StructArray must have the same length"
            );
            assert_eq!(
                field_types[i].data_type(),
                field_values[i].data().data_type(),
                "the field data types must match the array data in a StructArray"
            )
        }

        let data = ArrayData::builder(DataType::Struct(field_types))
            .child_data(field_values.into_iter().map(|a| a.data()).collect())
            .len(length)
            .build();
        Self::from(data)
    }
}

impl fmt::Debug for StructArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StructArray\n[\n")?;
        for (child_index, name) in self.column_names().iter().enumerate() {
            let column = self.column(child_index);
            writeln!(
                f,
                "-- child {}: \"{}\" ({:?})",
                child_index,
                name,
                column.data_type()
            )?;
            fmt::Debug::fmt(column, f)?;
            writeln!(f)?;
        }
        write!(f, "]")
    }
}

impl From<(Vec<(Field, ArrayRef)>, Buffer)> for StructArray {
    fn from(pair: (Vec<(Field, ArrayRef)>, Buffer)) -> Self {
        let (field_types, field_values): (Vec<_>, Vec<_>) = pair.0.into_iter().unzip();

        // Check the length of the child arrays
        let length = field_values[0].len();
        for i in 1..field_values.len() {
            assert_eq!(
                length,
                field_values[i].len(),
                "all child arrays of a StructArray must have the same length"
            );
            assert_eq!(
                field_types[i].data_type(),
                field_values[i].data().data_type(),
                "the field data types must match the array data in a StructArray"
            )
        }

        let data = ArrayData::builder(DataType::Struct(field_types))
            .null_bit_buffer(pair.1)
            .child_data(field_values.into_iter().map(|a| a.data()).collect())
            .len(length)
            .build();
        Self::from(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::{
        array::BooleanArray, array::Float32Array, array::Float64Array, array::Int32Array,
        array::StringArray, bitmap::Bitmap,
    };
    use crate::{
        array::Int64Array,
        datatypes::{DataType, Field},
    };
    use crate::{buffer::Buffer, datatypes::ToByteSlice};

    #[test]
    fn test_struct_array_builder() {
        let boolean_data = BooleanArray::from(vec![false, false, true, true]).data();
        let int_data = Int64Array::from(vec![42, 28, 19, 31]).data();

        let fields = vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
        ];
        let struct_array_data = ArrayData::builder(DataType::Struct(fields))
            .len(4)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .build();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());
    }

    #[test]
    fn test_struct_array_from() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Field::new("b", DataType::Boolean, false),
                boolean.clone() as ArrayRef,
            ),
            (
                Field::new("c", DataType::Int32, false),
                int.clone() as ArrayRef,
            ),
        ]);
        assert_eq!(struct_array.column(0).as_ref(), boolean.as_ref());
        assert_eq!(struct_array.column(1).as_ref(), int.as_ref());
        assert_eq!(4, struct_array.len());
        assert_eq!(0, struct_array.null_count());
        assert_eq!(0, struct_array.offset());
    }

    /// validates that the in-memory representation follows [the spec](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)
    #[test]
    fn test_struct_array_from_vec() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
        ]));
        let ints: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();

        let struct_data = arr.data();
        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(
            // 00001011
            &Some(Bitmap::from(Buffer::from(&[11_u8]))),
            struct_data.null_bitmap()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Buffer::from(&[9_u8]))
            .add_buffer(Buffer::from(&[0, 3, 3, 3, 7].to_byte_slice()))
            .add_buffer(Buffer::from(b"joemark"))
            .build();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Buffer::from(&[11_u8]))
            .add_buffer(Buffer::from(&[1, 2, 0, 4].to_byte_slice()))
            .build();

        assert_eq!(expected_string_data, arr.column(0).data());

        // TODO: implement equality for ArrayData
        assert_eq!(expected_int_data.len(), arr.column(1).data().len());
        assert_eq!(
            expected_int_data.null_count(),
            arr.column(1).data().null_count()
        );
        assert_eq!(
            expected_int_data.null_bitmap(),
            arr.column(1).data().null_bitmap()
        );
        let expected_value_buf = expected_int_data.buffers()[0].clone();
        let actual_value_buf = arr.column(1).data().buffers()[0].clone();
        for i in 0..expected_int_data.len() {
            if !expected_int_data.is_null(i) {
                assert_eq!(
                    expected_value_buf.as_slice()[i * 4..(i + 1) * 4],
                    actual_value_buf.as_slice()[i * 4..(i + 1) * 4]
                );
            }
        }
    }

    #[test]
    fn test_struct_array_from_vec_error() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            // 3 elements, not 4
        ]));
        let ints: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())]);

        match arr {
            Err(ArrowError::InvalidArgumentError(e)) => {
                assert!(e.starts_with("Array of field \"f2\" has length 4, but previous elements have length 3."));
            }
            _ => panic!("This test got an unexpected error type"),
        };
    }

    #[test]
    #[should_panic(
        expected = "the field data types must match the array data in a StructArray"
    )]
    fn test_struct_array_from_mismatched_types() {
        StructArray::from(vec![
            (
                Field::new("b", DataType::Int16, false),
                Arc::new(BooleanArray::from(vec![false, false, true, true]))
                    as Arc<Array>,
            ),
            (
                Field::new("c", DataType::Utf8, false),
                Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
            ),
        ]);
    }

    #[test]
    fn test_struct_array_slice() {
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(5)
            .add_buffer(Buffer::from([0b00010000]))
            .null_bit_buffer(Buffer::from([0b00010001]))
            .build();
        let int_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from([0, 28, 42, 0, 0].to_byte_slice()))
            .null_bit_buffer(Buffer::from([0b00000110]))
            .build();

        let mut field_types = vec![];
        field_types.push(Field::new("a", DataType::Boolean, false));
        field_types.push(Field::new("b", DataType::Int32, false));
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
            .len(5)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .null_bit_buffer(Buffer::from([0b00010111]))
            .build();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(5, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert!(struct_array.is_valid(0));
        assert!(struct_array.is_valid(1));
        assert!(struct_array.is_valid(2));
        assert!(struct_array.is_null(3));
        assert!(struct_array.is_valid(4));
        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());

        let c0 = struct_array.column(0);
        let c0 = c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(5, c0.len());
        assert_eq!(3, c0.null_count());
        assert!(c0.is_valid(0));
        assert_eq!(false, c0.value(0));
        assert!(c0.is_null(1));
        assert!(c0.is_null(2));
        assert!(c0.is_null(3));
        assert!(c0.is_valid(4));
        assert_eq!(true, c0.value(4));

        let c1 = struct_array.column(1);
        let c1 = c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c1.len());
        assert_eq!(3, c1.null_count());
        assert!(c1.is_null(0));
        assert!(c1.is_valid(1));
        assert_eq!(28, c1.value(1));
        assert!(c1.is_valid(2));
        assert_eq!(42, c1.value(2));
        assert!(c1.is_null(3));
        assert!(c1.is_null(4));

        let sliced_array = struct_array.slice(2, 3);
        let sliced_array = sliced_array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(3, sliced_array.len());
        assert_eq!(2, sliced_array.offset());
        assert_eq!(1, sliced_array.null_count());
        assert!(sliced_array.is_valid(0));
        assert!(sliced_array.is_null(1));
        assert!(sliced_array.is_valid(2));

        let sliced_c0 = sliced_array.column(0);
        let sliced_c0 = sliced_c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(3, sliced_c0.len());
        assert_eq!(2, sliced_c0.offset());
        assert!(sliced_c0.is_null(0));
        assert!(sliced_c0.is_null(1));
        assert!(sliced_c0.is_valid(2));
        assert_eq!(true, sliced_c0.value(2));

        let sliced_c1 = sliced_array.column(1);
        let sliced_c1 = sliced_c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, sliced_c1.len());
        assert_eq!(2, sliced_c1.offset());
        assert!(sliced_c1.is_valid(0));
        assert_eq!(42, sliced_c1.value(0));
        assert!(sliced_c1.is_null(1));
        assert!(sliced_c1.is_null(2));
    }

    #[test]
    #[should_panic(
        expected = "all child arrays of a StructArray must have the same length"
    )]
    fn test_invalid_struct_child_array_lengths() {
        StructArray::from(vec![
            (
                Field::new("b", DataType::Float32, false),
                Arc::new(Float32Array::from(vec![1.1])) as Arc<Array>,
            ),
            (
                Field::new("c", DataType::Float64, false),
                Arc::new(Float64Array::from(vec![2.2, 3.3])),
            ),
        ]);
    }
}
