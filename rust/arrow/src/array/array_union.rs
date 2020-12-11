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

//! Contains the `UnionArray` type.
//!
//! Each slot in a `UnionArray` can have a value chosen from a number of types.  Each of the
//! possible types are named like the fields of a [`StructArray`](crate::array::StructArray).
//! A `UnionArray` can have two possible memory layouts, "dense" or "sparse".  For more information
//! on please see the [specification](https://arrow.apache.org/docs/format/Columnar.html#union-layout).
//!
//! Builders are provided for `UnionArray`'s involving primitive types.  `UnionArray`'s of nested
//! types are also supported but not via `UnionBuilder`, see the tests for examples.
//!
//! # Example: Dense Memory Layout
//!
//! ```
//! use arrow::array::UnionBuilder;
//! use arrow::datatypes::{Float64Type, Int32Type};
//!
//! # fn main() -> arrow::error::Result<()> {
//! let mut builder = UnionBuilder::new_dense(3);
//! builder.append::<Int32Type>("a", 1).unwrap();
//! builder.append::<Float64Type>("b", 3.0).unwrap();
//! builder.append::<Int32Type>("a", 4).unwrap();
//! let union = builder.build().unwrap();
//!
//! assert_eq!(union.type_id(0), 0_i8);
//! assert_eq!(union.type_id(1), 1_i8);
//! assert_eq!(union.type_id(2), 0_i8);
//!
//! assert_eq!(union.value_offset(0), 0_i32);
//! assert_eq!(union.value_offset(1), 0_i32);
//! assert_eq!(union.value_offset(2), 1_i32);
//!
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Sparse Memory Layout
//! ```
//! use arrow::array::UnionBuilder;
//! use arrow::datatypes::{Float64Type, Int32Type};
//!
//! # fn main() -> arrow::error::Result<()> {
//! let mut builder = UnionBuilder::new_sparse(3);
//! builder.append::<Int32Type>("a", 1).unwrap();
//! builder.append::<Float64Type>("b", 3.0).unwrap();
//! builder.append::<Int32Type>("a", 4).unwrap();
//! let union = builder.build().unwrap();
//!
//! assert_eq!(union.type_id(0), 0_i8);
//! assert_eq!(union.type_id(1), 1_i8);
//! assert_eq!(union.type_id(2), 0_i8);
//!
//! assert_eq!(union.value_offset(0), 0_i32);
//! assert_eq!(union.value_offset(1), 1_i32);
//! assert_eq!(union.value_offset(2), 2_i32);
//!
//! # Ok(())
//! # }
//! ```
use crate::array::{make_array, Array, ArrayData, ArrayDataRef, ArrayRef};
use crate::buffer::Buffer;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use core::fmt;
use std::any::Any;
use std::mem;
use std::mem::size_of;

/// An Array that can represent slots of varying types.
pub struct UnionArray {
    data: ArrayDataRef,
    boxed_fields: Vec<ArrayRef>,
}

impl UnionArray {
    /// Creates a new `UnionArray`.
    ///
    /// Accepts type ids, child arrays and optionally offsets (for dense unions) to create
    /// a new `UnionArray`.  This method makes no attempt to validate the data provided by the
    /// caller and assumes that each of the components are correct and consistent with each other.
    /// See `try_new` for an alternative that validates the data provided.
    ///
    /// # Data Consistency
    ///
    /// The `type_ids` `Buffer` should contain `i8` values.  These values should be greater than
    /// zero and must be less than the number of children provided in `child_arrays`.  These values
    /// are used to index into the `child_arrays`.
    ///
    /// The `value_offsets` `Buffer` is only provided in the case of a dense union, sparse unions
    /// should use `None`.  If provided the `value_offsets` `Buffer` should contain `i32` values.
    /// These values should be greater than zero and must be less than the length of the overall
    /// array.
    ///
    /// In both cases above we use signed integer types to maintain compatibility with other
    /// Arrow implementations.
    ///
    /// In both of the cases above we are accepting `Buffer`'s which are assumed to be representing
    /// `i8` and `i32` values respectively.  `Buffer` objects are untyped and no attempt is made
    /// to ensure that the data provided is valid.
    pub fn new(
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
        bitmap_data: Option<(Buffer, usize)>,
    ) -> Self {
        let (field_types, field_values): (Vec<_>, Vec<_>) =
            child_arrays.into_iter().unzip();
        let len = type_ids.len();
        let mut builder = ArrayData::builder(DataType::Union(field_types))
            .add_buffer(type_ids)
            .child_data(field_values.into_iter().map(|a| a.data()).collect())
            .len(len);
        if let Some((bitmap, null_count)) = bitmap_data {
            builder = builder.null_bit_buffer(bitmap).null_count(null_count);
        }
        let data = match value_offsets {
            Some(b) => builder.add_buffer(b).build(),
            None => builder.build(),
        };
        Self::from(data)
    }
    /// Attempts to create a new `UnionArray` and validates the inputs provided.
    pub fn try_new(
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
        bitmap: Option<Buffer>,
    ) -> Result<Self> {
        let bitmap_data = bitmap.map(|b| {
            let null_count = type_ids.len() - b.count_set_bits();
            (b, null_count)
        });

        if let Some(b) = &value_offsets {
            let nulls = match bitmap_data {
                Some((_, n)) => n,
                None => 0,
            };
            if ((type_ids.len() - nulls) * 4) != b.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "Type Ids and Offsets represent a different number of array slots."
                        .to_string(),
                ));
            }
        }

        // Check the type_ids
        let type_id_slice: &[i8] = unsafe { type_ids.typed_data() };
        let invalid_type_ids = type_id_slice
            .iter()
            .filter(|i| *i < &0)
            .collect::<Vec<&i8>>();
        if !invalid_type_ids.is_empty() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Type Ids must be positive and cannot be greater than the number of \
                child arrays, found:\n{:?}",
                invalid_type_ids
            )));
        }

        // Check the value offsets if provided
        if let Some(offset_buffer) = &value_offsets {
            let max_len = type_ids.len() as i32;
            let offsets_slice: &[i32] = unsafe { offset_buffer.typed_data() };
            let invalid_offsets = offsets_slice
                .iter()
                .filter(|i| *i < &0 || *i > &max_len)
                .collect::<Vec<&i32>>();
            if !invalid_offsets.is_empty() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Offsets must be positive and within the length of the Array, \
                    found:\n{:?}",
                    invalid_offsets
                )));
            }
        }

        Ok(Self::new(
            type_ids,
            value_offsets,
            child_arrays,
            bitmap_data,
        ))
    }

    /// Accesses the child array for `type_id`.
    ///
    /// # Panics
    ///
    /// Panics if the `type_id` provided is less than zero or greater than the number of types
    /// in the `Union`.
    pub fn child(&self, type_id: i8) -> ArrayRef {
        assert!(0 <= type_id);
        assert!((type_id as usize) < self.boxed_fields.len());
        self.boxed_fields[type_id as usize].clone()
    }

    /// Returns the `type_id` for the array slot at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than the length of the array.
    pub fn type_id(&self, index: usize) -> i8 {
        assert!(index - self.offset() < self.len());
        self.data().buffers()[0].data()[index] as i8
    }

    /// Returns the offset into the underlying values array for the array slot at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than the length of the array.
    pub fn value_offset(&self, index: usize) -> i32 {
        assert!(index - self.offset() < self.len());
        if self.is_dense() {
            // In format v4 unions had their own validity bitmap and offsets are compressed by omitting null values
            // Starting with v5 unions don't have a validity bitmap and it's possible to directly index into the offsets buffer
            let valid_slots = match self.data.null_buffer() {
                Some(b) => b.count_set_bits_offset(0, index),
                None => index,
            };
            self.data().buffers()[1].data()[valid_slots * size_of::<i32>()] as i32
        } else {
            index as i32
        }
    }

    /// Returns the array's value at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than the length of the array.
    pub fn value(&self, index: usize) -> ArrayRef {
        let type_id = self.type_id(self.offset() + index);
        let value_offset = self.value_offset(self.offset() + index) as usize;
        let child_data = self.boxed_fields[type_id as usize].clone();
        child_data.slice(value_offset, 1)
    }

    /// Returns the names of the types in the union.
    pub fn type_names(&self) -> Vec<&str> {
        match self.data.data_type() {
            DataType::Union(fields) => fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>(),
            _ => unreachable!("Union array's data type is not a union!"),
        }
    }

    /// Returns whether the `UnionArray` is dense (or sparse if `false`).
    fn is_dense(&self) -> bool {
        self.data().buffers().len() == 2
    }
}

impl From<ArrayDataRef> for UnionArray {
    fn from(data: ArrayDataRef) -> Self {
        let mut boxed_fields = vec![];
        for cd in data.child_data() {
            boxed_fields.push(make_array(cd.clone()));
        }
        Self { data, boxed_fields }
    }
}

impl Array for UnionArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [UnionArray].
    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.data.get_buffer_memory_size();
        for field in &self.boxed_fields {
            size += field.get_buffer_memory_size();
        }
        size
    }

    /// Returns the total number of bytes of memory occupied physically by this [UnionArray].
    fn get_array_memory_size(&self) -> usize {
        let mut size = self.data.get_array_memory_size();
        size += mem::size_of_val(self) - mem::size_of_val(&self.boxed_fields);
        for field in &self.boxed_fields {
            size += field.get_array_memory_size();
        }
        size
    }
}

impl fmt::Debug for UnionArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let header = if self.is_dense() {
            "UnionArray(Dense)\n["
        } else {
            "UnionArray(Sparse)\n["
        };
        writeln!(f, "{}", header)?;

        writeln!(f, "-- type id buffer:")?;
        writeln!(f, "{:?}", self.data().buffers()[0])?;

        if self.is_dense() {
            writeln!(f, "-- offsets buffer:")?;
            writeln!(f, "{:?}", self.data().buffers()[1])?;
        }

        for (child_index, name) in self.type_names().iter().enumerate() {
            let column = &self.boxed_fields[child_index];
            writeln!(
                f,
                "-- child {}: \"{}\" ({:?})",
                child_index,
                *name,
                column.data_type()
            )?;
            fmt::Debug::fmt(column, f)?;
            writeln!(f)?;
        }
        writeln!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::array::*;
    use crate::buffer::Buffer;
    use crate::datatypes::{DataType, Field, ToByteSlice};

    #[test]
    fn test_dense_i32() {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_value_offsets = vec![0_i32, 0, 0, 1, 1, 2, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(
            union.data().buffers()[0],
            Buffer::from(&expected_type_ids.to_byte_slice())
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets
        assert_eq!(
            union.data().buffers()[1],
            Buffer::from(expected_value_offsets.to_byte_slice())
        );
        for (i, id) in expected_value_offsets.iter().enumerate() {
            assert_eq!(&union.value_offset(i), id);
        }

        // Check data
        assert_eq!(
            union.data().child_data()[0].buffers()[0],
            Buffer::from([1_i32, 4, 6].to_byte_slice())
        );
        assert_eq!(
            union.data().child_data()[1].buffers()[0],
            Buffer::from([2_i32, 7].to_byte_slice())
        );
        assert_eq!(
            union.data().child_data()[2].buffers()[0],
            Buffer::from([3_i32, 5].to_byte_slice()),
        );

        assert_eq!(expected_array_values.len(), union.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            assert_eq!(false, union.is_null(i));
            let slot = union.value(i);
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }

        assert_eq!(
            4 * 8 * 4 * mem::size_of::<i32>(),
            union.get_buffer_memory_size()
        );
        let internals_of_union_array = (8 + 72) + (union.boxed_fields.len() * 144); // Arc<ArrayData> & Vec<ArrayRef> combined.
        assert_eq!(
            union.get_buffer_memory_size() + internals_of_union_array,
            union.get_array_memory_size()
        );
    }

    #[test]
    fn test_dense_mixed() {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int64Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        assert_eq!(5, union.len());
        for i in 0..union.len() {
            let slot = union.value(i);
            assert_eq!(false, union.is_null(i));
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(3_i64, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(5_i64, value);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_nulls() {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 10).unwrap();
        builder.append_null().unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        assert_eq!(5, union.len());
        for i in 0..union.len() {
            let slot = union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(3_i64, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(10_i32, value);
                }
                3 => assert!(union.is_null(i)),
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_nulls_and_offset() {
        let mut builder = UnionBuilder::new_dense(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 10).unwrap();
        builder.append_null().unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        let slice = union.slice(2, 3);
        let new_union = slice.as_any().downcast_ref::<UnionArray>().unwrap();

        assert_eq!(3, new_union.len());
        for i in 0..new_union.len() {
            let slot = new_union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(10_i32, value);
                }
                1 => assert!(new_union.is_null(i)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_str() {
        let string_array = StringArray::from(vec!["foo", "bar", "baz"]);
        let int_array = Int32Array::from(vec![5, 6]);
        let float_array = Float64Array::from(vec![10.0]);

        let type_ids = [1_i8, 0, 0, 2, 0, 1];
        let value_offsets = [0_i32, 0, 1, 0, 2, 1];

        let type_id_buffer = Buffer::from(&type_ids.to_byte_slice());
        let value_offsets_buffer = Buffer::from(value_offsets.to_byte_slice());

        let mut children: Vec<(Field, Arc<Array>)> = Vec::new();
        children.push((
            Field::new("A", DataType::Utf8, false),
            Arc::new(string_array),
        ));
        children.push((Field::new("B", DataType::Int32, false), Arc::new(int_array)));
        children.push((
            Field::new("C", DataType::Float64, false),
            Arc::new(float_array),
        ));
        let array = UnionArray::try_new(
            type_id_buffer,
            Some(value_offsets_buffer),
            children,
            None,
        )
        .unwrap();

        // Check type ids
        assert_eq!(
            Buffer::from(&type_ids.to_byte_slice()),
            array.data().buffers()[0]
        );
        for (i, id) in type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        // Check offsets
        assert_eq!(
            Buffer::from(value_offsets.to_byte_slice()),
            array.data().buffers()[1]
        );
        for (i, id) in value_offsets.iter().enumerate() {
            assert_eq!(id, &array.value_offset(i));
        }

        // Check values
        assert_eq!(6, array.len());

        let slot = array.value(0);
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(5, value);

        let slot = array.value(1);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("foo", value);

        let slot = array.value(2);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("bar", value);

        let slot = array.value(3);
        let value = slot
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(10.0 - value < f64::EPSILON);

        let slot = array.value(4);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("baz", value);

        let slot = array.value(5);
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(6, value);
    }

    #[test]
    fn test_sparse_i32() {
        let mut builder = UnionBuilder::new_sparse(7);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(
            Buffer::from(&expected_type_ids.to_byte_slice()),
            union.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer
        assert_eq!(union.data().buffers().len(), 1);

        // Check data
        assert_eq!(
            union.data().child_data()[0].buffers()[0],
            Buffer::from([1_i32, 0, 0, 4, 0, 6, 0].to_byte_slice()),
        );
        assert_eq!(
            Buffer::from([0_i32, 2_i32, 0, 0, 0, 0, 7].to_byte_slice()),
            union.data().child_data()[1].buffers()[0]
        );
        assert_eq!(
            Buffer::from([0_i32, 0, 3_i32, 0, 5, 0, 0].to_byte_slice()),
            union.data().child_data()[2].buffers()[0]
        );

        assert_eq!(expected_array_values.len(), union.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            assert_eq!(false, union.is_null(i));
            let slot = union.value(i);
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    fn test_sparse_mixed() {
        let mut builder = UnionBuilder::new_sparse(5);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Float64Type>("c", 5.0).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 0, 1, 0];

        // Check type ids
        assert_eq!(
            Buffer::from(&expected_type_ids.to_byte_slice()),
            union.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert_eq!(union.data().buffers().len(), 1);

        for i in 0..union.len() {
            let slot = union.value(i);
            assert_eq!(false, union.is_null(i));
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert!(value - 3_f64 < f64::EPSILON);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert!(5_f64 - value < f64::EPSILON);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sparse_mixed_with_nulls() {
        let mut builder = UnionBuilder::new_sparse(5);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null().unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(
            Buffer::from(&expected_type_ids.to_byte_slice()),
            union.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert_eq!(union.data().buffers().len(), 1);

        for i in 0..union.len() {
            let slot = union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(union.is_null(i)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert!(value - 3_f64 < f64::EPSILON);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sparse_mixed_with_nulls_and_offset() {
        let mut builder = UnionBuilder::new_sparse(5);
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null().unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null().unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let slice = union.slice(1, 4);
        let new_union = slice.as_any().downcast_ref::<UnionArray>().unwrap();

        assert_eq!(4, new_union.len());
        for i in 0..new_union.len() {
            let slot = new_union.value(i);
            match i {
                0 => assert!(new_union.is_null(i)),
                1 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(false, new_union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert!(value - 3_f64 < f64::EPSILON);
                }
                2 => assert!(new_union.is_null(i)),
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(false, new_union.is_null(i));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }
}
