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

/// Contains the implementation of the `UnionArray` array and the `UnionBuilder`.
///
/// Each slot in a `UnionArray` can have a value chosen from a number of types.  Each of the
/// possible types are named like the fields of a `StructArray`.  A `UnionArray` can have two
/// possible memory layouts, "dense" or "sparse".  For more information on please
/// see the [specification](https://arrow.apache.org/docs/format/Columnar.html#union-layout).
// TODO: Examples
use crate::array::{
    builder::{builder_to_mutable_buffer, mutable_buffer_to_builder, BufferBuilderTrait},
    make_array, Array, ArrayData, ArrayDataBuilder, ArrayDataRef, ArrayRef,
    BooleanBufferBuilder, BufferBuilder, Int32BufferBuilder, Int8BufferBuilder,
};
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use core::fmt;
use std::any::Any;
use std::collections::HashMap;

/// An Array that can represent slots of varying types
pub struct UnionArray {
    data: ArrayDataRef,
    boxed_fields: Vec<ArrayRef>,
}

impl UnionArray {
    // TODO: Docs
    pub unsafe fn new(
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
    ) -> Self {
        let (field_types, field_values): (Vec<_>, Vec<_>) =
            child_arrays.into_iter().unzip();
        let len = type_ids.len();
        let builder = ArrayData::builder(DataType::Union(field_types))
            .add_buffer(type_ids)
            .child_data(field_values.into_iter().map(|a| a.data()).collect())
            .len(len);
        let data = match value_offsets {
            Some(b) => builder.add_buffer(b).build(),
            None => builder.build(),
        };
        Self::from(data)
    }
    /// Attempts to create a new `UnionArray`
    pub fn try_new(
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
    ) -> Result<Self> {
        if let Some(b) = &value_offsets {
            if (type_ids.len() * 4) != b.len() {
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
        if invalid_type_ids.len() > 0 {
            return Err(ArrowError::InvalidArgumentError(
                format!("Type Ids must be positive and within the length of the Array, found:\n{:?}", invalid_type_ids)));
        }

        // Check the value offsets if provided
        if let Some(offset_buffer) = &value_offsets {
            let max_len = type_ids.len() as i32;
            let offsets_slice: &[i32] = unsafe { offset_buffer.typed_data() };
            let invalid_offsets = offsets_slice
                .iter()
                .filter(|i| *i < &0 || *i > &max_len)
                .collect::<Vec<&i32>>();
            if invalid_offsets.len() > 0 {
                return Err(ArrowError::InvalidArgumentError(
                    format!("Offsets must be positive and within the length of the Array, found:\n{:?}", invalid_offsets)));
            }
        }

        Ok(unsafe { Self::new(type_ids, value_offsets, child_arrays) })
    }

    /// Accesses the child array for `type_id`
    pub fn child(&self, type_id: i8) -> Result<ArrayRef> {
        if type_id < 0 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "'type_id' cannot be negative, {} provided.",
                type_id
            )));
        }
        if type_id as usize > self.boxed_fields.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "'type_id' cannot be larger than the number of types ({}), {} provided.",
                self.boxed_fields.len(),
                type_id
            )));
        }
        Ok(unsafe { self.child_unchecked(type_id) })
    }

    /// Unsafe version of `child` that does not validate the `type_id` provided
    pub unsafe fn child_unchecked(&self, type_id: i8) -> ArrayRef {
        self.boxed_fields[type_id as usize].clone()
    }

    /// Returns the `type_id` for the array slot at index `i`
    pub fn type_id(&self, i: usize) -> Result<i8> {
        if i > self.len() {
            Err(ArrowError::InvalidArgumentError(format!(
                "The index provided ({}) exceeds the length of the Array ({}).",
                i,
                self.len()
            )))
        } else {
            Ok(unsafe { self.type_id_unchecked(i) })
        }
    }

    /// Unsafe version of `type_id` that does not validate `i`
    pub unsafe fn type_id_unchecked(&self, i: usize) -> i8 {
        self.data().buffers()[0].data()[i] as i8
    }

    /// Returns the offset into the underlying values array for the array slot at index `i`
    pub fn value_offset(&self, i: usize) -> Result<i32> {
        if i > self.len() {
            Err(ArrowError::InvalidArgumentError(format!(
                "The index provided ({}) exceeds the length of the Array ({}).",
                i,
                self.len()
            )))
        } else {
            Ok(unsafe { self.value_offset_unchecked(i) })
        }
    }

    /// Unsafe version of `value_offset` that does not validate `i`
    pub unsafe fn value_offset_unchecked(&self, i: usize) -> i32 {
        // TODO: panics...
        if self.data().buffers().len() == 2 {
            self.data().buffers()[1].data()[i * 4] as i32
        } else {
            i as i32
        }
    }

    /// Returns array slot at index `i`
    pub fn value(&self, i: usize) -> Result<ArrayRef> {
        let type_id = self.type_id(i)?;
        let value_offset = unsafe { self.value_offset_unchecked(i) } as usize;
        let child_data = unsafe { self.child_unchecked(type_id) };
        Ok(child_data.slice(value_offset, 1))
    }

    /// Unsafe version of `value` that does not validate `i`
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let type_id = self.type_id_unchecked(i);
        let value_offset = self.value_offset_unchecked(i) as usize;
        let child_data = self.child_unchecked(type_id);
        child_data.slice(value_offset, 1)
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
}

impl fmt::Debug for UnionArray {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

/// `FieldData` is a helper struct to track the state of the fields in the `UnionBuilder`.
struct FieldData {
    // The type id for this field
    type_id: i8,
    // The Arrow data type represented in the `values_buffer`
    data_type: DataType,
    // A buffer containing the values for this field
    values_buffer: Option<MutableBuffer>,
    //  The number of array slots represented by the buffer
    slots: usize,
    null_count: usize,
    bitmap_builder: Option<BooleanBufferBuilder>,
}

impl FieldData {
    fn new(
        type_id: i8,
        data_type: DataType,
        bitmap_builder: Option<BooleanBufferBuilder>,
    ) -> Self {
        Self {
            type_id,
            data_type,
            values_buffer: Some(MutableBuffer::new(1)),
            slots: 0,
            null_count: 0,
            bitmap_builder,
        }
    }

    fn append_to_values_buffer<T: ArrowPrimitiveType>(&mut self, v: T::Native) {
        let values_buffer = self
            .values_buffer
            .take()
            .expect("Values buffer was never created");
        let mut builder: BufferBuilder<T> =
            mutable_buffer_to_builder(values_buffer, self.slots);
        builder.append(v).unwrap();
        let mutable_buffer = builder_to_mutable_buffer(builder);
        self.values_buffer = Some(mutable_buffer);

        self.slots += 1;
        if let Some(b) = &mut self.bitmap_builder {
            b.append(true).unwrap()
        }
    }

    fn append_null<T: ArrowPrimitiveType>(&mut self) {
        if let Some(b) = &mut self.bitmap_builder {
            let values_buffer = self
                .values_buffer
                .take()
                .expect("Values buffer was never created");
            let mut builder: BufferBuilder<T> =
                mutable_buffer_to_builder(values_buffer, self.slots);
            builder.advance(1).unwrap();
            let mutable_buffer = builder_to_mutable_buffer(builder);
            self.values_buffer = Some(mutable_buffer);
            self.slots += 1;
            b.append(false).unwrap();
        }
    }
}

pub struct UnionBuilder {
    current_type_id: i8,
    len: usize,
    fields: HashMap<String, FieldData>,
    type_id_builder: Int8BufferBuilder,
    value_offset_builder: Option<Int32BufferBuilder>,
}

impl UnionBuilder {
    pub fn new_dense() -> Self {
        Self {
            current_type_id: 0,
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(0),
            value_offset_builder: Some(Int32BufferBuilder::new(0)),
        }
    }

    pub fn new_sparse() -> Self {
        Self {
            current_type_id: 0,
            len: 0,
            fields: HashMap::default(),
            type_id_builder: Int8BufferBuilder::new(0),
            value_offset_builder: None,
        }
    }

    // TODO: Defaults
    pub fn append<T: ArrowPrimitiveType>(&mut self, type_name: &str, v: T::Native) {
        let type_name = type_name.to_string();

        // Update the `FieldData` with `v`
        let mut field_data = match self.fields.remove(&type_name) {
            Some(data) => data,
            None => {
                let field_data = match self.value_offset_builder {
                    Some(_) => {
                        FieldData::new(self.current_type_id, T::get_data_type(), None)
                    }
                    None => {
                        let mut fd = FieldData::new(
                            self.current_type_id,
                            T::get_data_type(),
                            Some(BooleanBufferBuilder::new(1)),
                        );
                        for _ in 0..self.len {
                            fd.append_null::<T>();
                        }
                        fd
                    }
                };
                self.current_type_id += 1;
                field_data
            }
        };
        self.type_id_builder.append(field_data.type_id).unwrap();

        match &mut self.value_offset_builder {
            // Dense Union
            Some(offset_builder) => {
                offset_builder.append(field_data.slots as i32).unwrap();
            }
            // Sparse Union
            None => {
                for (name, fd) in self.fields.iter_mut() {
                    if name != &type_name {
                        match fd.data_type {
                            DataType::Boolean => fd.append_null::<BooleanType>(),
                            DataType::Int8 => fd.append_null::<Int8Type>(),
                            DataType::Int16 => fd.append_null::<Int16Type>(),
                            DataType::Int32 => fd.append_null::<Int32Type>(),
                            DataType::Int64 => fd.append_null::<Int64Type>(),
                            DataType::UInt8 => fd.append_null::<UInt8Type>(),
                            DataType::UInt16 => fd.append_null::<UInt16Type>(),
                            DataType::UInt32 => fd.append_null::<UInt32Type>(),
                            DataType::UInt64 => fd.append_null::<UInt64Type>(),
                            DataType::Float32 => fd.append_null::<Float32Type>(),
                            DataType::Float64 => fd.append_null::<Float64Type>(),
                            _ => unimplemented!(),
                        };
                        //                        fd.append_null::<T>();
                    }
                }
            }
        }
        field_data.append_to_values_buffer::<T>(v);
        self.fields.insert(type_name, field_data);
        self.len += 1;
    }

    pub fn build(mut self) -> UnionArray {
        let type_id_buffer = self.type_id_builder.finish();
        let value_offsets_buffer = self.value_offset_builder.map(|mut b| b.finish());
        let mut children = Vec::new();
        for (
            name,
            FieldData {
                type_id,
                data_type,
                values_buffer,
                slots,
                bitmap_builder,
                null_count,
            },
        ) in self.fields.into_iter()
        {
            let buffer = values_buffer.expect("UPDATE LATER").freeze();
            let arr_data_builder = ArrayDataBuilder::new(data_type.clone())
                .add_buffer(buffer)
                .null_count(null_count)
                .len(slots);
            //                .build();
            let arr_data_ref = match bitmap_builder {
                Some(mut bb) => {
                    // TODO: YOU'RE HERE
                    arr_data_builder.null_bit_buffer(bb.finish()).build()
                }
                None => arr_data_builder.build(),
            };
            let array_ref = make_array(arr_data_ref);
            children.push((type_id, (Field::new(&name, data_type, false), array_ref)))
        }

        children.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let children: Vec<_> = children.into_iter().map(|(_, b)| b).collect();

        UnionArray::try_new(type_id_buffer, value_offsets_buffer, children).unwrap()
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
    fn test_union_i32_dense() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1);
        builder.append::<Int32Type>("b", 2);
        builder.append::<Int32Type>("c", 3);
        builder.append::<Int32Type>("a", 4);
        builder.append::<Int32Type>("c", 5);
        builder.append::<Int32Type>("a", 6);
        builder.append::<Int32Type>("b", 7);
        let array = builder.build();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_value_offsets = vec![0_i32, 0, 0, 1, 1, 2, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(
            array.data().buffers()[0],
            Buffer::from(&expected_type_ids.clone().to_byte_slice())
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i).unwrap());
        }

        // Check offsets
        assert_eq!(
            array.data().buffers()[1],
            Buffer::from(expected_value_offsets.clone().to_byte_slice())
        );
        for (i, id) in expected_value_offsets.iter().enumerate() {
            assert_eq!(&array.value_offset(i).unwrap(), id);
        }

        // Check data
        assert_eq!(
            array.data().child_data()[0].buffers()[0],
            Buffer::from([1_i32, 4, 6].to_byte_slice())
        );
        assert_eq!(
            array.data().child_data()[1].buffers()[0],
            Buffer::from([2_i32, 7].to_byte_slice())
        );
        assert_eq!(
            array.data().child_data()[2].buffers()[0],
            Buffer::from([3_i32, 5].to_byte_slice()),
        );

        assert_eq!(expected_array_values.len(), array.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            let slot = array.value(i).unwrap();
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    fn test_dense_union_mixed() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1);
        builder.append::<BooleanType>("b", false);
        builder.append::<Int64Type>("c", 3);
        builder.append::<Int32Type>("a", 4);
        builder.append::<Int64Type>("c", 5);
        builder.append::<Int32Type>("a", 6);
        builder.append::<BooleanType>("b", true);
        let union = builder.build();

        assert_eq!(7, union.len());
        for i in 0..union.len() {
            let slot = union.value(i).unwrap();
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<BooleanArray>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(false, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(3_i64, value);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(5_i64, value);
                }
                5 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                6 => {
                    let slot = slot.as_any().downcast_ref::<BooleanArray>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(true, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_union_i32_sparse() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1);
        builder.append::<Int32Type>("b", 2);
        builder.append::<Int32Type>("c", 3);
        builder.append::<Int32Type>("a", 4);
        builder.append::<Int32Type>("c", 5);
        builder.append::<Int32Type>("a", 6);
        builder.append::<Int32Type>("b", 7);
        let array = builder.build();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(
            Buffer::from(&expected_type_ids.clone().to_byte_slice()),
            array.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i).unwrap());
        }

        // Check offsets, sparse union should only have a single buffer
        assert_eq!(array.data().buffers().len(), 1);

        // Check data
        assert_eq!(
            array.data().child_data()[0].buffers()[0],
            Buffer::from([1_i32, 0, 0, 4, 0, 6, 0].to_byte_slice()),
        );
        assert_eq!(
            Buffer::from([0_i32, 2_i32, 0, 0, 0, 0, 7].to_byte_slice()),
            array.data().child_data()[1].buffers()[0]
        );
        assert_eq!(
            Buffer::from([0_i32, 0, 3_i32, 0, 5, 0, 0].to_byte_slice()),
            array.data().child_data()[2].buffers()[0]
        );

        assert_eq!(expected_array_values.len(), array.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            let slot = array.value(i).unwrap();
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    fn test_union_i32_sparse_mixed() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1);
        builder.append::<BooleanType>("b", true);
        builder.append::<Float64Type>("c", 3.0);
        builder.append::<Int32Type>("a", 4);
        builder.append::<Float64Type>("c", 5.0);
        builder.append::<Int32Type>("a", 6);
        builder.append::<BooleanType>("b", false);
        let union = builder.build();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];

        // Check type ids
        assert_eq!(
            Buffer::from(&expected_type_ids.clone().to_byte_slice()),
            union.data().buffers()[0]
        );
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i).unwrap());
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert_eq!(union.data().buffers().len(), 1);

        for i in 0..union.len() {
            let slot = union.value(i).unwrap();
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<BooleanArray>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(true, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(5_f64, value);
                }
                5 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                6 => {
                    let slot = slot.as_any().downcast_ref::<BooleanArray>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(false, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_union_mixed_with_str() {
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
        let array =
            UnionArray::try_new(type_id_buffer, Some(value_offsets_buffer), children)
                .unwrap();

        // Check type ids
        assert_eq!(
            Buffer::from(&type_ids.to_byte_slice()),
            array.data().buffers()[0]
        );
        for (i, id) in type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i).unwrap());
        }

        // Check offsets
        assert_eq!(
            Buffer::from(value_offsets.to_byte_slice()),
            array.data().buffers()[1]
        );
        for (i, id) in value_offsets.iter().enumerate() {
            assert_eq!(id, &array.value_offset(i).unwrap());
        }

        // Check values
        assert_eq!(6, array.len());

        let slot = array.value(0).unwrap();
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(5, value);

        let slot = array.value(1).unwrap();
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("foo", value);

        let slot = array.value(2).unwrap();
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("bar", value);

        let slot = array.value(3).unwrap();
        let value = slot
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(10.0, value);

        let slot = array.value(4).unwrap();
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("baz", value);

        let slot = array.value(5).unwrap();
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(6, value);
    }
}
