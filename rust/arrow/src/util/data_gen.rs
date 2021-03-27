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

//! Utilities to generate random arrays and batches

use std::{convert::TryFrom, sync::Arc};

use rand::{distributions::uniform::SampleUniform, Rng};

use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchOptions};
use crate::{array::*, datatypes::SchemaRef};
use crate::{
    buffer::{Buffer, MutableBuffer},
    datatypes::*,
};

use super::{bench_util::*, bit_util, test_util::seedable_rng};

/// Create a random [RecordBatch] from a schema
pub fn create_random_batch(
    schema: SchemaRef,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| create_random_array(field, size, null_density, true_density))
        .collect::<Result<Vec<ArrayRef>>>()?;

    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions {
            match_field_names: false,
        },
    )
}

/// Create a random [ArrayRef] from a [DataType] with a length,
/// null density and true density (for [BooleanArray]).
pub fn create_random_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    use DataType::*;
    Ok(match field.data_type() {
        Null => Arc::new(NullArray::new(size)) as ArrayRef,
        Boolean => Arc::new(create_boolean_array(size, null_density, true_density)),
        Int8 => Arc::new(create_primitive_array::<Int8Type>(size, null_density)),
        Int16 => Arc::new(create_primitive_array::<Int16Type>(size, null_density)),
        Int32 => Arc::new(create_primitive_array::<Int32Type>(size, null_density)),
        Int64 => Arc::new(create_primitive_array::<Int64Type>(size, null_density)),
        UInt8 => Arc::new(create_primitive_array::<UInt8Type>(size, null_density)),
        UInt16 => Arc::new(create_primitive_array::<UInt16Type>(size, null_density)),
        UInt32 => Arc::new(create_primitive_array::<UInt32Type>(size, null_density)),
        UInt64 => Arc::new(create_primitive_array::<UInt64Type>(size, null_density)),
        Float16 => {
            return Err(ArrowError::NotYetImplemented(
                "Float16 is not implememted".to_string(),
            ))
        }
        Float32 => Arc::new(create_primitive_array::<Float32Type>(size, null_density)),
        Float64 => Arc::new(create_primitive_array::<Float64Type>(size, null_density)),
        Timestamp(_, _) => {
            let int64_array =
                Arc::new(create_primitive_array::<Int64Type>(size, null_density))
                    as ArrayRef;
            return crate::compute::cast(&int64_array, field.data_type());
        }
        Date32 => Arc::new(create_primitive_array::<Date32Type>(size, null_density)),
        Date64 => Arc::new(create_primitive_array::<Date64Type>(size, null_density)),
        Time32(unit) => match unit {
            TimeUnit::Second => Arc::new(create_primitive_array::<Time32SecondType>(
                size,
                null_density,
            )) as ArrayRef,
            TimeUnit::Millisecond => Arc::new(create_primitive_array::<
                Time32MillisecondType,
            >(size, null_density)),
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported unit {:?} for Time32",
                    unit
                )))
            }
        },
        Time64(unit) => match unit {
            TimeUnit::Microsecond => Arc::new(create_primitive_array::<
                Time64MicrosecondType,
            >(size, null_density)) as ArrayRef,
            TimeUnit::Nanosecond => Arc::new(create_primitive_array::<
                Time64NanosecondType,
            >(size, null_density)),
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported unit {:?} for Time64",
                    unit
                )))
            }
        },
        Utf8 => Arc::new(create_string_array::<i32>(size, null_density)),
        LargeUtf8 => Arc::new(create_string_array::<i64>(size, null_density)),
        Binary => Arc::new(create_binary_array::<i32>(size, null_density)),
        LargeBinary => Arc::new(create_binary_array::<i64>(size, null_density)),
        FixedSizeBinary(len) => {
            Arc::new(create_fsb_array(size, null_density, *len as usize))
        }
        List(_) => create_random_list_array(field, size, null_density, true_density)?,
        LargeList(_) => {
            create_random_list_array(field, size, null_density, true_density)?
        }
        Struct(fields) => Arc::new(StructArray::try_from(
            fields
                .iter()
                .map(|struct_field| {
                    create_random_array(struct_field, size, null_density, true_density)
                        .map(|array_ref| (field.name().as_str(), array_ref))
                })
                .collect::<Result<Vec<(&str, ArrayRef)>>>()?,
        )?),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Generating random arrays not yet implemented for {:?}",
                other
            )))
        }
    })
}

#[inline]
fn create_random_list_array(
    field: &Field,
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<ArrayRef> {
    let list_field;
    let (offsets, child_len) = match field.data_type() {
        DataType::List(f) => {
            let (offsets, child_len) = create_random_offsets::<i32>(size, 1, 3);
            list_field = f;
            (Buffer::from(offsets.to_byte_slice()), child_len as usize)
        }
        DataType::LargeList(f) => {
            let (offsets, child_len) = create_random_offsets::<i64>(size, 1, 3);
            list_field = f;
            (Buffer::from(offsets.to_byte_slice()), child_len as usize)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot create list array for field {:?}",
                field
            )))
        }
    };

    // Create list's child data
    let child_array =
        create_random_array(list_field, child_len as usize, null_density, true_density)?;
    let child_data = child_array.data();
    // Create list's null buffers, if it is nullable
    let null_buffer = match field.is_nullable() {
        true => Some(create_random_null_buffer(size, null_density)),
        false => None,
    };
    let list_data = ArrayData::new(
        field.data_type().clone(),
        size,
        None,
        null_buffer,
        0,
        vec![offsets],
        vec![child_data.clone()],
    );
    Ok(make_array(list_data))
}

/// Generate random offsets for list arrays
fn create_random_offsets<T: OffsetSizeTrait + SampleUniform>(
    size: usize,
    min: T,
    max: T,
) -> (Vec<T>, T) {
    let rng = &mut seedable_rng();

    let mut current_offset = T::zero();

    let mut offsets = Vec::with_capacity(size + 1);

    (0..size).for_each(|_| {
        current_offset += rng.gen_range(min, max);
        offsets.push(current_offset);
    });

    (offsets, current_offset)
}

fn create_random_null_buffer(size: usize, null_density: f32) -> Buffer {
    let mut rng = seedable_rng();
    let mut mut_buf = MutableBuffer::new_null(size);
    {
        let mut_slice = mut_buf.as_slice_mut();
        (0..size).for_each(|i| {
            if rng.gen::<f32>() >= null_density {
                bit_util::set_bit(mut_slice, i)
            }
        })
    };
    mut_buf.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_batch() {
        let size = 32;
        let fields = vec![Field::new("a", DataType::Int32, true)];
        let schema = Schema::new(fields);
        let schema_ref = Arc::new(schema.clone());
        let batch = create_random_batch(schema_ref.clone(), size, 0.35, 0.7).unwrap();

        assert_eq!(batch.schema(), schema_ref);
        assert_eq!(batch.num_columns(), schema_ref.fields().len());
        for array in batch.columns() {
            assert_eq!(array.len(), size);
        }
    }
}
