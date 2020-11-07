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

//! Contains writer which writes arrow data into parquet data.

use std::sync::Arc;

use arrow::array as arrow_array;
use arrow::datatypes::{DataType as ArrowDataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;

use super::levels::LevelInfo;
use super::schema::add_encoded_arrow_schema_to_metadata;

use crate::column::writer::{ColumnWriter, ColumnWriterImpl};
use crate::errors::{ParquetError, Result};
use crate::file::properties::WriterProperties;
use crate::{
    data_type::*,
    file::writer::{FileWriter, ParquetWriter, RowGroupWriter, SerializedFileWriter},
};

/// Arrow writer
///
/// Writes Arrow `RecordBatch`es to a Parquet writer
pub struct ArrowWriter<W: ParquetWriter> {
    /// Underlying Parquet writer
    writer: SerializedFileWriter<W>,
    /// A copy of the Arrow schema.
    ///
    /// The schema is used to verify that each record batch written has the correct schema
    arrow_schema: SchemaRef,
}

impl<W: 'static + ParquetWriter> ArrowWriter<W> {
    /// Try to create a new Arrow writer
    ///
    /// The writer will fail if:
    ///  * a `SerializedFileWriter` cannot be created from the ParquetWriter
    ///  * the Arrow schema contains unsupported datatypes such as Unions
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let schema = crate::arrow::arrow_to_parquet_schema(&arrow_schema)?;
        // add serialized arrow schema
        let mut props = props.unwrap_or_else(|| WriterProperties::builder().build());
        add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);

        let file_writer = SerializedFileWriter::new(
            writer.try_clone()?,
            schema.root_schema_ptr(),
            Arc::new(props),
        )?;

        Ok(Self {
            writer: file_writer,
            arrow_schema,
        })
    }

    /// Write a RecordBatch to writer
    ///
    /// *NOTE:* The writer currently does not support all Arrow data types
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // validate batch schema against writer's supplied schema
        if self.arrow_schema != batch.schema() {
            return Err(ParquetError::ArrowError(
                "Record batch schema does not match writer schema".to_string(),
            ));
        }
        // compute the definition and repetition levels of the batch
        let num_rows = batch.num_rows();
        let mut levels = vec![];
        let batch_level = LevelInfo {
            definition: vec![1; num_rows],
            repetition: None,
            definition_mask: vec![(true, 1); num_rows],
            array_offsets: (0..=(num_rows as i64)).collect(),
            array_mask: vec![true; num_rows],
            max_definition: 1,
            is_list: false,
            is_nullable: true, // setting as null treats non-null structs correctly
        };
        // TODO: between `max_definition` and `level` below, one might have to be 0
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels =
                    calculate_array_levels(array, field, 1, &batch_level);
                levels.append(&mut array_levels);
            });
        // reverse levels so we can use Vec::pop(&mut self)
        levels.reverse();

        let mut row_group_writer = self.writer.next_row_group()?;

        // write leaves
        for column in batch.columns() {
            write_leaves(&mut row_group_writer, column, &mut levels)?;
        }

        self.writer.close_row_group(row_group_writer)
    }

    /// Close and finalise the underlying Parquet writer
    pub fn close(&mut self) -> Result<()> {
        self.writer.close()
    }
}

/// Convenience method to get the next ColumnWriter from the RowGroupWriter
#[inline]
#[allow(clippy::borrowed_box)]
fn get_col_writer(
    row_group_writer: &mut Box<dyn RowGroupWriter>,
) -> Result<ColumnWriter> {
    let col_writer = row_group_writer
        .next_column()?
        .expect("Unable to get column writer");
    Ok(col_writer)
}

#[allow(clippy::borrowed_box)]
fn write_leaves(
    mut row_group_writer: &mut Box<dyn RowGroupWriter>,
    array: &arrow_array::ArrayRef,
    mut levels: &mut Vec<LevelInfo>,
) -> Result<()> {
    match array.data_type() {
        ArrowDataType::Null
        | ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Date32(_)
        | ArrowDataType::Date64(_)
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::LargeBinary
        | ArrowDataType::Binary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8 => {
            let mut col_writer = get_col_writer(&mut row_group_writer)?;
            write_leaf(
                &mut col_writer,
                array,
                levels.pop().expect("Levels exhausted"),
            )?;
            row_group_writer.close_column(col_writer)?;
            Ok(())
        }
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
            // write the child list
            let data = array.data();
            let child_array = arrow_array::make_array(data.child_data()[0].clone());
            write_leaves(&mut row_group_writer, &child_array, &mut levels)?;
            Ok(())
        }
        ArrowDataType::Struct(_) => {
            let struct_array: &arrow_array::StructArray = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("Unable to get struct array");
            for field in struct_array.columns() {
                write_leaves(&mut row_group_writer, field, &mut levels)?;
            }
            Ok(())
        }
        ArrowDataType::Dictionary(key_type, value_type) => {
            use arrow_array::{PrimitiveArray, StringArray};
            use ArrowDataType::*;
            use ColumnWriter::*;

            let array = &**array;
            let mut col_writer = get_col_writer(&mut row_group_writer)?;
            let levels = levels.pop().expect("Levels exhausted");

            macro_rules! dispatch_dictionary {
                ($($kt: pat, $vt: pat, $w: ident => $kat: ty, $vat: ty,)*) => (
                    match (&**key_type, &**value_type, &mut col_writer) {
                        $(($kt, $vt, $w(writer)) => write_dict::<$kat, $vat, _>(array, writer, levels),)*
                        (kt, vt, _) => unreachable!("Shouldn't be attempting to write dictionary of <{:?}, {:?}>", kt, vt),
                    }
                );
            }

            if let (UInt8, UInt32, Int32ColumnWriter(writer)) =
                (&**key_type, &**value_type, &mut col_writer)
            {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8DictionaryArray>()
                    .expect("Unable to get dictionary array");

                let keys = typed_array.keys();

                let value_buffer = typed_array.values();
                let value_array =
                    arrow::compute::cast(&value_buffer, &ArrowDataType::Int32)?;

                let values = value_array
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap();

                use std::convert::TryFrom;
                // This removes NULL values from the keys, but
                // they're encoded by the levels, so that's fine.

                // nevi-me: if we materialize values by iterating on the array, can't we instead 'just' cast to the values?
                // in the failing dictionary test, the materialized values here are incorrect (missing 22345678)
                let materialized_values: Vec<_> = keys
                    .into_iter()
                    .flatten()
                    .map(|key| {
                        usize::try_from(key)
                            .unwrap_or_else(|k| panic!("key {} does not fit in usize", k))
                    })
                    .map(|key| values.value(key))
                    .collect();

                let materialized_primitive_array =
                    PrimitiveArray::<arrow::datatypes::Int32Type>::from(
                        materialized_values,
                    );

                // I added this because we need to consider dictionaries in structs correctly,
                // I don't think it's the cause for the failing test though, as the materialized_p_arr
                // in the test is incorrect when it gets here (missing 22345678 value)
                let indices = filter_array_indices(&levels);
                let values = get_numeric_array_slice::<Int32Type, _>(
                    &materialized_primitive_array,
                    &indices,
                );

                writer.write_batch(
                    values.as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?;
                row_group_writer.close_column(col_writer)?;

                return Ok(());
            }

            dispatch_dictionary!(
                Int8, Utf8, ByteArrayColumnWriter => arrow::datatypes::Int8Type, StringArray,
                Int16, Utf8, ByteArrayColumnWriter => arrow::datatypes::Int16Type, StringArray,
                Int32, Utf8, ByteArrayColumnWriter => arrow::datatypes::Int32Type, StringArray,
                Int64, Utf8, ByteArrayColumnWriter => arrow::datatypes::Int64Type, StringArray,
                UInt8, Utf8, ByteArrayColumnWriter => arrow::datatypes::UInt8Type, StringArray,
                UInt16, Utf8, ByteArrayColumnWriter => arrow::datatypes::UInt16Type, StringArray,
                UInt32, Utf8, ByteArrayColumnWriter => arrow::datatypes::UInt32Type, StringArray,
                UInt64, Utf8, ByteArrayColumnWriter => arrow::datatypes::UInt64Type, StringArray,
            )?;

            row_group_writer.close_column(col_writer)?;

            Ok(())
        }
        ArrowDataType::Float16 => Err(ParquetError::ArrowError(
            "Float16 arrays not supported".to_string(),
        )),
        ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::Decimal(_, _)
        | ArrowDataType::Union(_) => Err(ParquetError::NYI(
            "Attempting to write an Arrow type that is not yet implemented".to_string(),
        )),
    }
}

trait Materialize<K, V> {
    type Output;

    // Materialize the packed dictionary. The writer will later repack it.
    fn materialize(&self) -> Vec<Self::Output>;
}

impl<K> Materialize<K, arrow_array::StringArray> for dyn Array
where
    K: arrow::datatypes::ArrowDictionaryKeyType,
{
    type Output = ByteArray;

    fn materialize(&self) -> Vec<Self::Output> {
        use arrow::datatypes::ArrowNativeType;

        let typed_array = self
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<K>>()
            .expect("Unable to get dictionary array");

        let keys = typed_array.keys();

        let value_buffer = typed_array.values();
        let values = value_buffer
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        // This removes NULL values from the keys, but
        // they're encoded by the levels, so that's fine.
        keys.into_iter()
            .flatten()
            .map(|key| {
                key.to_usize()
                    .unwrap_or_else(|| panic!("key {:?} does not fit in usize", key))
            })
            .map(|key| values.value(key))
            .map(ByteArray::from)
            .collect()
    }
}

fn write_dict<K, V, T>(
    array: &(dyn Array + 'static),
    writer: &mut ColumnWriterImpl<T>,
    levels: LevelInfo,
) -> Result<()>
where
    T: DataType,
    dyn Array: Materialize<K, V, Output = T::T>,
{
    writer.write_batch(
        &array.materialize(),
        Some(levels.definition.as_slice()),
        levels.repetition.as_deref(),
    )?;

    Ok(())
}

fn write_leaf(
    writer: &mut ColumnWriter,
    column: &arrow_array::ArrayRef,
    levels: LevelInfo,
) -> Result<i64> {
    let indices = filter_array_indices(&levels);
    let written = match writer {
        ColumnWriter::Int32ColumnWriter(ref mut typed) => {
            let array = arrow::compute::cast(column, &ArrowDataType::Int32)?;
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("Unable to get int32 array");
            // assigning values to make it easier to debug
            let slice = get_numeric_array_slice::<Int32Type, _>(&array, &indices); // TODO: this function is incomplete as it doesn't take into account the actual definition in slicing
            typed.write_batch(
                slice.as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::BoolColumnWriter(ref mut typed) => {
            let array = arrow_array::BooleanArray::from(column.data());
            typed.write_batch(
                get_bool_array_slice(&array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::Int64ColumnWriter(ref mut typed) => {
            let array = arrow_array::Int64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<Int64Type, _>(&array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::Int96ColumnWriter(ref mut _typed) => {
            unreachable!("Currently unreachable because data type not supported")
        }
        ColumnWriter::FloatColumnWriter(ref mut typed) => {
            let array = arrow_array::Float32Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<FloatType, _>(&array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::DoubleColumnWriter(ref mut typed) => {
            let array = arrow_array::Float64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<DoubleType, _>(&array, &indices).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::ByteArrayColumnWriter(ref mut typed) => match column.data_type() {
            ArrowDataType::Binary => {
                let array = arrow_array::BinaryArray::from(column.data());
                typed.write_batch(
                    get_binary_array(&array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::Utf8 => {
                let array = arrow_array::StringArray::from(column.data());
                typed.write_batch(
                    get_string_array(&array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::LargeBinary => {
                let array = arrow_array::LargeBinaryArray::from(column.data());
                typed.write_batch(
                    get_large_binary_array(&array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::LargeUtf8 => {
                let array = arrow_array::LargeStringArray::from(column.data());
                typed.write_batch(
                    get_large_string_array(&array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            _ => unreachable!("Currently unreachable because data type not supported"),
        },
        ColumnWriter::FixedLenByteArrayColumnWriter(ref mut _typed) => {
            unreachable!("Currently unreachable because data type not supported")
        }
    };
    Ok(written as i64)
}

/// Compute nested levels of the Arrow array, recursing into lists and structs
/// Returns a list of `LevelInfo`, where each level is for nested primitive arrays.
///
/// The algorithm works by eagerly incrementing non-null values, and decrementing
/// when a value is null.
///
/// *Examples:*
///
/// A record batch always starts at a populated definition = level 1.
/// When a batch only has a primitive, i.e. `<batch<primitive[a]>>, column `a`
/// can only have a maximum level of 1 if it is not null.
/// If it is null, we decrement by 1, such that the null slots will = level 0.
///
/// If a batch has nested arrays (list, struct, union, etc.), then the incrementing
/// takes place.
/// A `<batch<struct[a]<primitive[b]>>` will have up to 2 levels (if nullable).
/// When calculating levels for `a`, if the struct slot is not empty, we
/// increment by 1, such that we'd have `[2, 2, 2]` if all 3 slots are not null.
/// If there is an empty slot, we decrement, leaving us with `[2, 0, 2]` as the
/// null slot effectively means that no record is populated for the row altogether.
///
/// *Lists*
///
/// TODO
///
/// *Non-nullable arrays*
///
/// If an array is non-nullable, this is accounted for when converting the Arrow
/// schema to a Parquet schema.
/// When dealing with `<batch<primitive[_]>>` there is no issue, as the meximum
/// level will always be = 1.
///
/// When dealing with nested types, the logic becomes a bit complicate.
/// A non-nullable struct; `<batch<struct{non-null}[a]<primitive[b]>>>` will only
/// have 1 maximum level, where 0 means `b` is nul, and 1 means `b` is not null.
///
/// We account for the above by checking if the `Field` is nullable, and adjusting
/// the [inc|dec]rement accordingly.
fn calculate_array_levels(
    array: &arrow_array::ArrayRef,
    field: &Field,
    level: i16,
    level_info: &LevelInfo,
) -> Vec<LevelInfo> {
    match array.data_type() {
        ArrowDataType::Null => vec![LevelInfo {
            definition: level_info
                .definition
                .iter()
                .map(|d| (d - 1).max(0))
                .collect(),
            repetition: level_info.repetition.clone(),
            definition_mask: level_info.definition_mask.clone(),
            array_offsets: level_info.array_offsets.clone(),
            array_mask: level_info.array_mask.clone(),
            max_definition: level,
            is_list: level_info.is_list,
            is_nullable: true, // always nullable as all values are nulls
        }],
        ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float16
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Date32(_)
        | ArrowDataType::Date64(_)
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::Binary
        | ArrowDataType::LargeBinary => vec![LevelInfo {
            definition: get_primitive_def_levels(array, field, &level_info.definition),
            repetition: level_info.repetition.clone(),
            definition_mask: level_info.definition_mask.clone(),
            array_offsets: level_info.array_offsets.clone(),
            array_mask: level_info.array_mask.clone(),
            is_list: level_info.is_list,
            max_definition: level,
            is_nullable: field.is_nullable(),
        }],
        ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
        ArrowDataType::Decimal(_, _) => unimplemented!(),
        ArrowDataType::List(list_field) | ArrowDataType::LargeList(list_field) => {
            let array_data = array.data();
            let child_data = array_data.child_data().get(0).unwrap();
            // get offsets, accounting for large offsets if present
            let offsets: Vec<i64> = {
                if let ArrowDataType::LargeList(_) = array.data_type() {
                    unsafe { array_data.buffers()[0].typed_data::<i64>() }.to_vec()
                } else {
                    let offsets = unsafe { array_data.buffers()[0].typed_data::<i32>() };
                    offsets.to_vec().into_iter().map(|v| v as i64).collect()
                }
            };
            let child_array = arrow_array::make_array(child_data.clone());

            let mut list_def_levels = Vec::with_capacity(child_array.len());
            let mut list_rep_levels = Vec::with_capacity(child_array.len());
            let rep_levels: Vec<i16> = level_info
                .repetition
                .clone()
                .map(|l| l.to_vec())
                .unwrap_or_else(|| vec![0i16; level_info.definition.len()]);
            level_info
                .definition
                .iter()
                .zip(rep_levels)
                .zip(offsets.windows(2))
                .for_each(|((parent_def_level, parent_rep_level), window)| {
                    if *parent_def_level == 0 {
                        // parent is null, list element must also be null
                        list_def_levels.push(0);
                        list_rep_levels.push(0);
                    } else {
                        // parent is not null, check if list is empty or null
                        let start = window[0];
                        let end = window[1];
                        let len = end - start;
                        if len == 0 {
                            list_def_levels.push(*parent_def_level - 1);
                            list_rep_levels.push(parent_rep_level);
                        } else {
                            list_def_levels.push(*parent_def_level);
                            list_rep_levels.push(parent_rep_level);
                            for _ in 1..len {
                                list_def_levels.push(*parent_def_level);
                                list_rep_levels.push(parent_rep_level + 1);
                            }
                        }
                    }
                });

            // if datatype is a primitive, we can construct levels of the child array
            match child_array.data_type() {
                // TODO: The behaviour of a <list<null>> is untested
                ArrowDataType::Null => vec![LevelInfo {
                    definition: list_def_levels,
                    repetition: Some(list_rep_levels),
                    definition_mask: level_info.definition_mask.clone(), // TODO: list mask
                    array_offsets: offsets,
                    array_mask: level_info.array_mask.clone(), // TODO: list mask
                    is_list: true,
                    is_nullable: list_field.is_nullable(),
                    max_definition: level + 1, // TODO: compute correctly
                }],
                ArrowDataType::Boolean
                | ArrowDataType::Int8
                | ArrowDataType::Int16
                | ArrowDataType::Int32
                | ArrowDataType::Int64
                | ArrowDataType::UInt8
                | ArrowDataType::UInt16
                | ArrowDataType::UInt32
                | ArrowDataType::UInt64
                | ArrowDataType::Float16
                | ArrowDataType::Float32
                | ArrowDataType::Float64
                | ArrowDataType::Timestamp(_, _)
                | ArrowDataType::Date32(_)
                | ArrowDataType::Date64(_)
                | ArrowDataType::Time32(_)
                | ArrowDataType::Time64(_)
                | ArrowDataType::Duration(_)
                | ArrowDataType::Interval(_) => {
                    let def_levels = get_primitive_def_levels(
                        &child_array,
                        list_field,
                        &list_def_levels[..],
                    );
                    vec![LevelInfo {
                        definition: def_levels,
                        repetition: Some(list_rep_levels),
                        array_mask: vec![],
                        array_offsets: vec![],
                        definition_mask: vec![],
                        is_list: true,
                        is_nullable: list_field.is_nullable(),
                        max_definition: level + 1, // TODO: update
                    }]
                }
                ArrowDataType::Binary
                | ArrowDataType::Utf8
                | ArrowDataType::LargeUtf8 => unimplemented!(),
                ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
                ArrowDataType::Decimal(_, _) => unimplemented!(),
                ArrowDataType::LargeBinary => unimplemented!(),
                ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
                    // nested list
                    unimplemented!()
                }
                ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
                ArrowDataType::Struct(_) => {
                    let struct_level_info = LevelInfo {
                        definition: list_def_levels,
                        repetition: Some(list_rep_levels),
                        definition_mask: vec![],
                        array_offsets: vec![],
                        array_mask: vec![],
                        max_definition: level + 1,
                        is_list: list_field.is_nullable(),
                        is_nullable: true, // // indicates a nesting level of 2 (list + struct)
                    };
                    calculate_array_levels(
                        array,
                        list_field,
                        level + 1, // indicates a nesting level of 2 (list + struct)
                        &struct_level_info,
                    )
                }
                ArrowDataType::Union(_) => unimplemented!(),
                ArrowDataType::Dictionary(_, _) => unimplemented!(),
            }
        }
        ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
        ArrowDataType::Struct(struct_fields) => {
            let struct_array: &arrow_array::StructArray = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("Unable to get struct array");
            let array_len = struct_array.len();
            let mut struct_def_levels = Vec::with_capacity(array_len);
            let mut struct_mask = Vec::with_capacity(array_len);
            // we can have a <struct<struct<_>>, in which case we should check
            // the parent struct in the child struct's offsets
            for (i, def_level) in level_info.definition.iter().enumerate() {
                if *def_level == level {
                    if !field.is_nullable() {
                        // push the level as is
                        struct_def_levels.push(level);
                    } else if struct_array.is_valid(i) {
                        // Increment to indicate that this value is not null
                        // The next level will decrement if it is null
                        // we can check if current value is null
                        struct_def_levels.push(level + 1);
                    } else {
                        // decrement to show that only the previous level is populated
                        // we only decrement if previous field is nullable
                        struct_def_levels.push(level - (level_info.is_nullable as i16));
                    }
                } else {
                    struct_def_levels.push(*def_level);
                }
                // TODO: is it more efficient to use `bitvec` here?
                struct_mask.push(struct_array.is_valid(i));
            }
            // trying to create levels for struct's fields
            let mut struct_levels = vec![];
            let struct_level_info = LevelInfo {
                definition: struct_def_levels,
                // TODO: inherit the parent's repetition? (relevant for <list<struct<_>>)
                repetition: level_info.repetition.clone(),
                // Is it correct to increment this by 1 level?
                definition_mask: level_info
                    .definition_mask
                    .iter()
                    .map(|(state, index)| (*state, index + 1))
                    .collect(),
                // logically, a struct should inherit its parent's offsets
                array_offsets: level_info.array_offsets.clone(),
                // this should be just the struct's mask, not its parent's
                array_mask: struct_mask,
                max_definition: level_info.max_definition + (field.is_nullable() as i16),
                is_list: level_info.is_list,
                is_nullable: field.is_nullable(),
            };
            struct_array
                .columns()
                .into_iter()
                .zip(struct_fields)
                .for_each(|(col, struct_field)| {
                    let mut levels = calculate_array_levels(
                        col,
                        struct_field,
                        level + (field.is_nullable() as i16),
                        &struct_level_info,
                    );
                    struct_levels.append(&mut levels);
                });
            struct_levels
        }
        ArrowDataType::Union(_) => unimplemented!(),
        ArrowDataType::Dictionary(_, _) => {
            // Need to check for these cases not implemented in C++:
            // - "Writing DictionaryArray with nested dictionary type not yet supported"
            // - "Writing DictionaryArray with null encoded in dictionary type not yet supported"
            vec![LevelInfo {
                definition: get_primitive_def_levels(
                    array,
                    field,
                    &level_info.definition,
                ),
                repetition: level_info.repetition.clone(),
                definition_mask: level_info.definition_mask.clone(),
                array_offsets: level_info.array_offsets.clone(),
                array_mask: level_info.array_mask.clone(),
                is_list: level_info.is_list,
                max_definition: level,
                is_nullable: field.is_nullable(),
            }]
        }
    }
}

/// Get the definition levels of the numeric array, with level 0 being null and 1 being not null
/// In the case where the array in question is a child of either a list or struct, the levels
/// are incremented in accordance with the `level` parameter.
/// Parent levels are either 0 or 1, and are used to higher (correct terminology?) leaves as null
///
/// TODO: (a comment to remove, note to help me reduce the mental bookkeeping)
/// We want an array's levels to be additive here, i.e. if we have an array that
/// comes from <batch<primitive>>, we should consume &[0; array.len()], so that
/// we add values to it, instead of subtract values
///
/// An alternaitve is to pass the max level, and use it to compute whether we
/// should increment (though this is likely tricker)
fn get_primitive_def_levels(
    array: &arrow_array::ArrayRef,
    field: &Field,
    parent_def_levels: &[i16],
) -> Vec<i16> {
    let mut array_index = 0;
    let max_def_level = parent_def_levels.iter().max().unwrap();
    let mut primitive_def_levels = vec![];
    parent_def_levels.iter().for_each(|def_level| {
        // TODO: if field is non-nullable, can its parent be nullable? Ideally shouldn't
        // being non-null means that for a level > 1, then we should subtract 1?
        if !field.is_nullable() && *max_def_level > 1 {
            primitive_def_levels.push(*def_level - 1);
            array_index += 1;
        } else if def_level < max_def_level {
            primitive_def_levels.push(*def_level);
            array_index += 1;
        } else {
            primitive_def_levels.push(def_level - array.is_null(array_index) as i16);
            array_index += 1;
        }
    });
    primitive_def_levels
}

macro_rules! def_get_binary_array_fn {
    ($name:ident, $ty:ty) => {
        fn $name(array: &$ty) -> Vec<ByteArray> {
            let mut values = Vec::with_capacity(array.len() - array.null_count());
            for i in 0..array.len() {
                if array.is_valid(i) {
                    let bytes: Vec<u8> = array.value(i).into();
                    let bytes = ByteArray::from(bytes);
                    values.push(bytes);
                }
            }
            values
        }
    };
}

def_get_binary_array_fn!(get_binary_array, arrow_array::BinaryArray);
def_get_binary_array_fn!(get_string_array, arrow_array::StringArray);
def_get_binary_array_fn!(get_large_binary_array, arrow_array::LargeBinaryArray);
def_get_binary_array_fn!(get_large_string_array, arrow_array::LargeStringArray);

/// Get the underlying numeric array slice, skipping any null values.
/// If there are no null values, it might be quicker to get the slice directly instead of
/// calling this function.
fn get_numeric_array_slice<T, A>(
    array: &arrow_array::PrimitiveArray<A>,
    indices: &[usize],
) -> Vec<T::T>
where
    T: DataType,
    A: arrow::datatypes::ArrowNumericType,
    T::T: From<A::Native>,
{
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        values.push(array.value(*i).into())
    }
    values
}

fn get_bool_array_slice(
    array: &arrow_array::BooleanArray,
    indices: &[usize],
) -> Vec<bool> {
    let mut values = Vec::with_capacity(indices.len());
    for i in indices {
        values.push(array.value(*i))
    }
    values
}

/// Given a level's information, calculate the offsets required to index an array
/// correctly.
fn filter_array_indices(level: &LevelInfo) -> Vec<usize> {
    // TODO: we don't quite get the def levels right all the time, so for now we recalculate it
    // this has the downside that if no values are populated, the slicing will be wrong

    // TODO: we should reliably track this, to avoid finding the max value
    let max_def = level.definition.iter().max().cloned().unwrap();
    let mut filtered = vec![];
    // remove slots that are false from definition_mask
    let mut index = 0;
    level
        .definition
        .iter()
        .zip(&level.definition_mask)
        .for_each(|(def, (mask, _))| {
            if *mask {
                if *def == max_def {
                    filtered.push(index);
                }
                index += 1;
            }
        });
    filtered
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Seek;
    use std::sync::Arc;

    use arrow::datatypes::ToByteSlice;
    use arrow::datatypes::{DataType, Field, Schema, UInt32Type, UInt8Type};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, buffer::Buffer};

    use crate::arrow::{ArrowReader, ParquetFileArrowReader};
    use crate::file::{reader::SerializedFileReader, writer::InMemoryWriteableCursor};
    use crate::util::test_common::get_temp_file;

    #[test]
    fn arrow_writer() {
        // define schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )
        .unwrap();

        let file = get_temp_file("test_arrow_writer.parquet", &[]);
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn roundtrip_bytes() {
        // define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);

        // build a record batch
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let cursor = InMemoryWriteableCursor::default();

        {
            let mut writer = ArrowWriter::try_new(cursor.clone(), schema, None).unwrap();
            writer.write(&expected_batch).unwrap();
            writer.close().unwrap();
        }

        let buffer = cursor.into_inner().unwrap();

        let cursor = crate::file::serialized_reader::SliceableCursor::new(buffer);
        let reader = SerializedFileReader::new(cursor).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data, actual_data);
        }
    }

    #[test]
    #[ignore = "repetitions might be incorrect, will be addressed as part of ARROW-9728"]
    fn arrow_writer_non_null() {
        // define schema
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)]).unwrap();

        let file = get_temp_file("test_arrow_writer_non_null.parquet", &[]);
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    #[ignore = "list support is incomplete"]
    fn arrow_writer_list() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            false,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "items",
            DataType::Int32,
            true,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data())
        .build();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)]).unwrap();

        // I think this setup is incorrect because this should pass
        assert_eq!(batch.column(0).data().null_count(), 1);

        let file = get_temp_file("test_arrow_writer_list.parquet", &[]);
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn arrow_writer_binary() {
        let string_field = Field::new("a", DataType::Utf8, false);
        let binary_field = Field::new("b", DataType::Binary, false);
        let schema = Schema::new(vec![string_field, binary_field]);

        let raw_string_values = vec!["foo", "bar", "baz", "quux"];
        let raw_binary_values = vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"baz".to_vec(),
            b"quux".to_vec(),
        ];
        let raw_binary_value_refs = raw_binary_values
            .iter()
            .map(|x| x.as_slice())
            .collect::<Vec<_>>();

        let string_values = StringArray::from(raw_string_values.clone());
        let binary_values = BinaryArray::from(raw_binary_value_refs);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(string_values), Arc::new(binary_values)],
        )
        .unwrap();

        let mut file = get_temp_file("test_arrow_writer_binary.parquet", &[]);
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), Arc::new(schema), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let file_reader = SerializedFileReader::new(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let batch = record_batch_reader.next().unwrap().unwrap();
        let string_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let binary_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(string_col.value(i), raw_string_values[i]);
            assert_eq!(binary_col.value(i), raw_binary_values[i].as_slice());
        }
    }

    #[test]
    #[ignore = "list support is incomplete"]
    fn arrow_writer_complex() {
        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g = Field::new(
            "g",
            DataType::List(Box::new(Field::new("items", DataType::Int16, false))),
            false,
        );
        let struct_field_e = Field::new(
            "e",
            DataType::Struct(vec![struct_field_f.clone(), struct_field_g.clone()]),
            true,
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "c",
                DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()]),
                false,
            ),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
        let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

        let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let g_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.data())
            .build();
        let g = ListArray::from(g_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        roundtrip("test_arrow_writer_complex.parquet", batch);
    }

    #[test]
    fn arrow_writer_2_level_struct() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, true);
        let field_b = Field::new("b", DataType::Struct(vec![field_c]), true);
        let field_a = Field::new("a", DataType::Struct(vec![field_b.clone()]), true);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![Some(1), None, Some(3), None, None, Some(6)]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00100111]))
            .add_child_data(c.data())
            .build();
        let b = StructArray::from(b_data);
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00101111]))
            .add_child_data(b.data())
            .build();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 1);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip("test_arrow_writer_2_level_struct.parquet", batch);
    }

    #[test]
    fn arrow_writer_2_level_struct_non_null() {
        // tests writing <struct<struct<primitive>>
        let field_c = Field::new("c", DataType::Int32, false);
        let field_b = Field::new("b", DataType::Struct(vec![field_c]), true);
        let field_a = Field::new("a", DataType::Struct(vec![field_b.clone()]), false);
        let schema = Schema::new(vec![field_a.clone()]);

        // create data
        let c = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let b_data = ArrayDataBuilder::new(field_b.data_type().clone())
            .len(6)
            .null_bit_buffer(Buffer::from(vec![0b00100111]))
            .add_child_data(c.data())
            .build();
        let b = StructArray::from(b_data);
        // a intentionally has no null buffer, to test that this is handled correctly
        let a_data = ArrayDataBuilder::new(field_a.data_type().clone())
            .len(6)
            .add_child_data(b.data())
            .build();
        let a = StructArray::from(a_data);

        assert_eq!(a.null_count(), 0);
        assert_eq!(a.column(0).null_count(), 2);

        // build a racord batch
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        roundtrip("test_arrow_writer_2_level_struct_non_null.parquet", batch);
    }

    const SMALL_SIZE: usize = 100;

    fn roundtrip(filename: &str, expected_batch: RecordBatch) {
        let file = get_temp_file(filename, &[]);

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            expected_batch.schema(),
            None,
        )
        .expect("Unable to write file");
        writer.write(&expected_batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data, actual_data);
        }
    }

    fn one_column_roundtrip(filename: &str, values: ArrayRef, nullable: bool) {
        let schema = Schema::new(vec![Field::new(
            "col",
            values.data_type().clone(),
            nullable,
        )]);
        let expected_batch =
            RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

        roundtrip(filename, expected_batch);
    }

    fn values_required<A, I>(iter: I, filename: &str)
    where
        A: From<Vec<I::Item>> + Array + 'static,
        I: IntoIterator,
    {
        let raw_values: Vec<_> = iter.into_iter().collect();
        let values = Arc::new(A::from(raw_values));
        one_column_roundtrip(filename, values, false);
    }

    fn values_optional<A, I>(iter: I, filename: &str)
    where
        A: From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator,
    {
        let optional_raw_values: Vec<_> = iter
            .into_iter()
            .enumerate()
            .map(|(i, v)| if i % 2 == 0 { None } else { Some(v) })
            .collect();
        let optional_values = Arc::new(A::from(optional_raw_values));
        one_column_roundtrip(filename, optional_values, true);
    }

    fn required_and_optional<A, I>(iter: I, filename: &str)
    where
        A: From<Vec<I::Item>> + From<Vec<Option<I::Item>>> + Array + 'static,
        I: IntoIterator + Clone,
    {
        values_required::<A, I>(iter.clone(), filename);
        values_optional::<A, I>(iter, filename);
    }

    #[test]
    fn all_null_primitive_single_column() {
        let values = Arc::new(Int32Array::from(vec![None; SMALL_SIZE]));
        one_column_roundtrip("all_null_primitive_single_column", values, true);
    }
    #[test]
    fn null_single_column() {
        let values = Arc::new(NullArray::new(SMALL_SIZE));
        one_column_roundtrip("null_single_column", values, true);
        // null arrays are always nullable, a test with non-nullable nulls fails
    }

    #[test]
    fn bool_single_column() {
        required_and_optional::<BooleanArray, _>(
            [true, false].iter().cycle().copied().take(SMALL_SIZE),
            "bool_single_column",
        );
    }

    #[test]
    fn i8_single_column() {
        required_and_optional::<Int8Array, _>(0..SMALL_SIZE as i8, "i8_single_column");
    }

    #[test]
    fn i16_single_column() {
        required_and_optional::<Int16Array, _>(0..SMALL_SIZE as i16, "i16_single_column");
    }

    #[test]
    fn i32_single_column() {
        required_and_optional::<Int32Array, _>(0..SMALL_SIZE as i32, "i32_single_column");
    }

    #[test]
    fn i64_single_column() {
        required_and_optional::<Int64Array, _>(0..SMALL_SIZE as i64, "i64_single_column");
    }

    #[test]
    fn u8_single_column() {
        required_and_optional::<UInt8Array, _>(0..SMALL_SIZE as u8, "u8_single_column");
    }

    #[test]
    fn u16_single_column() {
        required_and_optional::<UInt16Array, _>(
            0..SMALL_SIZE as u16,
            "u16_single_column",
        );
    }

    #[test]
    fn u32_single_column() {
        required_and_optional::<UInt32Array, _>(
            0..SMALL_SIZE as u32,
            "u32_single_column",
        );
    }

    #[test]
    fn u64_single_column() {
        required_and_optional::<UInt64Array, _>(
            0..SMALL_SIZE as u64,
            "u64_single_column",
        );
    }

    #[test]
    fn f32_single_column() {
        required_and_optional::<Float32Array, _>(
            (0..SMALL_SIZE).map(|i| i as f32),
            "f32_single_column",
        );
    }

    #[test]
    fn f64_single_column() {
        required_and_optional::<Float64Array, _>(
            (0..SMALL_SIZE).map(|i| i as f64),
            "f64_single_column",
        );
    }

    // The timestamp array types don't implement From<Vec<T>> because they need the timezone
    // argument, and they also doesn't support building from a Vec<Option<T>>, so call
    // one_column_roundtrip manually instead of calling required_and_optional for these tests.

    #[test]
    fn timestamp_second_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampSecondArray::from_vec(raw_values, None));

        one_column_roundtrip("timestamp_second_single_column", values, false);
    }

    #[test]
    fn timestamp_millisecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMillisecondArray::from_vec(raw_values, None));

        one_column_roundtrip("timestamp_millisecond_single_column", values, false);
    }

    #[test]
    fn timestamp_microsecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampMicrosecondArray::from_vec(raw_values, None));

        one_column_roundtrip("timestamp_microsecond_single_column", values, false);
    }

    #[test]
    fn timestamp_nanosecond_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE as i64).collect();
        let values = Arc::new(TimestampNanosecondArray::from_vec(raw_values, None));

        one_column_roundtrip("timestamp_nanosecond_single_column", values, false);
    }

    #[test]
    fn date32_single_column() {
        required_and_optional::<Date32Array, _>(
            0..SMALL_SIZE as i32,
            "date32_single_column",
        );
    }

    #[test]
    #[ignore] // Date support isn't correct yet
    fn date64_single_column() {
        required_and_optional::<Date64Array, _>(
            0..SMALL_SIZE as i64,
            "date64_single_column",
        );
    }

    #[test]
    fn time32_second_single_column() {
        required_and_optional::<Time32SecondArray, _>(
            0..SMALL_SIZE as i32,
            "time32_second_single_column",
        );
    }

    #[test]
    fn time32_millisecond_single_column() {
        required_and_optional::<Time32MillisecondArray, _>(
            0..SMALL_SIZE as i32,
            "time32_millisecond_single_column",
        );
    }

    #[test]
    fn time64_microsecond_single_column() {
        required_and_optional::<Time64MicrosecondArray, _>(
            0..SMALL_SIZE as i64,
            "time64_microsecond_single_column",
        );
    }

    #[test]
    fn time64_nanosecond_single_column() {
        required_and_optional::<Time64NanosecondArray, _>(
            0..SMALL_SIZE as i64,
            "time64_nanosecond_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_second_single_column() {
        required_and_optional::<DurationSecondArray, _>(
            0..SMALL_SIZE as i64,
            "duration_second_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_millisecond_single_column() {
        required_and_optional::<DurationMillisecondArray, _>(
            0..SMALL_SIZE as i64,
            "duration_millisecond_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_microsecond_single_column() {
        required_and_optional::<DurationMicrosecondArray, _>(
            0..SMALL_SIZE as i64,
            "duration_microsecond_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Converting Duration to parquet not supported")]
    fn duration_nanosecond_single_column() {
        required_and_optional::<DurationNanosecondArray, _>(
            0..SMALL_SIZE as i64,
            "duration_nanosecond_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Currently unreachable because data type not supported")]
    fn interval_year_month_single_column() {
        required_and_optional::<IntervalYearMonthArray, _>(
            0..SMALL_SIZE as i32,
            "interval_year_month_single_column",
        );
    }

    #[test]
    #[should_panic(expected = "Currently unreachable because data type not supported")]
    fn interval_day_time_single_column() {
        required_and_optional::<IntervalDayTimeArray, _>(
            0..SMALL_SIZE as i64,
            "interval_day_time_single_column",
        );
    }

    #[test]
    fn binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<BinaryArray, _>(many_vecs_iter, "binary_single_column");
    }

    #[test]
    fn large_binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // LargeBinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<LargeBinaryArray, _>(
            many_vecs_iter,
            "large_binary_single_column",
        );
    }

    #[test]
    fn string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<StringArray, _>(raw_strs, "string_single_column");
    }

    #[test]
    fn large_string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<LargeStringArray, _>(
            raw_strs,
            "large_string_single_column",
        );
    }

    #[test]
    #[ignore = "list support is incomplete"]
    fn list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Box::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data())
        .build();

        // I think this setup is incorrect because this should pass
        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip("list_single_column", values, false);
    }

    #[test]
    #[ignore = "list support is incomplete"]
    fn large_list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0i64, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::LargeList(Box::new(Field::new(
            "large_item",
            DataType::Int32,
            true,
        ))))
        .len(5)
        .add_buffer(a_value_offsets)
        .add_child_data(a_values.data())
        .build();

        // I think this setup is incorrect because this should pass
        assert_eq!(a_list_data.null_count(), 1);

        let a = LargeListArray::from(a_list_data);
        let values = Arc::new(a);

        one_column_roundtrip("large_list_single_column", values, false);
    }

    #[test]
    fn struct_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let struct_field_a = Field::new("f", DataType::Int32, false);
        let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

        let values = Arc::new(s);
        one_column_roundtrip("struct_single_column", values, false);
    }

    #[test]
    fn arrow_writer_string_dictionary() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: Int32DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(
            "test_arrow_writer_string_dictionary.parquet",
            expected_batch,
        );
    }

    #[test]
    fn arrow_writer_primitive_dictionary() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32)),
            true,
            42,
            true,
        )]));

        // create some data
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        builder.append(12345678).unwrap();
        let d = builder.finish();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(
            "test_arrow_writer_primitive_dictionary.parquet",
            expected_batch,
        );
    }

    #[test]
    fn arrow_writer_string_dictionary_unsigned_index() {
        // define schema
        let schema = Arc::new(Schema::new(vec![Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
            42,
            true,
        )]));

        // create some data
        let d: UInt8DictionaryArray = [Some("alpha"), None, Some("beta"), Some("alpha")]
            .iter()
            .copied()
            .collect();

        // build a record batch
        let expected_batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();

        roundtrip(
            "test_arrow_writer_string_dictionary_unsigned_index.parquet",
            expected_batch,
        );
    }
}
