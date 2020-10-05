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

use std::rc::Rc;

use arrow::array as arrow_array;
use arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, PrimitiveArrayOps};

use super::schema::add_encoded_arrow_schema_to_metadata;
use crate::column::writer::ColumnWriter;
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
            Rc::new(props),
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
        let mut levels = vec![];
        batch.columns().iter().for_each(|array| {
            let mut array_levels =
                get_levels(array, 0, &vec![1i16; batch.num_rows()][..], None);
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
    mut levels: &mut Vec<Levels>,
) -> Result<()> {
    match array.data_type() {
        ArrowDataType::Int8
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
        ArrowDataType::Float16 => Err(ParquetError::ArrowError(
            "Float16 arrays not supported".to_string(),
        )),
        ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::Null
        | ArrowDataType::Boolean
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::Union(_)
        | ArrowDataType::Dictionary(_, _) => Err(ParquetError::NYI(
            "Attempting to write an Arrow type that is not yet implemented".to_string(),
        )),
    }
}

fn write_leaf(
    writer: &mut ColumnWriter,
    column: &arrow_array::ArrayRef,
    levels: Levels,
) -> Result<i64> {
    let written = match writer {
        ColumnWriter::Int32ColumnWriter(ref mut typed) => {
            let array = arrow::compute::cast(column, &ArrowDataType::Int32)?;
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("Unable to get int32 array");
            typed.write_batch(
                get_numeric_array_slice::<Int32Type, _>(&array).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::BoolColumnWriter(ref mut _typed) => {
            unreachable!("Currently unreachable because data type not supported")
        }
        ColumnWriter::Int64ColumnWriter(ref mut typed) => {
            let array = arrow_array::Int64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<Int64Type, _>(&array).as_slice(),
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
                get_numeric_array_slice::<FloatType, _>(&array).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::DoubleColumnWriter(ref mut typed) => {
            let array = arrow_array::Float64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<DoubleType, _>(&array).as_slice(),
                Some(levels.definition.as_slice()),
                levels.repetition.as_deref(),
            )?
        }
        ColumnWriter::ByteArrayColumnWriter(ref mut typed) => match column.data_type() {
            ArrowDataType::Binary | ArrowDataType::Utf8 => {
                let array = arrow_array::BinaryArray::from(column.data());
                typed.write_batch(
                    get_binary_array(&array).as_slice(),
                    Some(levels.definition.as_slice()),
                    levels.repetition.as_deref(),
                )?
            }
            ArrowDataType::LargeBinary | ArrowDataType::LargeUtf8 => {
                let array = arrow_array::LargeBinaryArray::from(column.data());
                typed.write_batch(
                    get_large_binary_array(&array).as_slice(),
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

/// A struct that represents definition and repetition levels.
/// Repetition levels are only populated if the parent or current leaf is repeated
#[derive(Debug)]
struct Levels {
    definition: Vec<i16>,
    repetition: Option<Vec<i16>>,
}

/// Compute nested levels of the Arrow array, recursing into lists and structs
fn get_levels(
    array: &arrow_array::ArrayRef,
    level: i16,
    parent_def_levels: &[i16],
    parent_rep_levels: Option<&[i16]>,
) -> Vec<Levels> {
    match array.data_type() {
        ArrowDataType::Null => unimplemented!(),
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
        | ArrowDataType::LargeBinary => vec![Levels {
            definition: get_primitive_def_levels(array, parent_def_levels),
            repetition: None,
        }],
        ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
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
            let rep_levels: Vec<i16> = parent_rep_levels
                .map(|l| l.to_vec())
                .unwrap_or_else(|| vec![0i16; parent_def_levels.len()]);
            parent_def_levels
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
                ArrowDataType::Null => unimplemented!(),
                ArrowDataType::Boolean => unimplemented!(),
                ArrowDataType::Int8
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
                    let def_levels =
                        get_primitive_def_levels(&child_array, &list_def_levels[..]);
                    vec![Levels {
                        definition: def_levels,
                        repetition: Some(list_rep_levels),
                    }]
                }
                ArrowDataType::Binary
                | ArrowDataType::Utf8
                | ArrowDataType::LargeUtf8 => unimplemented!(),
                ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
                ArrowDataType::LargeBinary => unimplemented!(),
                ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
                    // nested list
                    unimplemented!()
                }
                ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
                ArrowDataType::Struct(_) => get_levels(
                    array,
                    level + 1, // indicates a nesting level of 2 (list + struct)
                    &list_def_levels[..],
                    Some(&list_rep_levels[..]),
                ),
                ArrowDataType::Union(_) => unimplemented!(),
                ArrowDataType::Dictionary(_, _) => unimplemented!(),
            }
        }
        ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
        ArrowDataType::Struct(_) => {
            let struct_array: &arrow_array::StructArray = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("Unable to get struct array");
            let mut struct_def_levels = Vec::with_capacity(struct_array.len());
            for i in 0..array.len() {
                struct_def_levels.push(level + struct_array.is_valid(i) as i16);
            }
            // trying to create levels for struct's fields
            let mut struct_levels = vec![];
            struct_array.columns().into_iter().for_each(|col| {
                let mut levels =
                    get_levels(col, level + 1, &struct_def_levels[..], parent_rep_levels);
                struct_levels.append(&mut levels);
            });
            struct_levels
        }
        ArrowDataType::Union(_) => unimplemented!(),
        ArrowDataType::Dictionary(_, _) => unimplemented!(),
    }
}

/// Get the definition levels of the numeric array, with level 0 being null and 1 being not null
/// In the case where the array in question is a child of either a list or struct, the levels
/// are incremented in accordance with the `level` parameter.
/// Parent levels are either 0 or 1, and are used to higher (correct terminology?) leaves as null
fn get_primitive_def_levels(
    array: &arrow_array::ArrayRef,
    parent_def_levels: &[i16],
) -> Vec<i16> {
    let mut array_index = 0;
    let max_def_level = parent_def_levels.iter().max().unwrap();
    let mut primitive_def_levels = vec![];
    parent_def_levels.iter().for_each(|def_level| {
        if def_level < max_def_level {
            primitive_def_levels.push(*def_level);
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
                    let bytes = ByteArray::from(array.value(i).to_vec());
                    values.push(bytes);
                }
            }
            values
        }
    };
}

def_get_binary_array_fn!(get_binary_array, arrow_array::BinaryArray);
def_get_binary_array_fn!(get_large_binary_array, arrow_array::LargeBinaryArray);

/// Get the underlying numeric array slice, skipping any null values.
/// If there are no null values, it might be quicker to get the slice directly instead of
/// calling this function.
fn get_numeric_array_slice<T, A>(array: &arrow_array::PrimitiveArray<A>) -> Vec<T::T>
where
    T: DataType,
    A: arrow::datatypes::ArrowNumericType,
    T::T: From<A::Native>,
{
    let mut values = Vec::with_capacity(array.len() - array.null_count());
    for i in 0..array.len() {
        if array.is_valid(i) {
            values.push(array.value(i).into())
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Seek;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::ToByteSlice;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::arrow::{ArrowReader, ParquetFileArrowReader};
    use crate::file::{metadata::KeyValue, reader::SerializedFileReader};
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
    fn arrow_writer_list() {
        // define schema
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::List(Box::new(DataType::Int32)),
            false,
        )]);

        // create some data
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[false], [true, false], null, [true, false, true], [false, true, false, true]]
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let a_list_data = ArrayData::builder(DataType::List(Box::new(DataType::Int32)))
            .len(5)
            .add_buffer(a_value_offsets)
            .add_child_data(a_values.data())
            .build();
        let a = ListArray::from(a_list_data);

        // build a record batch
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)]).unwrap();

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
        let mut arrow_reader = ParquetFileArrowReader::new(Rc::new(file_reader));
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
    fn arrow_writer_complex() {
        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g =
            Field::new("g", DataType::List(Box::new(DataType::Int16)), false);
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
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue {
                key: "test_key".to_string(),
                value: Some("test_value".to_string()),
            }]))
            .build();

        let file = get_temp_file("test_arrow_writer_complex.parquet", &[]);
        let mut writer =
            ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    const SMALL_SIZE: usize = 100;

    fn roundtrip(filename: &str, expected_batch: RecordBatch) {
        let file = get_temp_file(filename, &[]);

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            expected_batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&expected_batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Rc::new(reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(1024).unwrap();

        let actual_batch = record_batch_reader.next().unwrap().unwrap();

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        for i in 0..expected_batch.num_columns() {
            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data.data_type(), actual_data.data_type());
            assert_eq!(expected_data.len(), actual_data.len());
            assert_eq!(expected_data.null_count(), actual_data.null_count());
            assert_eq!(expected_data.offset(), actual_data.offset());
            assert_eq!(expected_data.buffers(), actual_data.buffers());
            assert_eq!(expected_data.child_data(), actual_data.child_data());
            assert_eq!(expected_data.null_bitmap(), actual_data.null_bitmap());
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
    #[should_panic(expected = "Null arrays not supported")]
    fn null_single_column() {
        let values = Arc::new(NullArray::new(SMALL_SIZE));
        one_column_roundtrip("null_single_column", values.clone(), true);
        one_column_roundtrip("null_single_column", values, false);
    }

    #[test]
    #[should_panic(
        expected = "Attempting to write an Arrow type that is not yet implemented"
    )]
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
    #[ignore] // Timestamp support isn't correct yet
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
    #[ignore] // Timestamp support isn't correct yet
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
    #[ignore] // DateUnit resolution mismatch
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
    #[ignore] // DateUnit resolution mismatch
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
    #[ignore] // Binary support isn't correct yet - null_bitmap doesn't match
    fn binary_single_column() {
        let one_vec: Vec<u8> = (0..SMALL_SIZE as u8).collect();
        let many_vecs: Vec<_> = std::iter::repeat(one_vec).take(SMALL_SIZE).collect();
        let many_vecs_iter = many_vecs.iter().map(|v| v.as_slice());

        // BinaryArrays can't be built from Vec<Option<&str>>, so only call `values_required`
        values_required::<BinaryArray, _>(many_vecs_iter, "binary_single_column");
    }

    #[test]
    #[ignore] // Large Binary support isn't correct yet
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
    #[ignore] // String support isn't correct yet - null_bitmap doesn't match
    fn string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<StringArray, _>(raw_strs, "string_single_column");
    }

    #[test]
    #[ignore] // Large String support isn't correct yet - null_bitmap and buffers don't match
    fn large_string_single_column() {
        let raw_values: Vec<_> = (0..SMALL_SIZE).map(|i| i.to_string()).collect();
        let raw_strs = raw_values.iter().map(|s| s.as_str());

        required_and_optional::<LargeStringArray, _>(
            raw_strs,
            "large_string_single_column",
        );
    }

    #[test]
    #[should_panic(
        expected = "Reading parquet list array into arrow is not supported yet!"
    )]
    fn list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data = ArrayData::builder(DataType::List(Box::new(DataType::Int32)))
            .len(5)
            .add_buffer(a_value_offsets)
            .add_child_data(a_values.data())
            .build();
        let a = ListArray::from(a_list_data);

        let values = Arc::new(a);
        one_column_roundtrip("list_single_column", values, false);
    }

    #[test]
    #[should_panic(
        expected = "Reading parquet list array into arrow is not supported yet!"
    )]
    fn large_list_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0i64, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_data =
            ArrayData::builder(DataType::LargeList(Box::new(DataType::Int32)))
                .len(5)
                .add_buffer(a_value_offsets)
                .add_child_data(a_values.data())
                .build();
        let a = LargeListArray::from(a_list_data);

        let values = Arc::new(a);
        one_column_roundtrip("large_list_single_column", values, false);
    }

    #[test]
    #[ignore] // Struct support isn't correct yet - null_bitmap doesn't match
    fn struct_single_column() {
        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let struct_field_a = Field::new("f", DataType::Int32, false);
        let s = StructArray::from(vec![(struct_field_a, Arc::new(a_values) as ArrayRef)]);

        let values = Arc::new(s);
        one_column_roundtrip("struct_single_column", values, false);
    }
}
