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

use std::fs::File;
use std::rc::Rc;

use array::Array;
use arrow::array;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::column::writer::ColumnWriter;
use crate::errors::Result;
use crate::file::properties::WriterProperties;
use crate::{
    data_type::*,
    file::writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
};

struct ArrowWriter {
    writer: SerializedFileWriter<File>,
    rows: i64,
}

impl ArrowWriter {
    pub fn try_new(file: File, arrow_schema: &Schema) -> Result<Self> {
        let schema = crate::arrow::arrow_to_parquet_schema(arrow_schema)?;
        let props = Rc::new(WriterProperties::builder().build());
        let file_writer = SerializedFileWriter::new(
            file.try_clone()?,
            schema.root_schema_ptr(),
            props,
        )?;

        Ok(Self {
            writer: file_writer,
            rows: 0,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut row_group_writer = self.writer.next_row_group()?;
        self.rows += unnest_arrays_to_leaves(
            &mut row_group_writer,
            batch.schema().fields(),
            batch.columns(),
            &vec![1i16; batch.num_rows()][..],
            0,
        )?;
        self.writer.close_row_group(row_group_writer)
    }

    pub fn close(&mut self) -> Result<()> {
        self.writer.close()
    }
}

/// Write nested arrays by traversing into structs and lists until primitive
/// arrays are found.
fn unnest_arrays_to_leaves(
    row_group_writer: &mut Box<dyn RowGroupWriter>,
    // The fields from the record batch or struct
    fields: &Vec<Field>,
    // The columns from record batch or struct, must have same length as fields
    columns: &[array::ArrayRef],
    // The parent mask, in the case of a struct, this represents which values
    // of the struct are true (1) or false(0).
    // This is useful to respect the definition level of structs where all values are null in a row
    parent_mask: &[i16],
    // The current level that is being read at
    level: i16,
) -> Result<i64> {
    let mut rows_written = 0;
    for (field, column) in fields.iter().zip(columns) {
        match field.data_type() {
            ArrowDataType::List(_dtype) => unimplemented!("list not yet implemented"),
            ArrowDataType::FixedSizeList(_, _) => {
                unimplemented!("fsl not yet implemented")
            }
            ArrowDataType::Struct(fields) => {
                // fields in a struct should recursively be written out
                let array = column
                    .as_any()
                    .downcast_ref::<array::StructArray>()
                    .expect("Unable to get struct array");
                let mut null_mask = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    null_mask.push(array.is_valid(i) as i16);
                }
                rows_written += unnest_arrays_to_leaves(
                    row_group_writer,
                    fields,
                    &array.columns_ref()[..],
                    &null_mask[..],
                    // if the field is nullable, we have to increment level
                    level + field.is_nullable() as i16,
                )?;
            }
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
            | ArrowDataType::Timestamp(_, _)
            | ArrowDataType::Date32(_)
            | ArrowDataType::Date64(_)
            | ArrowDataType::Time32(_)
            | ArrowDataType::Time64(_)
            | ArrowDataType::Duration(_)
            | ArrowDataType::Interval(_)
            | ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::Utf8 => {
                let col_writer = row_group_writer.next_column()?;
                if let Some(mut writer) = col_writer {
                    // write_column
                    rows_written +=
                        write_column(&mut writer, column, level, parent_mask)? as i64;
                    row_group_writer.close_column(writer)?;
                } else {
                    panic!("No writer found")
                }
            }
            ArrowDataType::Union(_) => unimplemented!(),
            ArrowDataType::Dictionary(_, _) => unimplemented!(),
        }
    }
    Ok(rows_written)
}

/// Write column to writer
fn write_column(
    writer: &mut ColumnWriter,
    column: &array::ArrayRef,
    level: i16,
    parent_levels: &[i16],
) -> Result<usize> {
    match writer {
        ColumnWriter::Int32ColumnWriter(ref mut typed) => {
            let array = array::Int32Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<Int32Type, _>(&array).as_slice(),
                Some(get_primitive_def_levels(column, level, parent_levels).as_slice()),
                None,
            )
        }
        ColumnWriter::BoolColumnWriter(ref mut _typed) => unimplemented!(),
        ColumnWriter::Int64ColumnWriter(ref mut typed) => {
            let array = array::Int64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<Int64Type, _>(&array).as_slice(),
                Some(get_primitive_def_levels(column, level, parent_levels).as_slice()),
                None,
            )
        }
        ColumnWriter::Int96ColumnWriter(ref mut _typed) => unimplemented!(),
        ColumnWriter::FloatColumnWriter(ref mut typed) => {
            let array = array::Float32Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<FloatType, _>(&array).as_slice(),
                Some(get_primitive_def_levels(column, level, parent_levels).as_slice()),
                None,
            )
        }
        ColumnWriter::DoubleColumnWriter(ref mut typed) => {
            let array = array::Float64Array::from(column.data());
            typed.write_batch(
                get_numeric_array_slice::<DoubleType, _>(&array).as_slice(),
                Some(get_primitive_def_levels(column, level, parent_levels).as_slice()),
                None,
            )
        }
        ColumnWriter::ByteArrayColumnWriter(ref mut _typed) => unimplemented!(),
        ColumnWriter::FixedLenByteArrayColumnWriter(ref mut _typed) => unimplemented!(),
    }
}

/// Get the definition levels of the numeric array, with level 0 being null and 1 being not null
/// In the case where the array in question is a child of either a list or struct, the levels
/// are incremented in accordance with the `level` parameter.
/// Parent levels are either 0 or 1, and are used to higher (correct terminology?) leaves as null
fn get_primitive_def_levels(
    array: &array::ArrayRef,
    level: i16,
    parent_levels: &[i16],
) -> Vec<i16> {
    // convince the compiler that bounds are fine
    let len = array.len();
    assert_eq!(
        len,
        parent_levels.len(),
        "Parent definition levels must equal array length"
    );
    let levels = (0..len)
        .map(|index| (array.is_valid(index) as i16 + level) * parent_levels[index])
        .collect();
    levels
}

/// Get the underlying numeric array slice, skipping any null values.
/// If there are no null values, the entire slice is returned,
/// thus this should only be called when there are null values.
fn get_numeric_array_slice<T, A>(array: &array::PrimitiveArray<A>) -> Vec<T::T>
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

    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::ToByteSlice;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

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

        let file = File::create("test.parquet").unwrap();
        let mut writer = ArrowWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn arrow_writer_complex() {
        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g =
            Field::new("g", DataType::List(Box::new(DataType::Boolean)), false);
        let struct_field_e = Field::new(
            "e",
            DataType::Struct(vec![
                struct_field_f.clone(),
                // struct_field_g.clone()
            ]),
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

        let g_value = BooleanArray::from(vec![
            false, true, false, true, false, true, false, true, false, true,
        ]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[false], [true, false], null, [true, false, true], [false, true, false, true]]
        let g_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets.clone())
            .add_child_data(g_value.data())
            .build();
        let _g = ListArray::from(g_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            // (struct_field_g, Arc::new(g) as ArrayRef),
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

        let file = File::create("test_complex.parquet").unwrap();
        let mut writer = ArrowWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
