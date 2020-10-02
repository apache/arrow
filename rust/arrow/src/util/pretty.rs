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

//! Utilities for printing record batches

use crate::array;
use crate::array::{Array, PrimitiveArrayOps};
use crate::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::record_batch::RecordBatch;

use array::DictionaryArray;
use prettytable::format;
use prettytable::{Cell, Row, Table};

use crate::error::{ArrowError, Result};

///! Create a visual representation of record batches
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<String> {
    Ok(create_table(results)?.to_string())
}

///! Prints a visual representation of record batches to stdout
pub fn print_batches(results: &[RecordBatch]) -> Result<()> {
    create_table(results)?.printstd();
    Ok(())
}

///! Convert a series of record batches into a table
fn create_table(results: &[RecordBatch]) -> Result<Table> {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(&field.name()));
    }
    table.set_titles(Row::new(header));

    for batch in results {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&array_value_to_string(&column, row)?));
            }
            table.add_row(Row::new(cells));
        }
    }

    Ok(table)
}

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = if array.is_null($row) {
            "".to_string()
        } else {
            array.value($row).to_string()
        };

        Ok(s)
    }};
}

/// Get the value at the given row in an array as a String
pub fn array_value_to_string(column: &array::ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Utf8 => make_string!(array::StringArray, column, row),
        DataType::Boolean => make_string!(array::BooleanArray, column, row),
        DataType::Int16 => make_string!(array::Int16Array, column, row),
        DataType::Int32 => make_string!(array::Int32Array, column, row),
        DataType::Int64 => make_string!(array::Int64Array, column, row),
        DataType::UInt8 => make_string!(array::UInt8Array, column, row),
        DataType::UInt16 => make_string!(array::UInt16Array, column, row),
        DataType::UInt32 => make_string!(array::UInt32Array, column, row),
        DataType::UInt64 => make_string!(array::UInt64Array, column, row),
        DataType::Float16 => make_string!(array::Float32Array, column, row),
        DataType::Float32 => make_string!(array::Float32Array, column, row),
        DataType::Float64 => make_string!(array::Float64Array, column, row),
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
            make_string!(array::TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
            make_string!(array::TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
            make_string!(array::TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
            make_string!(array::TimestampNanosecondArray, column, row)
        }
        DataType::Date32(_) => make_string!(array::Date32Array, column, row),
        DataType::Date64(_) => make_string!(array::Date64Array, column, row),
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            make_string!(array::Time32SecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            make_string!(array::Time32MillisecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Microsecond => {
            make_string!(array::Time64MicrosecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            make_string!(array::Time64NanosecondArray, column, row)
        }
        DataType::Dictionary(index_type, _value_type) => match **index_type {
            DataType::Int8 => dict_array_value_to_string::<Int8Type>(column, row),
            DataType::Int16 => dict_array_value_to_string::<Int16Type>(column, row),
            DataType::Int32 => dict_array_value_to_string::<Int32Type>(column, row),
            DataType::Int64 => dict_array_value_to_string::<Int64Type>(column, row),
            DataType::UInt8 => dict_array_value_to_string::<UInt8Type>(column, row),
            DataType::UInt16 => dict_array_value_to_string::<UInt16Type>(column, row),
            DataType::UInt32 => dict_array_value_to_string::<UInt32Type>(column, row),
            DataType::UInt64 => dict_array_value_to_string::<UInt64Type>(column, row),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Pretty printing not supported for {:?} due to index type",
                column.data_type()
            ))),
        },
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Pretty printing not implemented for {:?} type",
            column.data_type()
        ))),
    }
}

/// Converts the value of the dictionary array at `row` to a String
fn dict_array_value_to_string<K: ArrowPrimitiveType>(
    colum: &array::ArrayRef,
    row: usize,
) -> Result<String> {
    let dict_array = colum.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    let keys_array = dict_array.keys_array();

    if keys_array.is_null(row) {
        return Ok(String::from(""));
    }

    let dict_index = keys_array.value(row).to_usize().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Can not convert value {:?} at index {:?} to usize for repl.",
            keys_array.value(row),
            row
        ))
    })?;

    array_value_to_string(&dict_array.values(), dict_index)
}

#[cfg(test)]
mod tests {
    use array::{PrimitiveBuilder, StringBuilder, StringDictionaryBuilder};

    use super::*;
    use crate::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_pretty_format_batches() -> Result<()> {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("d"),
                ])),
                Arc::new(array::Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(10),
                    Some(100),
                ])),
            ],
        )?;

        let table = pretty_format_batches(&[batch])?;

        let expected = vec![
            "+---+-----+",
            "| a | b   |",
            "+---+-----+",
            "| a | 1   |",
            "| b |     |",
            "|   | 10  |",
            "| d | 100 |",
            "+---+-----+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }

    #[test]
    fn test_pretty_format_dictionary() -> Result<()> {
        // define a schema.
        let field_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![Field::new("d1", field_type, true)]));

        let keys_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let values_builder = StringBuilder::new(10);
        let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        builder.append("one")?;
        builder.append_null()?;
        builder.append("three")?;
        let array = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema.clone(), vec![array])?;

        let table = pretty_format_batches(&[batch])?;

        let expected = vec![
            "+-------+",
            "| d1    |",
            "+-------+",
            "| one   |",
            "|       |",
            "| three |",
            "+-------+",
        ];

        let actual: Vec<&str> = table.lines().collect();

        assert_eq!(expected, actual, "Actual result:\n{}", table);

        Ok(())
    }
}
