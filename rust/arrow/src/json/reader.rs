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

//! JSON Reader
//!
//! This JSON reader allows JSON line-delimited files to be read into the Arrow memory
//! model. Records are loaded in batches and are then converted from row-based data to
//! columnar data.
//!
//! Example:
//!
//! ```
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use std::fs::File;
//! use std::io::BufReader;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("a", DataType::Float64, false),
//!     Field::new("b", DataType::Float64, false),
//!     Field::new("c", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/basic.json").unwrap();
//!
//! let mut json = json::Reader::new(BufReader::new(file), Arc::new(schema), 1024, None);
//! let batch = json.next().unwrap().unwrap();
//! ```

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use serde_json::Value;

use crate::array::*;
use crate::builder::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

/// Coerce data type, e.g. Int64 and Float64 should be Float64
fn coerce_data_type(dt: Vec<&DataType>, is_list: bool) -> DataType {
    let scalar_type = match dt.len() {
        1 => dt[0].clone(),
        2 => {
            if dt.contains(&&DataType::Float64) && dt.contains(&&DataType::Int64) {
                DataType::Float64
            } else {
                DataType::Utf8
            }
        }
        _ => DataType::Utf8,
    };
    if is_list {
        DataType::List(Box::new(scalar_type))
    } else {
        scalar_type
    }
}

/// Generate schema from JSON field names and inferred data types
fn generate_schema(spec: HashMap<String, HashSet<DataType>>) -> Arc<Schema> {
    let fields = spec
        .iter()
        .map(|(k, hs)| {
            let v: Vec<&DataType> = hs.iter().collect();
            Field::new(k, coerce_data_type(v, false), true)
        })
        .collect();
    let schema = Schema::new(fields);
    Arc::new(schema)
}

/// Infer the fields of a JSON file by reading the first n records of the file, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
fn infer_json_schema(file: File, max_read_records: Option<usize>) -> Result<Arc<Schema>> {
    let mut values: HashMap<String, HashSet<DataType>> = HashMap::new();
    let mut reader = BufReader::new(file.try_clone()?);

    let mut line = String::new();
    for _ in 0..max_read_records.unwrap_or(std::usize::MAX) {
        &reader.read_line(&mut line)?;
        if line.is_empty() {
            break;
        }
        let record: Value = serde_json::from_str(&line.trim()).expect("Not valid JSON");

        line = String::new();

        match record {
            Value::Object(map) => {
                let res = map
                    .iter()
                    .map(|(k, v)| {
                        match v {
                            Value::Array(a) => {
                                // collect the data types in array
                                let types: Result<Vec<Option<&DataType>>> = a
                                    .iter()
                                    .map(|a| match a {
                                        Value::Null => Ok(None),
                                        Value::Number(n) => {
                                            if n.is_i64() {
                                                Ok(Some(&DataType::Int64))
                                            } else {
                                                Ok(Some(&DataType::Float64))
                                            }
                                        }
                                        Value::Bool(_) => Ok(Some(&DataType::Boolean)),
                                        Value::String(_) => Ok(Some(&DataType::Utf8)),
                                        Value::Array(_) | Value::Object(_) => {
                                            Err(ArrowError::JsonError(
                                                "Nested lists and structs not supported"
                                                    .to_string(),
                                            ))
                                        }
                                    })
                                    .collect();
                                match types {
                                    Ok(types) => {
                                        // unwrap the Option and discard None values (from
                                        // JSON nulls)
                                        let mut types: Vec<&DataType> =
                                            types.into_iter().filter_map(|t| t).collect();
                                        types.dedup();
                                        // if a record contains only nulls, it is not
                                        // added to values
                                        if !types.is_empty() {
                                            let dt = coerce_data_type(types, true);

                                            if values.contains_key(k) {
                                                let x = values.get_mut(k).unwrap();
                                                x.insert(dt);
                                            } else {
                                                // create hashset and add value type
                                                let mut hs = HashSet::new();
                                                hs.insert(dt);
                                                values.insert(k.to_string(), hs);
                                            }
                                        }
                                        Ok(())
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                            Value::Bool(_) => {
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(DataType::Boolean);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(DataType::Boolean);
                                    values.insert(k.to_string(), hs);
                                }
                                Ok(())
                            }
                            Value::Null => {
                                // do nothing, we treat json as nullable by default when
                                // inferring
                                Ok(())
                            }
                            Value::Number(n) => {
                                if n.is_f64() {
                                    if values.contains_key(k) {
                                        let x = values.get_mut(k).unwrap();
                                        x.insert(DataType::Float64);
                                    } else {
                                        // create hashset and add value type
                                        let mut hs = HashSet::new();
                                        hs.insert(DataType::Float64);
                                        values.insert(k.to_string(), hs);
                                    }
                                } else {
                                    // default to i64
                                    if values.contains_key(k) {
                                        let x = values.get_mut(k).unwrap();
                                        x.insert(DataType::Int64);
                                    } else {
                                        // create hashset and add value type
                                        let mut hs = HashSet::new();
                                        hs.insert(DataType::Int64);
                                        values.insert(k.to_string(), hs);
                                    }
                                }
                                Ok(())
                            }
                            Value::String(_) => {
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(DataType::Utf8);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(DataType::Utf8);
                                    values.insert(k.to_string(), hs);
                                }
                                Ok(())
                            }
                            Value::Object(_) => Err(ArrowError::JsonError(
                                "Reading nested JSON structes currently not supported"
                                    .to_string(),
                            )),
                        }
                    })
                    .collect();
                match res {
                    Ok(()) => {}
                    Err(e) => return Err(e),
                }
            }
            t @ _ => {
                return Err(ArrowError::JsonError(format!(
                    "Expected JSON record to be an object, found {:?}",
                    t
                )));
            }
        };
    }

    let schema = generate_schema(values);

    // return the reader seek back to the start
    &reader.into_inner().seek(SeekFrom::Start(0))?;

    Ok(schema)
}

/// JSON file reader
pub struct Reader<R: Read> {
    /// Explicit schema for the JSON file
    schema: Arc<Schema>,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// File reader
    reader: BufReader<R>,
    /// Batch size (number of records to load each time)
    batch_size: usize,
}

impl<R: Read> Reader<R> {
    /// Create a new JSON Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn new(
        reader: BufReader<R>,
        schema: Arc<Schema>,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            schema,
            projection,
            reader,
            batch_size,
        }
    }

    /// Read the next batch of records
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows: Vec<Value> = Vec::with_capacity(self.batch_size);
        let mut line = String::new();
        for _ in 0..self.batch_size {
            self.reader.read_line(&mut line)?;
            if !line.is_empty() {
                rows.push(serde_json::from_str(&line).expect("Not valid JSON"));
                line = String::new();
            } else {
                break;
            }
        }

        let rows = &rows[..];
        let projection = self.projection.clone().unwrap_or(vec![]);
        let arrays: Result<Vec<ArrayRef>> = self
            .schema
            .clone()
            .fields()
            .iter()
            .filter(|field| {
                if projection.is_empty() {
                    return true;
                }
                projection.contains(field.name())
            })
            .map(|field| {
                match field.data_type().clone() {
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, field.name())
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, field.name())
                    }
                    DataType::Int64 => self.build_primitive_array::<Int64Type>(rows, field.name()),
                    DataType::Int32 => self.build_primitive_array::<Int32Type>(rows, field.name()),
                    DataType::Int16 => self.build_primitive_array::<Int16Type>(rows, field.name()),
                    DataType::Int8 => self.build_primitive_array::<Int8Type>(rows, field.name()),
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, field.name())
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, field.name())
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, field.name())
                    }
                    DataType::UInt8 => self.build_primitive_array::<UInt8Type>(rows, field.name()),
                    DataType::Utf8 => {
                        let mut builder = BinaryBuilder::new(rows.len());
                        for row_index in 0..rows.len() {
                            match rows[row_index].get(field.name()) {
                                Some(value) => {
                                    match value.as_str() {
                                        Some(v) => builder.append_string(v)?,
                                        // TODO: value might exist as something else, coerce so we don't lose it
                                        None => builder.append(false)?,
                                    }
                                }
                                None => builder.append(false)?,
                            }
                        }
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    DataType::List(ref t) => match t {
                        box DataType::Int8 => self.build_list_array::<Int8Type>(rows, field.name()),
                        box DataType::Int16 => self.build_list_array::<Int16Type>(rows, field.name()),
                        box DataType::Int32 => self.build_list_array::<Int32Type>(rows, field.name()),
                        box DataType::Int64 => self.build_list_array::<Int64Type>(rows, field.name()),
                        box DataType::UInt8 => self.build_list_array::<UInt8Type>(rows, field.name()),
                        box DataType::UInt16 => self.build_list_array::<UInt16Type>(rows, field.name()),
                        box DataType::UInt32 => self.build_list_array::<UInt32Type>(rows, field.name()),
                        box DataType::UInt64 => self.build_list_array::<UInt64Type>(rows, field.name()),
                        box DataType::Float32 => self.build_list_array::<Float32Type>(rows, field.name()),
                        box DataType::Float64 => self.build_list_array::<Float64Type>(rows, field.name()),
                        box DataType::Boolean => self.build_boolean_list_array(rows, field.name()),
                        box DataType::Utf8 => {
                            let values_builder = BinaryBuilder::new(rows.len() * 5);
                            let mut builder = ListBuilder::new(values_builder);
                            for row_index in 0..rows.len() {
                                match rows[row_index].get(field.name()) {
                                    Some(value) => {
                                        // value can be an array or a scalar
                                        let vals: Vec<Option<&str>> = if let Value::String(v) = value {
                                            vec![Some(v)]
                                        } else if let Value::Array(n) = value {
                                            n.iter().map(|v: &Value| v.as_str()).collect()
                                        } else {
                                            return Err(ArrowError::JsonError("Only scalars are currently supported in JSON arrays".to_string()))
                                        };
                                        for i in 0..vals.len() {
                                            match vals[i] {
                                                Some(v) => builder.values().append_string(v)?,
                                                None => builder.values().append_null()?,
                                            };
                                        }
                                    }
                                    None => {}
                                }
                                builder.append(true)?
                            }
                            Ok(Arc::new(builder.finish()) as ArrayRef)
                        }
                        _ => return Err(ArrowError::JsonError("Data type is currently not supported in a list".to_string())),
                    },
                    _ => return Err(ArrowError::JsonError("struct types are not yet supported".to_string())),
                }
            })
            .collect();

        match arrays {
            Ok(arr) => Ok(Some(RecordBatch::new(self.schema.clone(), arr))),
            Err(e) => Err(e),
        }
    }

    fn build_boolean_array(&self, rows: &[Value], col_name: &str) -> Result<ArrayRef> {
        let mut builder = BooleanBuilder::new(rows.len());
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => match value.as_bool() {
                    Some(v) => builder.append_value(v)?,
                    None => builder.append_null()?,
                },
                None => {
                    builder.append_null()?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_boolean_list_array(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef> {
        let values_builder = BooleanBuilder::new(rows.len() * 5);
        let mut builder = ListBuilder::new(values_builder);
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => {
                    // value can be an array or a scalar
                    let vals: Vec<Option<bool>> = if let Value::Bool(v) = value {
                        vec![Some(*v)]
                    } else if let Value::Array(n) = value {
                        n.iter().map(|v: &Value| v.as_bool()).collect()
                    } else {
                        return Err(ArrowError::JsonError(
                            "Only scalars are currently supported in JSON arrays"
                                .to_string(),
                        ));
                    };
                    for i in 0..vals.len() {
                        match vals[i] {
                            Some(v) => builder.values().append_value(v)?,
                            None => builder.values().append_null()?,
                        };
                    }
                }
                None => {}
            }
            builder.append(true)?
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_primitive_array<T: ArrowPrimitiveType>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef>
    where
        T: ArrowNumericType,
        T::Native: num::NumCast,
    {
        let mut builder = PrimitiveBuilder::<T>::new(rows.len());
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => {
                    // check that value is of expected datatype
                    match value.as_f64() {
                        Some(v) => match num::cast::cast(v) {
                            Some(v) => builder.append_value(v)?,
                            None => builder.append_null()?,
                        },
                        None => builder.append_null()?,
                    }
                }
                None => {
                    builder.append_null()?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_list_array<T: ArrowPrimitiveType>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef>
    where
        T::Native: num::NumCast,
    {
        let values_builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(rows.len());
        let mut builder = ListBuilder::new(values_builder);
        for row_index in 0..rows.len() {
            match rows[row_index].get(col_name) {
                Some(value) => {
                    // value can be an array or a scalar
                    let vals: Vec<Option<f64>> = if let Value::Number(value) = value {
                        vec![value.as_f64()]
                    } else if let Value::Array(n) = value {
                        n.iter().map(|v: &Value| v.as_f64()).collect()
                    } else {
                        return Err(ArrowError::JsonError(
                            "Only scalars are currently supported in JSON arrays"
                                .to_string(),
                        ));
                    };
                    for i in 0..vals.len() {
                        match vals[i] {
                            Some(v) => match num::cast::cast(v) {
                                Some(v) => builder.values().append_value(v)?,
                                None => builder.values().append_null()?,
                            },
                            None => builder.values().append_null()?,
                        };
                    }
                }
                None => {}
            }
            builder.append(true)?
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// JSON file reader builder
pub struct ReaderBuilder {
    /// Optional schema for the JSON file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the JSON structure.
    schema: Option<Arc<Schema>>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            max_records: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring JSON parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate arrow;
    ///
    /// use arrow::json;
    /// use std::fs::File;
    ///
    /// fn example() -> json::Reader<File> {
    ///     let file = File::open("test/data/basic.json").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = json::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build::<File>(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the JSON file's schema
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the JSON reader to infer the schema of the file
    pub fn infer_schema(mut self, max_records: Option<usize>) -> Self {
        // remove any schema that is set
        self.schema = None;
        self.max_records = max_records;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R: Read>(self, file: File) -> Result<Reader<File>> {
        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                let inferred = infer_json_schema(file.try_clone()?, self.max_records)?;

                inferred
            }
        };
        let buf_reader = BufReader::new(file);
        Ok(Reader::new(
            buf_reader,
            schema,
            self.batch_size,
            self.projection,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_basic() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(false, cc.value(0));
        assert_eq!(true, cc.value(10));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!("4", String::from_utf8(dd.value(0).to_vec()).unwrap());
        assert_eq!("text", String::from_utf8(dd.value(8).to_vec()).unwrap());
    }

    #[test]
    fn test_json_basic_with_nulls() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(true, bb.is_valid(0));
        assert_eq!(false, bb.is_valid(2));
        assert_eq!(false, bb.is_valid(11));
        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(true, cc.is_valid(0));
        assert_eq!(false, cc.is_valid(4));
        assert_eq!(false, cc.is_valid(11));
        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(false, dd.is_valid(0));
        assert_eq!(true, dd.is_valid(1));
        assert_eq!(false, dd.is_valid(4));
        assert_eq!(false, dd.is_valid(11));
    }

    #[test]
    fn test_json_basic_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::Utf8, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            BufReader::new(File::open("test/data/basic.json").unwrap()),
            Arc::new(schema),
            1024,
            None,
        );
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int32, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(&DataType::Float32, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        // test that a 64bit value is returned as null due to overflowing
        assert_eq!(false, aa.is_valid(11));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-3.5, bb.value(1));
    }

    #[test]
    fn test_json_basic_schema_projection() {
        // We test implicit and explicit projection:
        // Implicit: omitting fields from a schema
        // Explicit: supplying a vec of fields to take
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Boolean, false),
        ]);

        let mut reader: Reader<File> = Reader::new(
            BufReader::new(File::open("test/data/basic.json").unwrap()),
            Arc::new(schema),
            1024,
            Some(vec!["a".to_string(), "c".to_string()]),
        );
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(2, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int32, a.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(&DataType::Boolean, c.1.data_type());
    }

    #[test]
    fn test_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = batch.schema();

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(
            &DataType::List(Box::new(DataType::Float64)),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Box::new(DataType::Boolean)),
            c.1.data_type()
        );
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(&DataType::Utf8, d.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(1, aa.value(0));
        assert_eq!(-10, aa.value(1));
        let bb = batch
            .column(b.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bb = bb.values();
        let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(9, bb.len());
        assert_eq!(2.0, bb.value(0));
        assert_eq!(-6.1, bb.value(5));
        assert_eq!(false, bb.is_valid(7));

        let cc = batch
            .column(c.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let cc = cc.values();
        let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(6, cc.len());
        assert_eq!(false, cc.value(0));
        assert_eq!(false, cc.value(4));
        assert_eq!(false, cc.is_valid(5));
    }

    #[test]
    #[should_panic(expected = "Not valid JSON")]
    fn test_invalid_file() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/uk_cities_with_headers.csv").unwrap())
            .unwrap();
        let _batch = reader.next().unwrap().unwrap();
    }
}
