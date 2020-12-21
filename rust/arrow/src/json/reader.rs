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

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::iter::FromIterator;
use std::sync::Arc;

use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::Value;

use crate::buffer::MutableBuffer;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;
use crate::util::bit_util;
use crate::{array::*, buffer::Buffer};

/// Coerce data type during inference
///
/// * `Int64` and `Float64` should be `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * All other types are coerced to `Utf8`
fn coerce_data_type(dt: Vec<&DataType>) -> Result<DataType> {
    match dt.len() {
        1 => Ok(dt[0].clone()),
        2 => {
            // there can be a case where a list and scalar both exist
            if dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Float64,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Int64,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Boolean,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))) {
                // we have a list and scalars, so we should get the values and coerce them
                let mut dt = dt;
                // sorting guarantees that the list will be the second value
                dt.sort();
                match (dt[0], dt[1]) {
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Float64 => {
                        if t1 == &DataType::Float64 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Float64,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Float64])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Int64 => {
                        if t1 == &DataType::Int64 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Int64,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Int64])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Boolean => {
                        if t1 == &DataType::Boolean {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Boolean,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Boolean])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Utf8 => {
                        if t1 == &DataType::Utf8 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Utf8,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Utf8])?,
                                true,
                            ))))
                        }
                    }
                    (t1, t2) => Err(ArrowError::JsonError(format!(
                        "Cannot coerce data types for {:?} and {:?}",
                        t1, t2
                    ))),
                }
            } else if dt.contains(&&DataType::Float64) && dt.contains(&&DataType::Int64) {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Utf8)
            }
        }
        _ => {
            // TODO(nevi_me) It's possible to have [float, int, list(float)], which should
            // return list(float). Will hash this out later
            Ok(DataType::List(Box::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))))
        }
    }
}

/// Generate schema from JSON field names and inferred data types
fn generate_schema(spec: HashMap<String, HashSet<DataType>>) -> Result<SchemaRef> {
    let fields: Result<Vec<Field>> = spec
        .iter()
        .map(|(k, hs)| {
            let v: Vec<&DataType> = hs.iter().collect();
            coerce_data_type(v).map(|t| Field::new(k, t, true))
        })
        .collect();
    match fields {
        Ok(fields) => {
            let schema = Schema::new(fields);
            Ok(Arc::new(schema))
        }
        Err(e) => Err(e),
    }
}

/// JSON file reader that produces a serde_json::Value iterator from a Read trait
///
/// # Example
///
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow::json::reader::ValueIter;
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// for value in value_reader {
///     println!("JSON value: {}", value.unwrap());
/// }
/// ```
#[derive(Debug)]
pub struct ValueIter<'a, R: Read> {
    reader: &'a mut BufReader<R>,
    max_read_records: Option<usize>,
    record_count: usize,
    // reuse line buffer to avoid allocation on each record
    line_buf: String,
}

impl<'a, R: Read> ValueIter<'a, R> {
    pub fn new(reader: &'a mut BufReader<R>, max_read_records: Option<usize>) -> Self {
        Self {
            reader,
            max_read_records,
            record_count: 0,
            line_buf: String::new(),
        }
    }
}

impl<'a, R: Read> Iterator for ValueIter<'a, R> {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(max) = self.max_read_records {
            if self.record_count >= max {
                return None;
            }
        }

        loop {
            self.line_buf.truncate(0);
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // read_line returns 0 when stream reached EOF
                    return None;
                }
                Err(e) => {
                    return Some(Err(ArrowError::JsonError(format!(
                        "Failed to read JSON record: {}",
                        e
                    ))));
                }
                _ => {
                    let trimmed_s = self.line_buf.trim();
                    if trimmed_s.is_empty() {
                        // ignore empty lines
                        continue;
                    }

                    self.record_count += 1;
                    return Some(serde_json::from_str(trimmed_s).map_err(|e| {
                        ArrowError::JsonError(format!("Not valid JSON: {}", e))
                    }));
                }
            }
        }
    }
}

/// Infer the fields of a JSON file by reading the first n records of the file, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// Contrary to [`infer_json_schema`], this function will seek back to the start of the `reader`.
/// That way, the `reader` can be used immediately afterwards to create a [`Reader`].
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow::json::reader::infer_json_schema_from_seekable;
///
/// let file = File::open("test/data/mixed_arrays.json").unwrap();
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(file);
/// let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();
/// // file's cursor's offset automatically set at 0
/// ```
pub fn infer_json_schema_from_seekable<R: Read + Seek>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<SchemaRef> {
    let schema = infer_json_schema(reader, max_read_records);
    // return the reader seek back to the start
    reader.seek(SeekFrom::Start(0))?;

    schema
}

/// Infer the fields of a JSON file by reading the first n records of the buffer, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// This function will not seek back to the start of the `reader`. The user has to manage the
/// original file's cursor. This function is useful when the `reader`'s cursor is not available
/// (does not implement [`Seek`]), such is the case for compressed streams decoders.
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::{BufReader, SeekFrom, Seek};
/// use flate2::read::GzDecoder;
/// use arrow::json::reader::infer_json_schema;
///
/// let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
///
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(GzDecoder::new(&file));
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// // cursor's offset at end of file
///
/// // seek back to start so that the original file is usable again
/// file.seek(SeekFrom::Start(0)).unwrap();
/// ```
pub fn infer_json_schema<R: Read>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<SchemaRef> {
    infer_json_schema_from_iterator(ValueIter::new(reader, max_read_records))
}

/// Infer the fields of a JSON file by reading all items from the JSON Value Iterator.
pub fn infer_json_schema_from_iterator<I>(value_iter: I) -> Result<SchemaRef>
where
    I: Iterator<Item = Result<Value>>,
{
    let mut values: HashMap<String, HashSet<DataType>> = HashMap::new();

    for record in value_iter {
        match record? {
            Value::Object(map) => {
                let res = map.iter().try_for_each(|(k, v)| {
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
                                        let dt = coerce_data_type(types)?;

                                        if values.contains_key(k) {
                                            let x = values.get_mut(k).unwrap();
                                            x.insert(DataType::List(Box::new(
                                                Field::new("item", dt, true),
                                            )));
                                        } else {
                                            // create hashset and add value type
                                            let mut hs = HashSet::new();
                                            hs.insert(DataType::List(Box::new(
                                                Field::new("item", dt, true),
                                            )));
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
                            "Inferring schema from nested JSON structs currently not supported"
                                .to_string(),
                        )),
                    }
                });
                match res {
                    Ok(()) => {}
                    Err(e) => return Err(e),
                }
            }
            value => {
                return Err(ArrowError::JsonError(format!(
                    "Expected JSON record to be an object, found {:?}",
                    value
                )));
            }
        };
    }

    generate_schema(values)
}

/// JSON values to Arrow record batch decoder. Decoder's next_batch method takes a JSON Value
/// iterator as input and outputs Arrow record batch.
///
/// # Examples
/// ```
/// use arrow::json::reader::{Decoder, ValueIter, infer_json_schema};
/// use std::fs::File;
/// use std::io::{BufReader, Seek, SeekFrom};
///
/// let mut reader =
///     BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// let batch_size = 1024;
/// let decoder = Decoder::new(inferred_schema, batch_size, None);
///
/// // seek back to start so that the original file is usable again
/// reader.seek(SeekFrom::Start(0)).unwrap();
/// let mut value_reader = ValueIter::new(&mut reader, None);
/// let batch = decoder.next_batch(&mut value_reader).unwrap().unwrap();
/// assert_eq!(4, batch.num_rows());
/// assert_eq!(4, batch.num_columns());
/// ```
#[derive(Debug)]
pub struct Decoder {
    /// Explicit schema for the JSON file
    schema: SchemaRef,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// Batch size (number of records to load each time)
    batch_size: usize,
}

impl Decoder {
    /// Create a new JSON decoder from any value that implements the `Iterator<Item=Result<Value>>`
    /// trait.
    pub fn new(
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            schema,
            projection,
            batch_size,
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> = fields
                    .iter()
                    .filter_map(|field| {
                        if projection.contains(field.name()) {
                            Some(field.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    /// Read the next batch of records
    pub fn next_batch<I>(&self, value_iter: &mut I) -> Result<Option<RecordBatch>>
    where
        I: Iterator<Item = Result<Value>>,
    {
        let mut rows: Vec<Value> = Vec::with_capacity(self.batch_size);

        for value in value_iter.by_ref().take(self.batch_size) {
            let v = value?;
            match v {
                Value::Object(_) => rows.push(v),
                _ => {
                    return Err(ArrowError::JsonError(format!(
                        "Row needs to be of type object, got: {:?}",
                        v
                    )));
                }
            }
        }
        if rows.is_empty() {
            // reached end of file
            return Ok(None);
        }

        let rows = &rows[..];
        let projection = self.projection.clone().unwrap_or_else(Vec::new);
        let arrays = self.build_struct_array(rows, self.schema.fields(), &projection);

        let projected_fields: Vec<Field> = if projection.is_empty() {
            self.schema.fields().to_vec()
        } else {
            projection
                .iter()
                .map(|name| self.schema.column_with_name(name))
                .filter_map(|c| c)
                .map(|(_, field)| field.clone())
                .collect()
        };

        let projected_schema = Arc::new(Schema::new(projected_fields));

        arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr).map(Some))
    }

    fn build_wrapped_list_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
    ) -> Result<ArrayRef> {
        match *key_type {
            DataType::Int8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int8Type>(&dtype, col_name, rows)
            }
            DataType::Int16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int16Type>(&dtype, col_name, rows)
            }
            DataType::Int32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int32Type>(&dtype, col_name, rows)
            }
            DataType::Int64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int64Type>(&dtype, col_name, rows)
            }
            DataType::UInt8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt8Type>(&dtype, col_name, rows)
            }
            DataType::UInt16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt16Type>(&dtype, col_name, rows)
            }
            DataType::UInt32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt32Type>(&dtype, col_name, rows)
            }
            DataType::UInt64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt64Type>(&dtype, col_name, rows)
            }
            ref e => Err(ArrowError::JsonError(format!(
                "Data type is currently not supported for dictionaries in list : {:?}",
                e
            ))),
        }
    }

    #[inline(always)]
    fn list_array_string_array_builder<DICT_TY>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: &[Value],
    ) -> Result<ArrayRef>
    where
        DICT_TY: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder = StringBuilder::new(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder =
                    self.build_string_dictionary_builder::<DICT_TY>(rows.len() * 5)?;
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list data builder type is not supported: {:?}",
                    e
                )))
            }
        };

        for row in rows {
            if let Some(value) = row.get(col_name) {
                // value can be an array or a scalar
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(|v: &Value| {
                            if v.is_string() {
                                Some(v.as_str().unwrap().to_string())
                            } else if v.is_array() || v.is_object() || v.is_null() {
                                // implicitly drop nested values
                                // TODO support deep-nesting
                                None
                            } else {
                                Some(v.to_string())
                            }
                        })
                        .collect()
                } else if let Value::Null = value {
                    vec![None]
                } else if !value.is_object() {
                    vec![Some(value.to_string())]
                } else {
                    return Err(ArrowError::JsonError(
                        "Only scalars are currently supported in JSON arrays".to_string(),
                    ));
                };

                // TODO: ARROW-10335: APIs of dictionary arrays and others are different. Unify
                // them.
                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||ArrowError::JsonError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v)?
                            } else {
                                builder.values().append_null()?
                            };
                        }

                        // Append to the list
                        builder.append(true)?;
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<DICT_TY>>>().ok_or_else(||ArrowError::JsonError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                let _ = builder.values().append(&v)?;
                            } else {
                                builder.values().append_null()?
                            };
                        }

                        // Append to the list
                        builder.append(true)?;
                    }
                    e => {
                        return Err(ArrowError::JsonError(format!(
                            "Nested list data builder type is not supported: {:?}",
                            e
                        )))
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[inline(always)]
    fn build_string_dictionary_builder<T>(
        &self,
        row_len: usize,
    ) -> Result<StringDictionaryBuilder<T>>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let key_builder = PrimitiveBuilder::<T>::new(row_len);
        let values_builder = StringBuilder::new(row_len * 5);
        Ok(StringDictionaryBuilder::new(key_builder, values_builder))
    }

    #[inline(always)]
    fn build_string_dictionary_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<ArrayRef> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => {
                    self.build_dictionary_array::<Int16Type>(rows, col_name)
                }
                DataType::Int32 => {
                    self.build_dictionary_array::<Int32Type>(rows, col_name)
                }
                DataType::Int64 => {
                    self.build_dictionary_array::<Int64Type>(rows, col_name)
                }
                DataType::UInt8 => {
                    self.build_dictionary_array::<UInt8Type>(rows, col_name)
                }
                DataType::UInt16 => {
                    self.build_dictionary_array::<UInt16Type>(rows, col_name)
                }
                DataType::UInt32 => {
                    self.build_dictionary_array::<UInt32Type>(rows, col_name)
                }
                DataType::UInt64 => {
                    self.build_dictionary_array::<UInt64Type>(rows, col_name)
                }
                _ => Err(ArrowError::JsonError(
                    "unsupported dictionary key type".to_string(),
                )),
            }
        } else {
            Err(ArrowError::JsonError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    fn build_boolean_array(&self, rows: &[Value], col_name: &str) -> Result<ArrayRef> {
        let mut builder = BooleanBuilder::new(rows.len());
        for row in rows {
            if let Some(value) = row.get(&col_name) {
                if let Some(boolean) = value.as_bool() {
                    builder.append_value(boolean)?
                } else {
                    builder.append_null()?;
                }
            } else {
                builder.append_null()?;
            }
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
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    row.get(&col_name)
                        .and_then(|value| value.as_f64())
                        .and_then(num::cast::cast)
                })
                .collect::<PrimitiveArray<T>>(),
        ))
    }

    /// Build a nested GenericListArray from a list of unnested `Value`s
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        rows: &[Value],
        list_field: &Field,
    ) -> Result<ArrayRef> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls =
            MutableBuffer::new(num_list_bytes).with_bitset(num_list_bytes, false);
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            if let Value::Array(a) = v {
                cur_offset = cur_offset + OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(list_nulls.data_mut(), i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset = cur_offset + OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).data(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                let mut bool_nulls =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                rows.iter().for_each(|v| {
                    if let Value::Array(vs) = v {
                        vs.iter().for_each(|value| {
                            if let Value::Bool(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(bool_values.data_mut(), curr_index);
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(bool_nulls.data_mut(), curr_index);
                            }
                            curr_index += 1;
                        });
                    }
                });
                ArrayData::builder(list_field.data_type().clone())
                    .len(valid_len)
                    .add_buffer(bool_values.freeze())
                    .null_bit_buffer(bool_nulls.freeze())
                    .build()
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => {
                return Err(ArrowError::JsonError("Float16 not supported".to_string()))
            }
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(ArrowError::JsonError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ))
            }
            DataType::Utf8 => {
                StringArray::from_iter(flatten_json_string_values(rows).into_iter())
                    .data()
            }
            DataType::LargeUtf8 => {
                LargeStringArray::from_iter(flatten_json_string_values(rows).into_iter())
                    .data()
            }
            DataType::List(field) => {
                let child = self
                    .build_nested_list_array::<i32>(&flatten_json_values(rows), field)?;
                child.data()
            }
            DataType::LargeList(field) => {
                let child = self
                    .build_nested_list_array::<i64>(&flatten_json_values(rows), field)?;
                child.data()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let len = rows.len();
                let num_bytes = bit_util::ceil(len, 8);
                let mut null_buffer =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                let mut struct_index = 0;
                let rows: Vec<Value> = rows
                    .iter()
                    .flat_map(|row| {
                        if let Value::Array(values) = row {
                            values.iter().for_each(|_| {
                                bit_util::set_bit(null_buffer.data_mut(), struct_index);
                                struct_index += 1;
                            });
                            values.clone()
                        } else {
                            struct_index += 1;
                            vec![Value::Null]
                        }
                    })
                    .collect();
                let arrays =
                    self.build_struct_array(rows.as_slice(), fields.as_slice(), &[])?;
                let data_type = DataType::Struct(fields.clone());
                let buf = null_buffer.freeze();
                ArrayDataBuilder::new(data_type)
                    .len(rows.len())
                    .null_bit_buffer(buf)
                    .child_data(arrays.into_iter().map(|a| a.data()).collect())
                    .build()
            }
            datatype => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list of {:?} not supported",
                    datatype
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(Box::new(list_field.clone())))
            .len(list_len)
            .add_buffer(Buffer::from(offsets.to_byte_slice()))
            .add_child_data(array_data)
            .null_bit_buffer(list_nulls.freeze())
            .build();
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    /// Builds the child values of a `StructArray`, falling short of constructing the StructArray.
    /// The function does not construct the StructArray as some callers would want the child arrays.
    ///
    /// *Note*: The function is recursive, and will read nested structs.
    ///
    /// If `projection` is not empty, then all values are returned. The first level of projection
    /// occurs at the `RecordBatch` level. No further projection currently occurs, but would be
    /// useful if plucking values from a struct, e.g. getting `a.b.c.e` from `a.b.c.{d, e}`.
    fn build_struct_array(
        &self,
        rows: &[Value],
        struct_fields: &[Field],
        projection: &[String],
    ) -> Result<Vec<ArrayRef>> {
        let arrays: Result<Vec<ArrayRef>> = struct_fields
            .iter()
            .filter(|field| projection.is_empty() || projection.contains(field.name()))
            .map(|field| {
                match field.data_type() {
                    DataType::Null => {
                        Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef)
                    }
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, field.name())
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, field.name())
                    }
                    DataType::Int64 => {
                        self.build_primitive_array::<Int64Type>(rows, field.name())
                    }
                    DataType::Int32 => {
                        self.build_primitive_array::<Int32Type>(rows, field.name())
                    }
                    DataType::Int16 => {
                        self.build_primitive_array::<Int16Type>(rows, field.name())
                    }
                    DataType::Int8 => {
                        self.build_primitive_array::<Int8Type>(rows, field.name())
                    }
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, field.name())
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, field.name())
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, field.name())
                    }
                    DataType::UInt8 => {
                        self.build_primitive_array::<UInt8Type>(rows, field.name())
                    }
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<TimestampSecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<TimestampNanosecondType>(
                                rows,
                                field.name(),
                            ),
                    },
                    DataType::Date64(_) => {
                        self.build_primitive_array::<Date64Type>(rows, field.name())
                    }
                    DataType::Date32(_) => {
                        self.build_primitive_array::<Date32Type>(rows, field.name())
                    }
                    DataType::Time64(unit) => match unit {
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<Time64MicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<Time64NanosecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::JsonError(format!(
                            "TimeUnit {:?} not supported with Time64",
                            t
                        ))),
                    },
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<Time32SecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<Time32MillisecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::JsonError(format!(
                            "TimeUnit {:?} not supported with Time32",
                            t
                        ))),
                    },
                    DataType::Utf8 => {
                        let mut builder = StringBuilder::new(rows.len());
                        for row in rows {
                            if let Some(value) = row.get(field.name()) {
                                if let Some(str_v) = value.as_str() {
                                    builder.append_value(str_v)?
                                } else {
                                    builder.append(false)?
                                }
                            } else {
                                builder.append(false)?
                            }
                        }
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    DataType::List(ref list_field) => {
                        match list_field.data_type() {
                            DataType::Dictionary(ref key_ty, _) => {
                                self.build_wrapped_list_array(rows, field.name(), key_ty)
                            }
                            _ => {
                                // extract rows by name
                                let extracted_rows = rows
                                    .iter()
                                    .map(|row| {
                                        row.get(field.name())
                                            .cloned()
                                            .unwrap_or(Value::Null)
                                    })
                                    .collect::<Vec<Value>>();
                                self.build_nested_list_array::<i32>(
                                    extracted_rows.as_slice(),
                                    list_field,
                                )
                            }
                        }
                    }
                    DataType::Dictionary(ref key_ty, ref val_ty) => self
                        .build_string_dictionary_array(
                            rows,
                            field.name(),
                            key_ty,
                            val_ty,
                        ),
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer =
                            MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| {
                                (
                                    i,
                                    row.as_object()
                                        .map(|v| v.get(field.name()))
                                        .flatten(),
                                )
                            })
                            .map(|(i, v)| match v {
                                // we want the field as an object, if it's not, we treat as null
                                Some(Value::Object(value)) => {
                                    bit_util::set_bit(null_buffer.data_mut(), i);
                                    Value::Object(value.clone())
                                }
                                _ => Value::Object(Default::default()),
                            })
                            .collect::<Vec<Value>>();
                        let arrays =
                            self.build_struct_array(&struct_rows, fields, &[])?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(null_buffer.freeze())
                            .child_data(arrays.into_iter().map(|a| a.data()).collect())
                            .build();
                        Ok(make_array(data))
                    }
                    _ => Err(ArrowError::JsonError(format!(
                        "{:?} type is not supported",
                        field.data_type()
                    ))),
                }
            })
            .collect();
        arrays
    }

    #[inline(always)]
    fn build_dictionary_array<T>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef>
    where
        T::Native: num::NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len())?;
        for row in rows {
            if let Some(value) = row.get(&col_name) {
                if let Some(str_v) = value.as_str() {
                    builder.append(str_v).map(drop)?
                } else {
                    builder.append_null()?
                }
            } else {
                builder.append_null()?
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    /// Read the primitive list's values into ArrayData
    fn read_primitive_list_values<T>(&self, rows: &[Value]) -> ArrayDataRef
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: num::NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                // read values from list
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(|value| {
                            let v: Option<T::Native> =
                                value.as_f64().and_then(num::cast::cast);
                            v
                        })
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Value::Number(value) = row {
                    // handle the scalar number case
                    let v: Option<T::Native> = value.as_f64().and_then(num::cast::cast);
                    v.map(|v| vec![Some(v)]).unwrap_or_default()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = PrimitiveArray::<T>::from_iter(values.iter());
        array.data()
    }
}

/// Reads a JSON value as a string, regardless of its type.
/// This is useful if the expected datatype is a string, in which case we preserve
/// all the values regardless of they type.
///
/// Applying `value.to_string()` unfortunately results in an escaped string, which
/// is not what we want.
#[inline(always)]
fn json_value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(string) => Some(string.clone()),
        _ => Some(value.to_string()),
    }
}

/// Flattens a list of JSON values, by flattening lists, and treating all other values as
/// single-value lists.
/// This is used to read into nested lists (list of list, list of struct) and non-dictionary lists.
#[inline]
fn flatten_json_values(values: &[Value]) -> Vec<Value> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values.clone()
            } else if let Value::Null = row {
                vec![Value::Null]
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![row.clone()]
            }
        })
        .collect()
}

/// Flattens a list into string values, dropping Value::Null in the process.
/// This is useful for interpreting any JSON array as string, dropping nulls.
/// See `json_value_as_string`.
#[inline]
fn flatten_json_string_values(values: &[Value]) -> Vec<Option<String>> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values
                    .iter()
                    .map(json_value_as_string)
                    .collect::<Vec<Option<_>>>()
            } else if let Value::Null = row {
                vec![]
            } else {
                vec![json_value_as_string(row)]
            }
        })
        .collect::<Vec<Option<_>>>()
}
/// JSON file reader
#[derive(Debug)]
pub struct Reader<R: Read> {
    reader: BufReader<R>,
    /// JSON value decoder
    decoder: Decoder,
}

impl<R: Read> Reader<R> {
    /// Create a new JSON Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn new(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self::from_buf_reader(BufReader::new(reader), schema, batch_size, projection)
    }

    /// Create a new JSON Reader from a `BufReader<R: Read>`
    ///
    /// To customize the schema, such as to enable schema inference, use `ReaderBuilder`
    pub fn from_buf_reader(
        reader: BufReader<R>,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Self {
        Self {
            reader,
            decoder: Decoder::new(schema, batch_size, projection),
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.decoder
            .next_batch(&mut ValueIter::new(&mut self.reader, None))
    }
}

/// JSON file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the JSON file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the JSON structure.
    schema: Option<SchemaRef>,
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
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
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
    pub fn build<R>(self, source: R) -> Result<Reader<R>>
    where
        R: Read + Seek,
    {
        let mut buf_reader = BufReader::new(source);

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => infer_json_schema_from_seekable(&mut buf_reader, self.max_records)?,
        };

        Ok(Reader::from_buf_reader(
            buf_reader,
            schema,
            self.batch_size,
            self.projection,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        buffer::Buffer,
        datatypes::DataType::{Dictionary, List},
    };

    use super::*;
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::Cursor;

    #[test]
    fn test_json_basic() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(4, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int64, a.1.data_type());
        let b = schema.column_with_name("b").unwrap();
        assert_eq!(1, b.0);
        assert_eq!(&DataType::Float64, b.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(2, c.0);
        assert_eq!(&DataType::Boolean, c.1.data_type());
        let d = schema.column_with_name("d").unwrap();
        assert_eq!(3, d.0);
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
        assert!(2.0 - bb.value(0) < f64::EPSILON);
        assert!(-3.5 - bb.value(1) < f64::EPSILON);
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
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("4", dd.value(0));
        assert_eq!("text", dd.value(8));
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

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

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
            .downcast_ref::<StringArray>()
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
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema.clone()),
            1024,
            None,
        );
        let reader_schema = reader.schema();
        assert_eq!(reader_schema, Arc::new(schema));
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
        assert!(2.0 - bb.value(0) < f32::EPSILON);
        assert!(-3.5 - bb.value(1) < f32::EPSILON);
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
            File::open("test/data/basic.json").unwrap(),
            Arc::new(schema),
            1024,
            Some(vec!["a".to_string(), "c".to_string()]),
        );
        let reader_schema = reader.schema();
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Boolean, false),
        ]));
        assert_eq!(reader_schema, expected_schema);

        let batch = reader.next().unwrap().unwrap();

        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.schema().fields().len());
        assert_eq!(12, batch.num_rows());

        let schema = batch.schema();
        assert_eq!(reader_schema, schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(0, a.0);
        assert_eq!(&DataType::Int32, a.1.data_type());
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(1, c.0);
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
            &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            b.1.data_type()
        );
        let c = schema.column_with_name("c").unwrap();
        assert_eq!(
            &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
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
        assert!(2.0 - bb.value(0) < f64::EPSILON);
        assert!(-6.1 - bb.value(5) < f64::EPSILON);
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
    fn test_invalid_json_infer_schema() {
        let re = infer_json_schema_from_seekable(
            &mut BufReader::new(
                File::open("test/data/uk_cities_with_headers.csv").unwrap(),
            ),
            None,
        );
        assert_eq!(
            re.err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_invalid_json_read_record() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new("a", DataType::Utf8, true)]),
            true,
        )]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/uk_cities_with_headers.csv").unwrap())
            .unwrap();
        assert_eq!(
            reader.next().err().unwrap().to_string(),
            "Json error: Not valid JSON: expected value at line 1 column 1",
        );
    }

    #[test]
    fn test_coersion_scalar_and_list() {
        use crate::datatypes::DataType::*;

        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
            .unwrap()
        );
        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
            .unwrap()
        );
        assert_eq!(
            List(Box::new(Field::new("item", Int64, true))),
            coerce_data_type(vec![
                &Int64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
            .unwrap()
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            List(Box::new(Field::new("item", Utf8, true))),
            coerce_data_type(vec![
                &Boolean,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
            .unwrap()
        );
    }

    #[test]
    fn test_mixed_json_arrays() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/mixed_arrays.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let schema = infer_json_schema(&mut reader, None).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let reader = BufReader::new(GzDecoder::new(&file));
        let mut reader = Reader::from_buf_reader(reader, schema, 64, None);
        let batch_gz = reader.next().unwrap().unwrap();

        for batch in vec![batch, batch_gz] {
            assert_eq!(4, batch.num_columns());
            assert_eq!(4, batch.num_rows());

            let schema = batch.schema();

            let a = schema.column_with_name("a").unwrap();
            assert_eq!(&DataType::Int64, a.1.data_type());
            let b = schema.column_with_name("b").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                b.1.data_type()
            );
            let c = schema.column_with_name("c").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                c.1.data_type()
            );
            let d = schema.column_with_name("d").unwrap();
            assert_eq!(
                &DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                d.1.data_type()
            );

            let bb = batch
                .column(b.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let bb = bb.values();
            let bb = bb.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(10, bb.len());
            assert!(4.0 - bb.value(9) < f64::EPSILON);

            let cc = batch
                .column(c.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            // test that the list offsets are correct
            assert_eq!(
                cc.data().buffers()[0],
                Buffer::from(vec![0i32, 2, 2, 4, 5].to_byte_slice())
            );
            let cc = cc.values();
            let cc = cc.as_any().downcast_ref::<BooleanArray>().unwrap();
            let cc_expected = BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(false),
            ]);
            assert_eq!(cc.data_ref(), cc_expected.data_ref());

            let dd: &ListArray = batch
                .column(d.0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            // test that the list offsets are correct
            assert_eq!(
                dd.data().buffers()[0],
                Buffer::from(vec![0i32, 1, 1, 2, 6].to_byte_slice())
            );
            let dd = dd.values();
            let dd = dd.as_any().downcast_ref::<StringArray>().unwrap();
            // values are 6 because a `d: null` is treated as a null slot
            // and a list's null slot can be omitted from the child (i.e. same offset)
            assert_eq!(6, dd.len());
            assert_eq!("text", dd.value(1));
            assert_eq!("1", dd.value(2));
            assert_eq!("false", dd.value(3));
            assert_eq!("array", dd.value(4));
            assert_eq!("2.4", dd.value(5));
        }
    }

    #[test]
    fn test_nested_struct_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/nested_structs.json").unwrap())
            .unwrap();

        // build expected output
        let d = StringArray::from(vec![Some("text"), None, Some("text"), None]);
        let c = ArrayDataBuilder::new(c_field.data_type().clone())
            .null_count(2)
            .len(4)
            .add_child_data(d.data())
            .null_bit_buffer(Buffer::from(vec![0b00000101]))
            .build();
        let b = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let a = ArrayDataBuilder::new(a_field.data_type().clone())
            .null_count(1)
            .len(4)
            .add_child_data(b.data())
            .add_child_data(c)
            .null_bit_buffer(Buffer::from(vec![0b00000111]))
            .build();
        let expected = make_array(a);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert!(
            expected.data_ref() == read.data_ref(),
            format!("{:?} != {:?}", expected.data(), read.data())
        );
    }

    #[test]
    fn test_nested_list_json_arrays() {
        let c_field = Field::new(
            "c",
            DataType::Struct(vec![Field::new("d", DataType::Utf8, true)]),
            true,
        );
        let a_struct_field = Field::new(
            "a",
            DataType::Struct(vec![
                Field::new("b", DataType::Boolean, true),
                c_field.clone(),
            ]),
            true,
        );
        let a_field =
            Field::new("a", DataType::List(Box::new(a_struct_field.clone())), true);
        let schema = Arc::new(Schema::new(vec![a_field.clone()]));
        let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
        let json_content = r#"
        {"a": [{"b": true, "c": {"d": "a_text"}}, {"b": false, "c": {"d": "b_text"}}]}
        {"a": [{"b": false, "c": null}]}
        {"a": [{"b": true, "c": {"d": "c_text"}}, {"b": null, "c": {"d": "d_text"}}, {"b": true, "c": {"d": null}}]}
        {"a": null}
        {"a": []}
        "#;
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();

        // build expected output
        let d = StringArray::from(vec![
            Some("a_text"),
            Some("b_text"),
            None,
            Some("c_text"),
            Some("d_text"),
            None,
            None,
        ]);
        let c = ArrayDataBuilder::new(c_field.data_type().clone())
            .null_count(2)
            .len(7)
            .add_child_data(d.data())
            .null_bit_buffer(Buffer::from(vec![0b00111011]))
            .build();
        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
            Some(true),
            None,
        ]);
        let a = ArrayDataBuilder::new(a_struct_field.data_type().clone())
            .len(7)
            .add_child_data(b.data())
            .add_child_data(c.clone())
            .null_bit_buffer(Buffer::from(vec![0b00111111]))
            .build();
        let a_list = ArrayDataBuilder::new(a_field.data_type().clone())
            .null_count(1)
            .len(5)
            .add_buffer(Buffer::from(vec![0i32, 2, 3, 6, 6, 6].to_byte_slice()))
            .add_child_data(a)
            .null_bit_buffer(Buffer::from(vec![0b00010111]))
            .build();
        let expected = make_array(a_list);

        // compare `a` with result from json reader
        let batch = reader.next().unwrap().unwrap();
        let read = batch.column(0);
        assert_eq!(read.len(), 5);
        // compare the arrays the long way around, to better detect differences
        let read: &ListArray = read.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = expected.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            read.data().buffers()[0],
            Buffer::from(vec![0i32, 2, 3, 6, 6, 6].to_byte_slice())
        );
        // compare list null buffers
        assert_eq!(read.data().null_buffer(), expected.data().null_buffer());
        // build struct from list
        let struct_values = read.values();
        let struct_array: &StructArray = struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let expected_struct_values = expected.values();
        let expected_struct_array = expected_struct_values
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_eq!(7, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert_eq!(7, expected_struct_array.len());
        assert_eq!(1, expected_struct_array.null_count());
        // test struct's nulls
        assert_eq!(
            struct_array.data().null_buffer(),
            expected_struct_array.data().null_buffer()
        );
        // test struct's fields
        let read_b = struct_array.column(0);
        assert_eq!(b.data_ref(), read_b.data_ref());
        let read_c = struct_array.column(1);
        assert_eq!(&c, read_c.data_ref());
        let read_c: &StructArray = read_c.as_any().downcast_ref::<StructArray>().unwrap();
        let read_d = read_c.column(0);
        assert_eq!(d.data_ref(), read_d.data_ref());

        assert_eq!(read.data_ref(), expected.data_ref());
    }

    #[test]
    fn test_dictionary_from_json_basic_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            d.1.data_type()
        );

        let dd = batch
            .column(d.0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .unwrap();
        assert_eq!(false, dd.is_valid(0));
        assert_eq!(true, dd.is_valid(1));
        assert_eq!(true, dd.is_valid(2));
        assert_eq!(false, dd.is_valid(11));

        assert_eq!(
            dd.keys(),
            &Int16Array::from(vec![
                None,
                Some(0),
                Some(1),
                Some(0),
                None,
                None,
                Some(0),
                None,
                Some(1),
                Some(0),
                Some(0),
                None
            ])
        );
    }

    #[test]
    fn test_dictionary_from_json_int8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_int64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_skip_empty_lines() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let json_content = "
        {\"a\": 1}

        {\"a\": 2}

        {\"a\": 3}";
        let mut reader = builder.build(Cursor::new(json_content)).unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let c = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Int64, c.1.data_type());
    }

    #[test]
    fn test_row_type_validation() {
        let builder = ReaderBuilder::new().infer_schema(None).with_batch_size(64);
        let json_content = "
        [1, \"hello\"]
        \"world\"";
        let re = builder.build(Cursor::new(json_content));
        assert_eq!(
            re.err().unwrap().to_string(),
            r#"Json error: Expected JSON record to be an object, found Array([Number(1), String("hello")])"#,
        );
    }

    #[test]
    fn test_list_of_string_dictionary_from_json() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/list_string_dict_nested.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(6, evs_list.len());
        assert_eq!(true, evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_list_of_string_dictionary_from_json_with_nulls() {
        let schema = Schema::new(vec![Field::new(
            "events",
            List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true,
            ))),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(
                File::open("test/data/list_string_dict_nested_nulls.json").unwrap(),
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let events = schema.column_with_name("events").unwrap();
        assert_eq!(
            &List(Box::new(Field::new(
                "item",
                Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                true
            ))),
            events.1.data_type()
        );

        let evs_list = batch
            .column(events.0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let evs_list = evs_list.values();
        let evs_list = evs_list
            .as_any()
            .downcast_ref::<DictionaryArray<UInt64Type>>()
            .unwrap();
        assert_eq!(8, evs_list.len());
        assert_eq!(true, evs_list.is_valid(1));
        assert_eq!(DataType::Utf8, evs_list.value_type());

        // dict from the events list
        let dict_el = evs_list.values();
        let dict_el = dict_el.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(2, evs_list.null_count());
        assert_eq!(3, dict_el.len());
        assert_eq!("Elect Leader", dict_el.value(0));
        assert_eq!("Do Ballot", dict_el.value(1));
        assert_eq!("Send Data", dict_el.value(2));
    }

    #[test]
    fn test_dictionary_from_json_uint8() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint32() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_dictionary_from_json_uint64() {
        let schema = Schema::new(vec![Field::new(
            "d",
            Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            true,
        )]);
        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let d = schema.column_with_name("d").unwrap();
        assert_eq!(
            &Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            d.1.data_type()
        );
    }

    #[test]
    fn test_with_multiple_batches() {
        let builder = ReaderBuilder::new()
            .infer_schema(Some(4))
            .with_batch_size(5);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();

        let mut num_records = Vec::new();
        while let Some(rb) = reader.next().unwrap() {
            num_records.push(rb.num_rows());
        }

        assert_eq!(vec![5, 5, 2], num_records);
    }

    #[test]
    fn test_json_infer_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "c",
                DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "d",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        let mut reader =
            BufReader::new(File::open("test/data/mixed_arrays.json").unwrap());
        let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, Arc::new(schema.clone()));

        let file = File::open("test/data/mixed_arrays.json.gz").unwrap();
        let mut reader = BufReader::new(GzDecoder::new(&file));
        let inferred_schema = infer_json_schema(&mut reader, None).unwrap();

        assert_eq!(inferred_schema, Arc::new(schema));
    }

    #[test]
    fn test_timestamp_from_json_seconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Second, None),
            a.1.data_type()
        );

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_timestamp_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            a.1.data_type()
        );

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_date_from_json_milliseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Date64(DateUnit::Millisecond),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Date64(DateUnit::Millisecond), a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }

    #[test]
    fn test_time_from_json_nanoseconds() {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .with_batch_size(64);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open("test/data/basic_nulls.json").unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(12, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let a = schema.column_with_name("a").unwrap();
        assert_eq!(&DataType::Time64(TimeUnit::Nanosecond), a.1.data_type());

        let aa = batch
            .column(a.0)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();
        assert_eq!(true, aa.is_valid(0));
        assert_eq!(false, aa.is_valid(1));
        assert_eq!(false, aa.is_valid(2));
        assert_eq!(1, aa.value(0));
        assert_eq!(1, aa.value(3));
        assert_eq!(5, aa.value(7));
    }
}
