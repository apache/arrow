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

//! CSV Reader
//!
//! This CSV reader allows CSV files to be read into the Arrow memory model. Records are
//! loaded in batches and are then converted from row-based data to columnar data.
//!
//! Example:
//!
//! ```
//! use arrow::csv;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use std::fs::File;
//! use std::sync::Arc;
//!
//! let schema = Schema::new(vec![
//!     Field::new("city", DataType::Utf8, false),
//!     Field::new("lat", DataType::Float64, false),
//!     Field::new("lng", DataType::Float64, false),
//! ]);
//!
//! let file = File::open("test/data/uk_cities.csv").unwrap();
//!
//! let mut csv = csv::Reader::new(file, Arc::new(schema), false, None, 1024, None, None);
//! let batch = csv.next().unwrap().unwrap();
//! ```

use core::cmp::min;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use csv as csv_crate;

use crate::array::{ArrayRef, BooleanArray, PrimitiveArray, StringBuilder};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use self::csv_crate::{ByteRecord, StringRecord};

lazy_static! {
    static ref DECIMAL_RE: Regex = Regex::new(r"^-?(\d+\.\d+)$").unwrap();
    static ref INTEGER_RE: Regex = Regex::new(r"^-?(\d+)$").unwrap();
    static ref BOOLEAN_RE: Regex = RegexBuilder::new(r"^(true)$|^(false)$")
        .case_insensitive(true)
        .build()
        .unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^\d{4}-\d\d-\d\d$").unwrap();
    static ref DATETIME_RE: Regex =
        Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$").unwrap();
}

/// Infer the data type of a record
fn infer_field_schema(string: &str) -> DataType {
    // when quoting is enabled in the reader, these quotes aren't escaped, we default to
    // Utf8 for them
    if string.starts_with('"') {
        return DataType::Utf8;
    }
    // match regex in a particular order
    if BOOLEAN_RE.is_match(string) {
        DataType::Boolean
    } else if DECIMAL_RE.is_match(string) {
        DataType::Float64
    } else if INTEGER_RE.is_match(string) {
        DataType::Int64
    } else if DATETIME_RE.is_match(string) {
        DataType::Date64(DateUnit::Millisecond)
    } else if DATE_RE.is_match(string) {
        DataType::Date32(DateUnit::Day)
    } else {
        DataType::Utf8
    }
}

/// Infer the schema of a CSV file by reading through the first n records of the file,
/// with `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its schema.
///
/// Return infered schema and number of records used for inference.
fn infer_file_schema<R: Read + Seek>(
    reader: &mut R,
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<(Schema, usize)> {
    let mut csv_reader = csv_crate::ReaderBuilder::new()
        .delimiter(delimiter)
        .from_reader(reader);

    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let headers: Vec<String> = if has_header {
        let headers = &csv_reader.headers()?.clone();
        headers.iter().map(|s| s.to_string()).collect()
    } else {
        let first_record_count = &csv_reader.headers()?.len();
        (0..*first_record_count)
            .map(|i| format!("column_{}", i + 1))
            .collect()
    };

    // save the csv reader position after reading headers
    let position = csv_reader.position().clone();

    let header_length = headers.len();
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<DataType>> = vec![HashSet::new(); header_length];
    // keep track of columns with nulls
    let mut nulls: Vec<bool> = vec![false; header_length];

    // return csv reader position to after headers
    csv_reader.seek(position)?;

    let mut records_count = 0;
    let mut fields = vec![];

    let mut record = StringRecord::new();
    let max_records = max_read_records.unwrap_or(usize::MAX);
    while records_count < max_records {
        if !csv_reader.read_record(&mut record)? {
            break;
        }
        records_count += 1;

        for i in 0..header_length {
            if let Some(string) = record.get(i) {
                if string.is_empty() {
                    nulls[i] = true;
                } else {
                    column_types[i].insert(infer_field_schema(string));
                }
            }
        }
    }

    // build schema from inference results
    for i in 0..header_length {
        let possibilities = &column_types[i];
        let has_nulls = nulls[i];
        let field_name = &headers[i];

        // determine data type based on possible types
        // if there are incompatible types, use DataType::Utf8
        match possibilities.len() {
            1 => {
                for dtype in possibilities.iter() {
                    fields.push(Field::new(&field_name, dtype.clone(), has_nulls));
                }
            }
            2 => {
                if possibilities.contains(&DataType::Int64)
                    && possibilities.contains(&DataType::Float64)
                {
                    // we have an integer and double, fall down to double
                    fields.push(Field::new(&field_name, DataType::Float64, has_nulls));
                } else {
                    // default to Utf8 for conflicting datatypes (e.g bool and int)
                    fields.push(Field::new(&field_name, DataType::Utf8, has_nulls));
                }
            }
            _ => fields.push(Field::new(&field_name, DataType::Utf8, has_nulls)),
        }
    }

    // return the reader seek back to the start
    csv_reader.into_inner().seek(SeekFrom::Start(0))?;

    Ok((Schema::new(fields), records_count))
}

/// Infer schema from a list of CSV files by reading through first n records
/// with `max_read_records` controlling the maximum number of records to read.
///
/// Files will be read in the given order untill n records have been reached.
///
/// If `max_read_records` is not set, all files will be read fully to infer the schema.
pub fn infer_schema_from_files(
    files: &[String],
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<Schema> {
    let mut schemas = vec![];
    let mut records_to_read = max_read_records.unwrap_or(std::usize::MAX);

    for fname in files.iter() {
        let (schema, records_read) = infer_file_schema(
            &mut File::open(fname)?,
            delimiter,
            Some(records_to_read),
            has_header,
        )?;
        if records_read == 0 {
            continue;
        }
        schemas.push(schema.clone());
        records_to_read -= records_read;
        if records_to_read == 0 {
            break;
        }
    }

    Schema::try_merge(&schemas)
}

// optional bounds of the reader, of the form (min line, max line).
type Bounds = Option<(usize, usize)>;

/// CSV file reader
pub struct Reader<R: Read> {
    /// Explicit schema for the CSV file
    schema: SchemaRef,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// File reader
    reader: csv_crate::Reader<R>,
    /// Current line number
    line_number: usize,
    /// Maximum number of rows to read
    end: usize,
    /// Number of records per batch
    batch_size: usize,
    /// Vector that can hold the `StringRecord`s of the batches
    batch_records: Vec<StringRecord>,
}

impl<R> fmt::Debug for Reader<R>
where
    R: Read,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("line_number", &self.line_number)
            .finish()
    }
}

impl<R: Read> Reader<R> {
    /// Create a new CsvReader from any value that implements the `Read` trait.
    ///
    /// If reading a `File` or an input that supports `std::io::Read` and `std::io::Seek`;
    /// you can customise the Reader, such as to enable schema inference, use
    /// `ReaderBuilder`.
    pub fn new(
        reader: R,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self::from_reader(
            reader, schema, has_header, delimiter, batch_size, bounds, projection,
        )
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    /// Create a new CsvReader from a Reader
    ///
    /// This constructor allows you more flexibility in what records are processed by the
    /// csv reader.
    pub fn from_reader(
        reader: R,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        batch_size: usize,
        bounds: Bounds,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let mut reader_builder = csv_crate::ReaderBuilder::new();
        reader_builder.has_headers(has_header);

        if let Some(c) = delimiter {
            reader_builder.delimiter(c);
        }

        let mut csv_reader = reader_builder.from_reader(reader);

        let (start, end) = match bounds {
            None => (0, usize::MAX),
            Some((start, end)) => (start, end),
        };

        // First we will skip `start` rows
        // note that this skips by iteration. This is because in general it is not possible
        // to seek in CSV. However, skiping still saves the burden of creating arrow arrays,
        // which is a slow operation that scales with the number of columns

        let mut record = ByteRecord::new();
        // Skip first start items
        for _ in 0..start {
            let res = csv_reader.read_byte_record(&mut record);
            if !res.unwrap_or(false) {
                break;
            }
        }

        // Initialize batch_records with StringRecords so they
        // can be reused accross batches
        let mut batch_records = Vec::with_capacity(batch_size);
        batch_records.resize_with(batch_size, Default::default);

        Self {
            schema,
            projection,
            reader: csv_reader,
            line_number: if has_header { start + 1 } else { start },
            batch_size,
            end,
            batch_records,
        }
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.end - self.line_number;

        let mut read_records = 0;
        for i in 0..min(self.batch_size, remaining) {
            match self.reader.read_record(&mut self.batch_records[i]) {
                Ok(true) => {
                    read_records += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    return Some(Err(ArrowError::ParseError(format!(
                        "Error parsing line {}: {:?}",
                        self.line_number + i,
                        e
                    ))))
                }
            }
        }

        // return early if no data was loaded
        if read_records == 0 {
            return None;
        }

        // parse the batches into a RecordBatch
        let result = parse(
            &self.batch_records[..read_records],
            &self.schema.fields(),
            &self.projection,
            self.line_number,
        );

        self.line_number += read_records;

        Some(result)
    }
}

/// parses a slice of [csv_crate::StringRecord] into a [array::record_batch::RecordBatch].
fn parse(
    rows: &[StringRecord],
    fields: &[Field],
    projection: &Option<Vec<usize>>,
    line_number: usize,
) -> Result<RecordBatch> {
    let projection: Vec<usize> = match projection {
        Some(ref v) => v.clone(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };

    let arrays: Result<Vec<ArrayRef>> = projection
        .iter()
        .map(|i| {
            let i = *i;
            let field = &fields[i];
            match field.data_type() {
                &DataType::Boolean => build_boolean_array(line_number, rows, i),
                &DataType::Int8 => {
                    build_primitive_array::<Int8Type>(line_number, rows, i)
                }
                &DataType::Int16 => {
                    build_primitive_array::<Int16Type>(line_number, rows, i)
                }
                &DataType::Int32 => {
                    build_primitive_array::<Int32Type>(line_number, rows, i)
                }
                &DataType::Int64 => {
                    build_primitive_array::<Int64Type>(line_number, rows, i)
                }
                &DataType::UInt8 => {
                    build_primitive_array::<UInt8Type>(line_number, rows, i)
                }
                &DataType::UInt16 => {
                    build_primitive_array::<UInt16Type>(line_number, rows, i)
                }
                &DataType::UInt32 => {
                    build_primitive_array::<UInt32Type>(line_number, rows, i)
                }
                &DataType::UInt64 => {
                    build_primitive_array::<UInt64Type>(line_number, rows, i)
                }
                &DataType::Float32 => {
                    build_primitive_array::<Float32Type>(line_number, rows, i)
                }
                &DataType::Float64 => {
                    build_primitive_array::<Float64Type>(line_number, rows, i)
                }
                &DataType::Date32(_) => {
                    build_primitive_array::<Date32Type>(line_number, rows, i)
                }
                &DataType::Date64(_) => {
                    build_primitive_array::<Date64Type>(line_number, rows, i)
                }
                &DataType::Utf8 => {
                    let mut builder = StringBuilder::new(rows.len());
                    for row in rows.iter() {
                        match row.get(i) {
                            Some(s) => builder.append_value(s).unwrap(),
                            _ => builder.append(false).unwrap(),
                        }
                    }
                    Ok(Arc::new(builder.finish()) as ArrayRef)
                }
                other => Err(ArrowError::ParseError(format!(
                    "Unsupported data type {:?}",
                    other
                ))),
            }
        })
        .collect();

    let projected_fields: Vec<Field> =
        projection.iter().map(|i| fields[*i].clone()).collect();

    let projected_schema = Arc::new(Schema::new(projected_fields));

    arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr))
}

/// Specialized parsing implementations
trait Parser: ArrowPrimitiveType {
    fn parse(string: &str) -> Option<Self::Native> {
        string.parse::<Self::Native>().ok()
    }
}

impl Parser for Float32Type {
    fn parse(string: &str) -> Option<f32> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}
impl Parser for Float64Type {
    fn parse(string: &str) -> Option<f64> {
        lexical_core::parse(string.as_bytes()).ok()
    }
}

impl Parser for UInt64Type {}

impl Parser for UInt32Type {}

impl Parser for UInt16Type {}

impl Parser for UInt8Type {}

impl Parser for Int64Type {}

impl Parser for Int32Type {}

impl Parser for Int16Type {}

impl Parser for Int8Type {}

/// Number of days between 0001-01-01 and 1970-01-01
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

impl Parser for Date32Type {
    fn parse(string: &str) -> Option<i32> {
        use chrono::Datelike;

        match Self::DATA_TYPE {
            DataType::Date32(DateUnit::Day) => {
                let date = string.parse::<chrono::NaiveDate>().ok()?;
                Self::Native::from_i32(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
            }
            _ => None,
        }
    }
}

impl Parser for Date64Type {
    fn parse(string: &str) -> Option<i64> {
        match Self::DATA_TYPE {
            DataType::Date64(DateUnit::Millisecond) => {
                let date_time = string.parse::<chrono::NaiveDateTime>().ok()?;
                Self::Native::from_i64(date_time.timestamp_millis())
            }
            _ => None,
        }
    }
}

fn parse_item<T: Parser>(string: &str) -> Option<T::Native> {
    T::parse(string)
}

fn parse_bool(string: &str) -> Option<bool> {
    if string.eq_ignore_ascii_case("false") {
        Some(false)
    } else if string.eq_ignore_ascii_case("true") {
        Some(true)
    } else {
        None
    }
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_primitive_array<T: ArrowPrimitiveType + Parser>(
    line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
) -> Result<ArrayRef> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            match row.get(col_idx) {
                Some(s) => {
                    if s.is_empty() {
                        return Ok(None);
                    }

                    let parsed = parse_item::<T>(s);
                    match parsed {
                        Some(e) => Ok(Some(e)),
                        None => Err(ArrowError::ParseError(format!(
                            // TODO: we should surface the underlying error here.
                            "Error while parsing value {} for column {} at line {}",
                            s,
                            col_idx,
                            line_number + row_index
                        ))),
                    }
                }
                None => Ok(None),
            }
        })
        .collect::<Result<PrimitiveArray<T>>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

// parses a specific column (col_idx) into an Arrow Array.
fn build_boolean_array(
    line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
) -> Result<ArrayRef> {
    rows.iter()
        .enumerate()
        .map(|(row_index, row)| {
            match row.get(col_idx) {
                Some(s) => {
                    if s.is_empty() {
                        return Ok(None);
                    }

                    let parsed = parse_bool(s);
                    match parsed {
                        Some(e) => Ok(Some(e)),
                        None => Err(ArrowError::ParseError(format!(
                            // TODO: we should surface the underlying error here.
                            "Error while parsing value {} for column {} at line {}",
                            s,
                            col_idx,
                            line_number + row_index
                        ))),
                    }
                }
                None => Ok(None),
            }
        })
        .collect::<Result<BooleanArray>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

/// CSV file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the CSV file
    ///
    /// If the schema is not supplied, the reader will try to infer the schema
    /// based on the CSV structure.
    schema: Option<SchemaRef>,
    /// Whether the file has headers or not
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional maximum number of records to read during schema inference
    ///
    /// If a number is not provided, all the records are read.
    max_records: Option<usize>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// The bounds over which to scan the reader. `None` starts from 0 and runs until EOF.
    bounds: Bounds,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            has_header: false,
            delimiter: None,
            max_records: None,
            batch_size: 1024,
            bounds: None,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring CSV parsing options.
    ///
    /// To convert a builder into a reader, call `ReaderBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// extern crate arrow;
    ///
    /// use arrow::csv;
    /// use std::fs::File;
    ///
    /// fn example() -> csv::Reader<File> {
    ///     let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = csv::ReaderBuilder::new().infer_schema(Some(100));
    ///
    ///     let reader = builder.build(file).unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> ReaderBuilder {
        ReaderBuilder::default()
    }

    /// Set the CSV file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set whether the CSV file has headers
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the CSV reader to infer the schema of the file
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
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<R: Read + Seek>(self, mut reader: R) -> Result<Reader<R>> {
        // check if schema should be inferred
        let delimiter = self.delimiter.unwrap_or(b',');
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                let (inferred_schema, _) = infer_file_schema(
                    &mut reader,
                    delimiter,
                    self.max_records,
                    self.has_header,
                )?;

                Arc::new(inferred_schema)
            }
        };
        Ok(Reader::from_reader(
            reader,
            schema,
            self.has_header,
            self.delimiter,
            self.batch_size,
            None,
            self.projection.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    use crate::array::*;
    use crate::datatypes::Field;

    #[test]
    fn test_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = Reader::new(
            file,
            Arc::new(schema.clone()),
            false,
            None,
            1024,
            None,
            None,
        );
        assert_eq!(Arc::new(schema), csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(57.653484 - lat.value(0) < f64::EPSILON);

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_from_buf_reader() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file_with_headers =
            File::open("test/data/uk_cities_with_headers.csv").unwrap();
        let file_without_headers = File::open("test/data/uk_cities.csv").unwrap();
        let both_files = file_with_headers
            .chain(Cursor::new("\n".to_string()))
            .chain(file_without_headers);
        let mut csv = Reader::from_reader(
            both_files,
            Arc::new(schema),
            true,
            None,
            1024,
            None,
            None,
        );
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(74, batch.num_rows());
        assert_eq!(3, batch.num_columns());
    }

    #[test]
    fn test_csv_with_schema_inference() {
        let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();

        let builder = ReaderBuilder::new().has_header(true).infer_schema(None);

        let mut csv = builder.build(file).unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);
        assert_eq!(Arc::new(expected_schema), csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(57.653484 - lat.value(0) < f64::EPSILON);

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_with_schema_inference_no_headers() {
        let file = File::open("test/data/uk_cities.csv").unwrap();

        let builder = ReaderBuilder::new().infer_schema(None);

        let mut csv = builder.build(file).unwrap();

        // csv field names should be 'column_{number}'
        let schema = csv.schema();
        assert_eq!("column_1", schema.field(0).name());
        assert_eq!("column_2", schema.field(1).name());
        assert_eq!("column_3", schema.field(2).name());
        let batch = csv.next().unwrap().unwrap();
        let batch_schema = batch.schema();

        assert_eq!(schema, batch_schema);
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(57.653484 - lat.value(0) < f64::EPSILON);

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }

    #[test]
    fn test_csv_with_projection() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = Reader::new(
            file,
            Arc::new(schema),
            false,
            None,
            1024,
            None,
            Some(vec![0, 1]),
        );
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
        ]));
        assert_eq!(projected_schema, csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(projected_schema, batch.schema());
        assert_eq!(37, batch.num_rows());
        assert_eq!(2, batch.num_columns());
    }

    #[test]
    fn test_nulls() {
        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
        ]);

        let file = File::open("test/data/null_test.csv").unwrap();

        let mut csv = Reader::new(file, Arc::new(schema), true, None, 1024, None, None);
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(false, batch.column(1).is_null(0));
        assert_eq!(false, batch.column(1).is_null(1));
        assert_eq!(true, batch.column(1).is_null(2));
        assert_eq!(false, batch.column(1).is_null(3));
        assert_eq!(false, batch.column(1).is_null(4));
    }

    #[test]
    fn test_nulls_with_inference() {
        let file = File::open("test/data/various_types.csv").unwrap();

        let builder = ReaderBuilder::new()
            .infer_schema(None)
            .has_header(true)
            .with_delimiter(b'|')
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3, 4, 5]);

        let mut csv = builder.build(file).unwrap();
        let batch = csv.next().unwrap().unwrap();

        assert_eq!(5, batch.num_rows());
        assert_eq!(6, batch.num_columns());

        let schema = batch.schema();

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Float64, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());
        assert_eq!(
            &DataType::Date32(DateUnit::Day),
            schema.field(4).data_type()
        );
        assert_eq!(
            &DataType::Date64(DateUnit::Millisecond),
            schema.field(5).data_type()
        );

        let names: Vec<&str> =
            schema.fields().iter().map(|x| x.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "c_int",
                "c_float",
                "c_string",
                "c_bool",
                "c_date",
                "c_datetime"
            ]
        );

        assert_eq!(false, schema.field(0).is_nullable());
        assert_eq!(true, schema.field(1).is_nullable());
        assert_eq!(true, schema.field(2).is_nullable());
        assert_eq!(false, schema.field(3).is_nullable());
        assert_eq!(true, schema.field(4).is_nullable());
        assert_eq!(true, schema.field(5).is_nullable());

        assert_eq!(false, batch.column(1).is_null(0));
        assert_eq!(false, batch.column(1).is_null(1));
        assert_eq!(true, batch.column(1).is_null(2));
        assert_eq!(false, batch.column(1).is_null(3));
        assert_eq!(false, batch.column(1).is_null(4));
    }

    #[test]
    fn test_parse_invalid_csv() {
        let file = File::open("test/data/various_types_invalid.csv").unwrap();

        let schema = Schema::new(vec![
            Field::new("c_int", DataType::UInt64, false),
            Field::new("c_float", DataType::Float32, false),
            Field::new("c_string", DataType::Utf8, false),
            Field::new("c_bool", DataType::Boolean, false),
        ]);

        let builder = ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .has_header(true)
            .with_delimiter(b'|')
            .with_batch_size(512)
            .with_projection(vec![0, 1, 2, 3]);

        let mut csv = builder.build(file).unwrap();
        match csv.next() {
            Some(e) => match e {
                Err(e) => assert_eq!(
                    "ParseError(\"Error while parsing value 4.x4 for column 1 at line 4\")",
                    format!("{:?}", e)
                ),
                Ok(_) => panic!("should have failed"),
            },
            None => panic!("should have failed"),
        }
    }

    #[test]
    fn test_infer_field_schema() {
        assert_eq!(infer_field_schema("A"), DataType::Utf8);
        assert_eq!(infer_field_schema("\"123\""), DataType::Utf8);
        assert_eq!(infer_field_schema("10"), DataType::Int64);
        assert_eq!(infer_field_schema("10.2"), DataType::Float64);
        assert_eq!(infer_field_schema("true"), DataType::Boolean);
        assert_eq!(infer_field_schema("false"), DataType::Boolean);
        assert_eq!(
            infer_field_schema("2020-11-08"),
            DataType::Date32(DateUnit::Day)
        );
        assert_eq!(
            infer_field_schema("2020-11-08T14:20:01"),
            DataType::Date64(DateUnit::Millisecond)
        );
    }

    #[test]
    fn parse_date32() {
        assert_eq!(parse_item::<Date32Type>("1970-01-01").unwrap(), 0);
        assert_eq!(parse_item::<Date32Type>("2020-03-15").unwrap(), 18336);
        assert_eq!(parse_item::<Date32Type>("1945-05-08").unwrap(), -9004);
    }

    #[test]
    fn parse_date64() {
        assert_eq!(parse_item::<Date64Type>("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(
            parse_item::<Date64Type>("2018-11-13T17:11:10").unwrap(),
            1542129070000
        );
        assert_eq!(
            parse_item::<Date64Type>("2018-11-13T17:11:10.011").unwrap(),
            1542129070011
        );
        assert_eq!(
            parse_item::<Date64Type>("1900-02-28T12:34:56").unwrap(),
            -2203932304000
        );
    }

    #[test]
    fn test_infer_schema_from_multiple_files() -> Result<()> {
        let mut csv1 = NamedTempFile::new()?;
        let mut csv2 = NamedTempFile::new()?;
        let csv3 = NamedTempFile::new()?; // empty csv file should be skipped
        let mut csv4 = NamedTempFile::new()?;
        writeln!(csv1, "c1,c2,c3")?;
        writeln!(csv1, "1,\"foo\",0.5")?;
        writeln!(csv1, "3,\"bar\",1")?;
        // reading csv2 will set c2 to optional
        writeln!(csv2, "c1,c2,c3,c4")?;
        writeln!(csv2, "10,,3.14,true")?;
        // reading csv4 will set c3 to optional
        writeln!(csv4, "c1,c2,c3")?;
        writeln!(csv4, "10,\"foo\",")?;

        let schema = infer_schema_from_files(
            &[
                csv3.path().to_str().unwrap().to_string(),
                csv1.path().to_str().unwrap().to_string(),
                csv2.path().to_str().unwrap().to_string(),
                csv4.path().to_str().unwrap().to_string(),
            ],
            b',',
            Some(3), // only csv1 and csv2 should be read
            true,
        )?;

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(false, schema.field(0).is_nullable());
        assert_eq!(true, schema.field(1).is_nullable());
        assert_eq!(false, schema.field(2).is_nullable());
        assert_eq!(false, schema.field(3).is_nullable());

        assert_eq!(&DataType::Int64, schema.field(0).data_type());
        assert_eq!(&DataType::Utf8, schema.field(1).data_type());
        assert_eq!(&DataType::Float64, schema.field(2).data_type());
        assert_eq!(&DataType::Boolean, schema.field(3).data_type());

        Ok(())
    }

    #[test]
    fn test_bounded() -> Result<()> {
        let schema = Schema::new(vec![Field::new("int", DataType::UInt32, false)]);
        let data = vec![
            vec!["0"],
            vec!["1"],
            vec!["2"],
            vec!["3"],
            vec!["4"],
            vec!["5"],
            vec!["6"],
        ];

        let data = data
            .iter()
            .map(|x| x.join(","))
            .collect::<Vec<_>>()
            .join("\n");
        let data = data.as_bytes();

        let reader = std::io::Cursor::new(data);

        let mut csv = Reader::new(
            reader,
            Arc::new(schema),
            false,
            None,
            2,
            // starting at row 2 and up to row 6.
            Some((2, 6)),
            Some(vec![0]),
        );

        let batch = csv.next().unwrap().unwrap();
        let a = batch.column(0);
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(a, &UInt32Array::from(vec![2, 3]));

        let batch = csv.next().unwrap().unwrap();
        let a = batch.column(0);
        let a = a.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(a, &UInt32Array::from(vec![4, 5]));

        assert!(csv.next().is_none());
        Ok(())
    }

    #[test]
    fn test_parsing_bool() {
        // Encode the expected behavior of boolean parsing
        assert_eq!(Some(true), parse_bool("true"));
        assert_eq!(Some(true), parse_bool("tRUe"));
        assert_eq!(Some(true), parse_bool("True"));
        assert_eq!(Some(true), parse_bool("TRUE"));
        assert_eq!(None, parse_bool("t"));
        assert_eq!(None, parse_bool("T"));
        assert_eq!(None, parse_bool(""));

        assert_eq!(Some(false), parse_bool("false"));
        assert_eq!(Some(false), parse_bool("fALse"));
        assert_eq!(Some(false), parse_bool("False"));
        assert_eq!(Some(false), parse_bool("FALSE"));
        assert_eq!(None, parse_bool("f"));
        assert_eq!(None, parse_bool("F"));
        assert_eq!(None, parse_bool(""));
    }

    #[test]
    fn test_parsing_float() {
        assert_eq!(Some(12.34), parse_item::<Float64Type>("12.34"));
        assert_eq!(Some(-12.34), parse_item::<Float64Type>("-12.34"));
        assert_eq!(Some(12.0), parse_item::<Float64Type>("12"));
        assert_eq!(Some(0.0), parse_item::<Float64Type>("0"));
        assert!(parse_item::<Float64Type>("nan").unwrap().is_nan());
        assert!(parse_item::<Float64Type>("NaN").unwrap().is_nan());
        assert!(parse_item::<Float64Type>("inf").unwrap().is_infinite());
        assert!(parse_item::<Float64Type>("inf").unwrap().is_sign_positive());
        assert!(parse_item::<Float64Type>("-inf").unwrap().is_infinite());
        assert!(parse_item::<Float64Type>("-inf")
            .unwrap()
            .is_sign_negative());
        assert_eq!(None, parse_item::<Float64Type>(""));
        assert_eq!(None, parse_item::<Float64Type>("dd"));
        assert_eq!(None, parse_item::<Float64Type>("12.34.56"));
    }
}
