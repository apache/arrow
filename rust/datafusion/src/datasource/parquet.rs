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

//! Parquet Data source

use std::fs::File;
use std::string::String;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, PrimitiveArray};
use arrow::builder::{BinaryBuilder, PrimitiveBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use parquet::column::reader::*;
use parquet::data_type::{ByteArray, Int96};
use parquet::file::reader::*;
use parquet::reader::schema::parquet_to_arrow_schema;

use crate::datasource::{RecordBatchIterator, ScanResult, TableProvider};
use crate::error::{ExecutionError, Result};

/// Table-based representation of a `ParquetFile`
pub struct ParquetTable {
    filename: String,
    schema: Arc<Schema>,
}

impl ParquetTable {
    /// Attempt to initialize a new `ParquetTable` from a file path
    pub fn try_new(filename: &str) -> Result<Self> {
        let file = File::open(filename)?;
        let parquet_file = ParquetFile::open(file, None, 0)?;
        let schema = parquet_file.projection_schema.clone();
        Ok(Self {
            filename: filename.to_string(),
            schema,
        })
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Vec<ScanResult>> {
        let file = File::open(self.filename.clone())?;
        let parquet_file = ParquetFile::open(file, projection.clone(), batch_size)?;
        Ok(vec![Arc::new(Mutex::new(parquet_file))])
    }
}

/// Loader and reader for parquet data
pub struct ParquetFile {
    reader: SerializedFileReader<File>,
    /// Projection expressed as column indices into underlying parquet reader
    projection: Vec<usize>,
    /// The schema of the projection
    projection_schema: Arc<Schema>,
    batch_size: usize,
    row_group_index: usize,
    current_row_group: Option<Box<RowGroupReader>>,
    column_readers: Vec<ColumnReader>,
}

macro_rules! read_binary_column {
    ($SELF:ident, $R:ident, $INDEX:expr) => {{
        let mut read_buffer: Vec<ByteArray> =
            vec![ByteArray::default(); $SELF.batch_size];
        let mut def_levels: Vec<i16> = vec![0; $SELF.batch_size];
        let (_, levels_read) = $R.read_batch(
            $SELF.batch_size,
            Some(&mut def_levels),
            None,
            &mut read_buffer,
        )?;
        let mut builder = BinaryBuilder::new(levels_read);
        let mut value_index = 0;
        for i in 0..levels_read {
            if def_levels[i] > 0 {
                builder.append_string(
                    &String::from_utf8(read_buffer[value_index].data().to_vec()).unwrap(),
                )?;
                value_index += 1;
            } else {
                builder.append_null()?;
            }
        }
        Arc::new(builder.finish())
    }};
}

trait ArrowReader<T>
where
    T: ArrowPrimitiveType,
{
    fn read(
        &mut self,
        batch_size: usize,
        is_nullable: bool,
    ) -> Result<Arc<PrimitiveArray<T>>>;
}

impl<A, P> ArrowReader<A> for ColumnReaderImpl<P>
where
    A: ArrowPrimitiveType,
    P: parquet::data_type::DataType,
    P::T: std::convert::From<A::Native>,
    A::Native: std::convert::From<P::T>,
{
    fn read(
        &mut self,
        batch_size: usize,
        is_nullable: bool,
    ) -> Result<Arc<PrimitiveArray<A>>> {
        // create read buffer
        let mut read_buffer: Vec<P::T> = vec![A::default_value().into(); batch_size];

        if is_nullable {
            let mut def_levels: Vec<i16> = vec![0; batch_size];

            let (values_read, levels_read) = self.read_batch(
                batch_size,
                Some(&mut def_levels),
                None,
                &mut read_buffer,
            )?;
            let mut builder = PrimitiveBuilder::<A>::new(levels_read);
            let converted_buffer: Vec<A::Native> =
                read_buffer.into_iter().map(|v| v.into()).collect();
            if values_read == levels_read {
                builder.append_slice(&converted_buffer[0..values_read])?;
            } else {
                let mut value_index = 0;
                for i in 0..def_levels.len() {
                    if def_levels[i] != 0 {
                        builder.append_value(converted_buffer[value_index].into())?;
                        value_index += 1;
                    } else {
                        builder.append_null()?;
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        } else {
            let (values_read, _) =
                self.read_batch(batch_size, None, None, &mut read_buffer)?;

            let mut builder = PrimitiveBuilder::<A>::new(values_read);
            let converted_buffer: Vec<A::Native> =
                read_buffer.into_iter().map(|v| v.into()).collect();
            builder.append_slice(&converted_buffer[0..values_read])?;
            Ok(Arc::new(builder.finish()))
        }
    }
}

impl ParquetFile {
    /// Read parquet data from a `File`
    pub fn open(
        file: File,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let reader = SerializedFileReader::new(file)?;

        let metadata = reader.metadata();
        let schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr_ptr())?;

        // even if we aren't referencing structs or lists in our projection, column reader
        // indexes will be off until we have support for nested schemas
        for i in 0..schema.fields().len() {
            match schema.field(i).data_type() {
                DataType::List(_) => {
                    return Err(ExecutionError::NotImplemented(
                        "Parquet datasource does not support LIST".to_string(),
                    ));
                }
                DataType::Struct(_) => {
                    return Err(ExecutionError::NotImplemented(
                        "Parquet datasource does not support STRUCT".to_string(),
                    ));
                }
                _ => {}
            }
        }

        let projection = match projection {
            Some(p) => p,
            None => {
                let mut p = Vec::with_capacity(schema.fields().len());
                for i in 0..schema.fields().len() {
                    p.push(i);
                }
                p
            }
        };

        let projected_schema = schema_projection(&schema, &projection)?;

        Ok(ParquetFile {
            reader: reader,
            row_group_index: 0,
            projection_schema: projected_schema,
            projection,
            batch_size,
            current_row_group: None,
            column_readers: vec![],
        })
    }

    fn load_next_row_group(&mut self) -> Result<()> {
        if self.row_group_index < self.reader.num_row_groups() {
            let reader = self.reader.get_row_group(self.row_group_index)?;

            self.column_readers.clear();
            self.column_readers = Vec::with_capacity(self.projection.len());

            for i in 0..self.projection.len() {
                self.column_readers
                    .push(reader.get_column_reader(self.projection[i])?);
            }

            self.current_row_group = Some(reader);
            self.row_group_index += 1;

            Ok(())
        } else {
            Err(ExecutionError::General(
                "Attempt to read past final row group".to_string(),
            ))
        }
    }

    fn load_batch(&mut self) -> Result<Option<RecordBatch>> {
        match &self.current_row_group {
            Some(reader) => {
                let mut batch: Vec<Arc<Array>> = Vec::with_capacity(reader.num_columns());
                for i in 0..self.column_readers.len() {
                    let dt = self.schema().field(i).data_type().clone();
                    let is_nullable = self.schema().field(i).is_nullable();
                    let array: Arc<Array> = match self.column_readers[i] {
                        ColumnReader::BoolColumnReader(ref mut r) => {
                            ArrowReader::<BooleanType>::read(
                                r,
                                self.batch_size,
                                is_nullable,
                            )?
                        }
                        ColumnReader::Int32ColumnReader(ref mut r) => match dt {
                            DataType::Date32(DateUnit::Day) => {
                                ArrowReader::<Date32Type>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            DataType::Time32(TimeUnit::Millisecond) => {
                                ArrowReader::<Time32MillisecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            _ => ArrowReader::<Int32Type>::read(
                                r,
                                self.batch_size,
                                is_nullable,
                            )?,
                        },
                        ColumnReader::Int64ColumnReader(ref mut r) => match dt {
                            DataType::Time64(TimeUnit::Microsecond) => {
                                ArrowReader::<Time64MicrosecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            DataType::Time64(TimeUnit::Nanosecond) => {
                                ArrowReader::<Time64NanosecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            DataType::Timestamp(TimeUnit::Millisecond) => {
                                ArrowReader::<TimestampMillisecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            DataType::Timestamp(TimeUnit::Microsecond) => {
                                ArrowReader::<TimestampMicrosecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            DataType::Timestamp(TimeUnit::Nanosecond) => {
                                ArrowReader::<TimestampMicrosecondType>::read(
                                    r,
                                    self.batch_size,
                                    is_nullable,
                                )?
                            }
                            _ => ArrowReader::<Int64Type>::read(
                                r,
                                self.batch_size,
                                is_nullable,
                            )?,
                        },
                        ColumnReader::Int96ColumnReader(ref mut r) => {
                            let mut read_buffer: Vec<Int96> =
                                vec![Int96::new(); self.batch_size];

                            let mut def_levels: Vec<i16> = vec![0; self.batch_size];
                            let (_, levels_read) = r.read_batch(
                                self.batch_size,
                                Some(&mut def_levels),
                                None,
                                &mut read_buffer,
                            )?;

                            let mut builder =
                                TimestampNanosecondBuilder::new(levels_read);
                            let mut value_index = 0;
                            for i in 0..levels_read {
                                if def_levels[i] > 0 {
                                    builder.append_value(convert_int96_timestamp(
                                        read_buffer[value_index].data(),
                                    ))?;
                                    value_index += 1;
                                } else {
                                    builder.append_null()?;
                                }
                            }
                            Arc::new(builder.finish())
                        }
                        ColumnReader::FloatColumnReader(ref mut r) => {
                            ArrowReader::<Float32Type>::read(
                                r,
                                self.batch_size,
                                is_nullable,
                            )?
                        }
                        ColumnReader::DoubleColumnReader(ref mut r) => {
                            ArrowReader::<Float64Type>::read(
                                r,
                                self.batch_size,
                                is_nullable,
                            )?
                        }
                        ColumnReader::FixedLenByteArrayColumnReader(ref mut r) => {
                            read_binary_column!(self, r, i)
                        }
                        ColumnReader::ByteArrayColumnReader(ref mut r) => {
                            read_binary_column!(self, r, i)
                        }
                    };

                    batch.push(array);
                }

                if batch.len() == 0 || batch[0].data().len() == 0 {
                    Ok(None)
                } else {
                    Ok(Some(RecordBatch::try_new(
                        self.projection_schema.clone(),
                        batch,
                    )?))
                }
            }
            _ => Ok(None),
        }
    }
}

/// Create a new schema by applying a projection to this schema's fields
fn schema_projection(schema: &Schema, projection: &[usize]) -> Result<Arc<Schema>> {
    let mut fields: Vec<Field> = Vec::with_capacity(projection.len());
    for i in projection {
        if *i < schema.fields().len() {
            fields.push(schema.field(*i).clone());
        } else {
            return Err(ExecutionError::InvalidColumn(format!(
                "Invalid column index {} in projection",
                i
            )));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// convert a Parquet INT96 to an Arrow timestamp in nanoseconds
fn convert_int96_timestamp(v: &[u32]) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MILLIS_PER_SECOND: i64 = 1_000;

    let day = v[2] as i64;
    let nanoseconds = ((v[1] as i64) << 32) + v[0] as i64;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
    seconds * MILLIS_PER_SECOND * 1_000_000 + nanoseconds
}

impl RecordBatchIterator for ParquetFile {
    fn schema(&self) -> &Arc<Schema> {
        &self.projection_schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        // advance the row group reader if necessary
        if self.current_row_group.is_none() {
            self.load_next_row_group()?;
            self.load_batch()
        } else {
            match self.load_batch() {
                Ok(Some(b)) => Ok(Some(b)),
                Ok(None) => {
                    if self.row_group_index < self.reader.num_row_groups() {
                        self.load_next_row_group()?;
                        self.load_batch()
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampNanosecondArray,
    };
    use std::env;

    #[test]
    fn read_small_batches() {
        let table = load_table("alltypes_plain.parquet");

        let projection = None;
        let scan = table.scan(&projection, 2).unwrap();
        let mut it = scan[0].lock().unwrap();

        let mut count = 0;
        while let Some(batch) = it.next().unwrap() {
            assert_eq!(11, batch.num_columns());
            assert_eq!(2, batch.num_rows());
            count += 1;
        }

        // we should have seen 4 batches of 2 rows
        assert_eq!(4, count);
    }

    #[test]
    fn read_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let x: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Utf8\n\
             string_col: Utf8\n\
             timestamp_col: Timestamp(Nanosecond)",
            y
        );

        let projection = None;
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());
    }

    #[test]
    fn read_bool_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![1]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_i32_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![0]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));
    }

    #[test]
    fn read_i96_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![10]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));
    }

    #[test]
    fn read_f32_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![6]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_f64_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![7]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );
    }

    #[test]
    fn read_utf8_alltypes_plain_parquet() {
        let table = load_table("alltypes_plain.parquet");

        let projection = Some(vec![9]);
        let scan = table.scan(&projection, 1024).unwrap();
        let mut it = scan[0].lock().unwrap();
        let batch = it.next().unwrap().unwrap();

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<String> = vec![];
        for i in 0..batch.num_rows() {
            let str: String = String::from_utf8(array.value(i).to_vec()).unwrap();
            values.push(str);
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );
    }

    fn load_table(name: &str) -> Box<TableProvider> {
        let testdata =
            env::var("PARQUET_TEST_DATA").expect("PARQUET_TEST_DATA not defined");
        let filename = format!("{}/{}", testdata, name);
        let table = ParquetTable::try_new(&filename).unwrap();
        Box::new(table)
    }
}
