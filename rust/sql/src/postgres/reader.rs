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

use std::convert::TryFrom;
use std::{
    io::{Read, Write},
    sync::Arc,
};

use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::{
    DataType, DateUnit, Field, IntervalUnit, Schema, TimeUnit, ToByteSlice,
};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use byteorder::{NetworkEndian, ReadBytesExt};
use chrono::Timelike;
use postgres::types::*;
use postgres::{Client, NoTls, Row};

use super::{
    Postgres, PostgresReadIterator, EPOCH_DAYS, EPOCH_MICROS, MAGIC, UNSUPPORTED_TYPE_ERR,
};
use crate::SqlDataSource;

impl SqlDataSource for Postgres {
    fn get_table_schema(connection_string: &str, table_name: &str) -> Result<Schema> {
        let (table_schema, table_name) = if table_name.contains('.') {
            let split = table_name.split('.').collect::<Vec<&str>>();
            if split.len() != 2 {
                return Err(ArrowError::IoError(
                    "table name must have schema and table name only, or just table name"
                        .to_string(),
                ));
            }
            (
                format!("table_schema = '{}'", split[0]),
                format!(" and table_name = '{}'", split[1]),
            )
        } else {
            (String::new(), format!("table_name = '{}'", table_name))
        };
        let mut client = Client::connect(connection_string, NoTls).unwrap();
        let results = client
            .query(format!("select column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, datetime_precision from information_schema.columns where {}{}", table_schema, table_name).as_str(), &[])
            .unwrap();
        let fields: Result<Vec<Field>> = results
            .iter()
            .map(|row| PgDataType {
                column_name: row.get("column_name"),
                ordinal_position: row.get("ordinal_position"),
                is_nullable: row.get("is_nullable"),
                data_type: row.get("data_type"),
                char_max_length: row.get("character_maximum_length"),
                numeric_precision: row.get("numeric_precision"),
                datetime_precision: row.get("datetime_precision"),
            })
            .map(Field::try_from)
            .collect();
        Ok(Schema::new(fields.unwrap()))
    }

    fn read_table(
        connection: &str,
        table_name: &str,
        limit: Option<usize>,
        _batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        // create connection
        let mut client = Client::connect(connection, NoTls).unwrap();
        let total_rows = client
            .query_one(
                format!("select count(1) as freq from {}", table_name).as_str(),
                &[],
            )
            .expect("Unable to get row count");
        let total_rows: i64 = total_rows.get("freq");
        let limit = limit.unwrap_or(std::usize::MAX).min(total_rows as usize);
        // get schema
        // TODO: reuse connection
        // TODO: split read into multiple batches, using limit and skip
        let schema = Postgres::get_table_schema(connection, table_name)?;
        let reader = get_binary_reader(
            &mut client,
            format!("select * from {} limit {}", table_name, limit).as_str(),
        )?;
        read_from_binary(reader, &schema).map(|batch| vec![batch])
    }

    fn read_query(
        connection: &str,
        query: &str,
        limit: Option<usize>,
        _batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        // create connection
        let mut client = Client::connect(connection, NoTls).unwrap();
        let total_rows = client
            .query_one(
                format!("select count(1) as freq from ({}) a", query).as_str(),
                &[],
            )
            .expect("Unable to get row count");
        let total_rows: i64 = total_rows.get("freq");
        let limit = limit.unwrap_or(std::usize::MAX).min(total_rows as usize);
        // get schema
        // TODO: reuse connection
        // TODO: split read into multiple batches, using limit and skip
        let row = client
            .query_one(
                format!("select a.* from ({}) a limit 1", query).as_str(),
                &[],
            )
            .unwrap();
        let schema = row_to_schema(&row).expect("Unable to get schema from row");
        let reader = get_binary_reader(
            &mut client,
            format!("select a.* from ({}) a limit {}", query, limit).as_str(),
        )?;
        read_from_binary(reader, &schema).map(|batch| vec![batch])
    }
}

impl PostgresReadIterator {
    /// Create a new Postgres reader
    pub fn try_new(
        connection: &str,
        query: &str,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Self> {
        let mut client = Client::connect(connection, NoTls).unwrap();
        // get schema
        let row = client
            .query_one(
                format!("select a.* from ({}) a limit 1", query).as_str(),
                &[],
            )
            .unwrap();
        let schema = row_to_schema(&row).expect("Unable to get schema from row");
        Ok(Self {
            client,
            query: query.to_string(),
            limit: limit.unwrap_or(std::usize::MAX),
            batch_size,
            schema,
            read_records: 0,
            is_complete: false,
        })
    }

    /// Read the next batch
    fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_complete {
            return Ok(None);
        }
        let reader = get_binary_reader(
            &mut self.client,
            format!(
                "select a.* from ({}) a limit {} offset {}",
                self.query, self.batch_size, self.read_records
            )
            .as_str(),
        )?;
        let batch = read_from_binary(reader, &self.schema)?;
        println!(
            "Read {} records from offset {}",
            batch.num_rows(),
            self.read_records
        );
        self.read_records += batch.num_rows();
        if batch.num_rows() == 0 {
            self.is_complete = true;
            return Ok(None);
        } else if self.read_records >= self.limit {
            self.is_complete = true;
            return Ok(Some(batch));
        }
        Ok(Some(batch))
    }

    /// Get a reference to the table's schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl RecordBatchReader for PostgresReadIterator {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(self.schema.clone())
    }

    fn next_batch(&mut self) -> arrow::error::Result<Option<RecordBatch>> {
        self.next().transpose()
    }
}

impl Iterator for PostgresReadIterator {
    type Item = arrow::error::Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.read_batch()
            .map_err(|e| {
                eprintln!("Postgres error: {:?}", e);
                arrow::error::ArrowError::IoError("Postgres error".to_string())
            })
            .transpose()
    }
}

fn get_binary_reader<'a>(
    client: &'a mut Client,
    query: &str,
) -> Result<postgres::CopyOutReader<'a>> {
    Ok(client
        .copy_out(format!("COPY ({}) TO stdout with (format binary)", query).as_str())
        .unwrap())
}

struct PgDataType {
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    char_max_length: Option<i32>,
    numeric_precision: Option<i32>,
    datetime_precision: Option<i32>,
}

impl TryFrom<PgDataType> for Field {
    type Error = ArrowError;
    fn try_from(field: PgDataType) -> Result<Self> {
        let data_type = match field.data_type.as_str() {
            "int" | "integer" => match field.numeric_precision {
                Some(8) => Ok(DataType::Int8),
                Some(16) => Ok(DataType::Int16),
                Some(32) => Ok(DataType::Int32),
                Some(64) => Ok(DataType::Int64),
                _ => Err(UNSUPPORTED_TYPE_ERR),
            },
            "bigint" => Ok(DataType::Int64),
            "\"char\"" | "character" => {
                if field.char_max_length == Some(1) {
                    Ok(DataType::UInt8)
                } else {
                    Ok(DataType::Binary)
                }
            }
            // "anyarray" | "ARRAY" => Err(UNSUPPORTED_TYPE_ERR),
            "boolean" => Ok(DataType::Boolean),
            "bytea" => Ok(DataType::Binary),
            "character varying" => Ok(DataType::Utf8),
            "date" => Ok(DataType::Date32(DateUnit::Day)),
            "double precision" => Ok(DataType::Float64),
            // "inet" => Err(UNSUPPORTED_TYPE_ERR),
            "interval" => Ok(DataType::Interval(IntervalUnit::DayTime)), // TODO: use appropriate unit
            // "name" => Err(UNSUPPORTED_TYPE_ERR),
            "numeric" => Ok(DataType::Float64),
            // "oid" => Err(UNSUPPORTED_TYPE_ERR),
            "real" => Ok(DataType::Float32),
            "smallint" => Ok(DataType::Int16),
            "text" => Ok(DataType::Utf8),
            "time" | "time without time zone" => {
                Ok(DataType::Time64(TimeUnit::Microsecond))
            } // TODO: use datetime_precision to determine correct type
            "timestamp with time zone" => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            "timestamp" | "timestamp without time zone" => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            "uuid" => Ok(DataType::Binary), // TODO: use a more specialised data type
            t => {
                eprintln!("Conversion not set for data type: {:?}", t);
                Err(UNSUPPORTED_TYPE_ERR)
            }
        };
        Ok(Field::new(
            &field.column_name,
            data_type.unwrap(),
            &field.is_nullable == "YES",
        ))
    }
}

/// Convert Postgres Type to Arrow DataType
///
/// Not all types are covered, but can be easily added
fn pg_to_arrow_type(dt: &Type) -> Option<DataType> {
    match dt {
        &Type::BOOL => Some(DataType::Boolean),
        &Type::BYTEA
        | &Type::CHAR
        | &Type::BPCHAR
        | &Type::NAME
        | &Type::TEXT
        | &Type::VARCHAR => Some(DataType::Utf8),
        &Type::INT2 => Some(DataType::Int16),
        &Type::INT4 => Some(DataType::Int32),
        &Type::INT8 => Some(DataType::Int64),
        // &Type::NUMERIC => Some(DataType::Float64),
        //        &OID => None,
        //        &JSON => None,
        &Type::FLOAT4 => Some(DataType::Float32),
        &Type::FLOAT8 => Some(DataType::Float64),
        //        &ABSTIME => None,
        //        &RELTIME => None,
        //        &TINTERVAL => None,
        //        &MONEY => None,
        &Type::BOOL_ARRAY => Some(DataType::List(Box::new(Field::new(
            "list",
            DataType::Boolean,
            true,
        )))),
        &Type::BYTEA_ARRAY | &Type::CHAR_ARRAY | &Type::NAME_ARRAY => Some(
            DataType::List(Box::new(Field::new("list", DataType::Utf8, true))),
        ),
        //        &INT2_ARRAY => None,
        //        &INT2_VECTOR => None,
        //        &INT2_VECTOR_ARRAY => None,
        //        &INT4_ARRAY => None,
        //        &TEXT_ARRAY => None,
        //        &INT8_ARRAY => None,
        //        &FLOAT4_ARRAY => None,
        //        &FLOAT8_ARRAY => None,
        //        &ABSTIME_ARRAY => None,
        //        &RELTIME_ARRAY => None,
        //        &TINTERVAL_ARRAY => None,
        &Type::DATE => Some(DataType::Date32(DateUnit::Day)),
        &Type::TIME => Some(DataType::Time64(TimeUnit::Microsecond)),
        &Type::INTERVAL => Some(DataType::Interval(IntervalUnit::DayTime)),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        //        &TIMESTAMP_ARRAY => None,
        &Type::DATE_ARRAY => Some(DataType::List(Box::new(Field::new(
            "item",
            DataType::Date32(DateUnit::Day),
            true,
        )))),
        //        &TIME_ARRAY => None,
        //        &TIMESTAMPTZ => None,
        //        &TIMESTAMPTZ_ARRAY => None,
        //        &INTERVAL => None,
        //        &INTERVAL_ARRAY => None,
        //        &NUMERIC_ARRAY => None,
        //        &TIMETZ => None,
        //        &BIT => None,
        //        &BIT_ARRAY => None,
        //        &VARBIT => None,
        //        &NUMERIC => None,
        &Type::UUID => Some(DataType::FixedSizeBinary(16)),
        t => panic!("Postgres type {:?} not supported", t),
    }
}

fn read_table_by_rows(
    connection_string: &str,
    table_name: &str,
    _limit: usize,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    // create connection
    let mut client = Client::connect(connection_string, NoTls).unwrap();
    let results = client
        .query(format!("SELECT * FROM {}", table_name).as_str(), &[])
        .unwrap();
    if results.is_empty() {
        return Ok(vec![]);
    }

    // TODO: replace with metadata version of call
    let schema = row_to_schema(results.get(0).unwrap()).unwrap();
    let field_len = schema.fields().len();
    let mut builder = StructBuilder::from_fields(schema.fields().clone(), batch_size);
    let chunks = results.chunks(batch_size);
    let mut batches = vec![];
    chunks.for_each(|chunk: &[Row]| {
        for j in 0..field_len {
            match schema.field(j).data_type() {
                DataType::Int32 => {
                    let field_builder = builder.field_builder::<Int32Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Int64 => {
                    let field_builder = builder.field_builder::<Int64Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Float64 => {
                    let field_builder =
                        builder.field_builder::<Float64Builder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        match row.try_get(j) {
                            Ok(value) => field_builder.append_value(value).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let field_builder = builder
                        .field_builder::<TimestampMillisecondBuilder>(j)
                        .unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        let timestamp: chrono::NaiveDateTime = row.get(j);
                        field_builder
                            .append_value(timestamp.timestamp_millis())
                            .unwrap();
                    }
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    let field_builder = builder
                        .field_builder::<Time64MicrosecondBuilder>(j)
                        .unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        let time: chrono::NaiveTime = row.get(j);
                        field_builder
                            .append_value(
                                time.num_seconds_from_midnight() as i64 * 1000000
                                    + time.nanosecond() as i64 / 1000,
                            )
                            .unwrap();
                    }
                }
                DataType::Boolean => {
                    let field_builder =
                        builder.field_builder::<BooleanBuilder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        field_builder.append_value(row.get(j)).unwrap();
                    }
                }
                DataType::Utf8 => {
                    let field_builder =
                        builder.field_builder::<StringBuilder>(j).unwrap();
                    for i in 0..chunk.len() {
                        let row: &Row = chunk.get(i).unwrap();
                        field_builder.append_value(row.get(j)).unwrap();
                    }
                }
                t => panic!("Field builder for {:?} not yet supported", t),
            }
        }
        // TODO perhaps change the order of processing so we can do this earlier
        for _ in 0..chunk.len() {
            builder.append(true).unwrap();
        }
        let batch: RecordBatch = RecordBatch::from(&builder.finish());
        batches.push(batch);
    });
    Ok(batches)
}

/// Generate Arrow schema from a row
fn row_to_schema(row: &postgres::Row) -> Result<Schema> {
    let mut metadata = std::collections::HashMap::new();
    let fields = row
        .columns()
        .iter()
        .map(|col: &postgres::Column| {
            metadata.insert(col.name().to_string(), col.type_().to_string());
            Field::new(col.name(), pg_to_arrow_type(col.type_()).unwrap(), true)
        })
        .collect();
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn read_from_binary<R>(mut reader: R, schema: &Schema) -> Result<RecordBatch>
where
    R: Read,
{
    // read signature
    let mut bytes = [0u8; 11];
    reader.read_exact(&mut bytes).unwrap();
    if bytes != MAGIC {
        eprintln!("Unexpected binary format type");
        return Err(ArrowError::IoError(
            "Unexpected Postgres binary type".to_string(),
        ));
    }
    // read flags
    let mut bytes: [u8; 4] = [0; 4];
    reader.read_exact(&mut bytes).unwrap();
    let _size = u32::from_be_bytes(bytes);

    // header extension area length
    let mut bytes: [u8; 4] = [0; 4];
    reader.read_exact(&mut bytes).unwrap();
    let _size = u32::from_be_bytes(bytes);

    read_rows(&mut reader, schema)
}

/// Read row tuples
fn read_rows<R>(mut reader: R, schema: &Schema) -> Result<RecordBatch>
where
    R: Read,
{
    let mut is_done = false;
    let field_len = schema.fields().len();

    let mut buffers: Vec<Vec<u8>> = vec![vec![]; field_len];
    let mut null_buffers: Vec<Vec<bool>> = vec![vec![]; field_len];
    let mut offset_buffers: Vec<Vec<i32>> = vec![vec![]; field_len];

    let default_values: Vec<Vec<u8>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Null => vec![],
            DataType::Boolean => vec![0],
            DataType::Int8 => vec![0],
            DataType::Int16 => vec![0; 2],
            DataType::Int32 => vec![0; 4],
            DataType::Int64 => vec![0; 8],
            DataType::UInt8 => vec![0],
            DataType::UInt16 => vec![0; 2],
            DataType::UInt32 => vec![0; 4],
            DataType::UInt64 => vec![0; 8],
            DataType::Float16 => vec![0; 2],
            DataType::Float32 => vec![0; 4],
            DataType::Float64 => vec![0; 8],
            DataType::Timestamp(_, _) => vec![0; 8],
            DataType::Date32(_) => vec![0; 4],
            DataType::Date64(_) => vec![0; 8],
            DataType::Time32(_) => vec![0; 4],
            DataType::Time64(_) => vec![0; 8],
            DataType::Duration(_) => vec![0; 8],
            DataType::Interval(_) => vec![0; 8],
            DataType::Binary => vec![],
            DataType::FixedSizeBinary(len) => vec![0; *len as usize],
            DataType::Utf8 => vec![],
            DataType::List(_) => vec![],
            DataType::FixedSizeList(_, len) => vec![0; *len as usize],
            DataType::Struct(_) => vec![],
            DataType::Union(_) => vec![],
            DataType::Dictionary(_, _) => vec![],
            DataType::LargeBinary => vec![],
            DataType::LargeUtf8 => vec![],
            DataType::LargeList(_) => vec![],
            DataType::Decimal(_, _) => vec![],
        })
        .collect();

    let mut record_num = -1;
    while !is_done {
        record_num += 1;
        // tuple length
        let mut bytes: [u8; 2] = [0; 2];
        reader.read_exact(&mut bytes).unwrap();
        let size = i16::from_be_bytes(bytes);
        if size == -1 {
            // trailer
            is_done = true;
            continue;
        }
        let size = size as usize;
        // in almost all cases, the tuple length should equal schema field length
        assert_eq!(size, field_len);
        for i in 0..field_len {
            let mut bytes = [0u8; 4];
            reader.read_exact(&mut bytes).unwrap();
            let col_length = i32::from_be_bytes(bytes);
            // populate offsets for types that need them
            match schema.field(i).data_type() {
                DataType::Binary | DataType::Utf8 => {
                    offset_buffers[i].push(col_length);
                }
                DataType::FixedSizeBinary(binary_size) => {
                    offset_buffers[i].push(record_num * binary_size);
                }
                DataType::List(_) => {}
                DataType::FixedSizeList(_, _) => {}
                DataType::Struct(_) => {}
                _ => {}
            }
            // populate values
            if col_length == -1 {
                // null value
                null_buffers[i].push(false);
                buffers[i].write_all(default_values[i].as_slice())?;
            } else {
                null_buffers[i].push(true);
                // big endian data, needs to be converted to little endian
                let mut data = read_col(
                    &mut reader,
                    schema.field(i).data_type(),
                    col_length as usize,
                )
                .expect("Unable to read data");
                buffers[i].append(&mut data);
            }
        }
    }

    let mut arrays = vec![];
    // build record batches
    buffers
        .into_iter()
        .zip(null_buffers.into_iter())
        .zip(schema.fields().iter())
        .enumerate()
        .for_each(|(i, ((b, n), f))| {
            let null_count = n.iter().filter(|v| v == &&false).count();
            let mut null_buffer = BooleanBufferBuilder::new(n.len() / 8 + 1);
            null_buffer.append_slice(&n[..]).unwrap();
            let null_buffer = null_buffer.finish();
            match f.data_type() {
                DataType::Boolean => {
                    let bools = b.iter().map(|v| v == &0).collect::<Vec<bool>>();
                    let mut bool_buffer = BooleanBufferBuilder::new(bools.len());
                    bool_buffer.append_slice(&bools[..]).unwrap();
                    let bool_buffer = bool_buffer.finish();
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![bool_buffer],
                        vec![],
                    );
                    arrays.push(Arc::new(BooleanArray::from(Arc::new(data))) as ArrayRef)
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date32(_)
                | DataType::Date64(_)
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(arrow::array::make_array(Arc::new(data)))
                }
                DataType::FixedSizeBinary(_size) => {
                    // TODO: do we need to calculate any offsets?
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(arrow::array::make_array(Arc::new(data)))
                }
                DataType::Binary
                | DataType::Utf8
                | DataType::LargeBinary
                | DataType::LargeUtf8 => {
                    // recontruct offsets
                    let mut offset = 0;
                    let mut offsets = vec![0];
                    offset_buffers[i].iter().for_each(|o| {
                        offsets.push(offset + o);
                        offset += o;
                    });
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(offsets.to_byte_slice()), Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(arrow::array::make_array(Arc::new(data)))
                }
                DataType::List(_) | DataType::LargeList(_) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(arrow::array::make_array(Arc::new(data)))
                }
                DataType::FixedSizeList(_, _) => {
                    let data = ArrayData::new(
                        f.data_type().clone(),
                        n.len(),
                        Some(null_count),
                        Some(null_buffer),
                        0,
                        vec![Buffer::from(b)],
                        vec![],
                    );
                    arrays.push(arrow::array::make_array(Arc::new(data)))
                }
                DataType::Float16 => panic!("Float16 not yet implemented"),
                DataType::Struct(_) => panic!("Reading struct arrays not implemented"),
                DataType::Dictionary(_, _) => {
                    panic!("Reading dictionary arrays not implemented")
                }
                DataType::Union(_) => panic!("Union not supported"),
                DataType::Null => panic!("Null not supported"),
                DataType::Decimal(_, _) => panic!("Decimal not suported"),
            }
        });
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays)?)
}

fn read_col<R: Read>(
    reader: &mut R,
    data_type: &DataType,
    length: usize,
) -> Result<Vec<u8>> {
    match data_type {
        DataType::Boolean => read_bool(reader),
        DataType::Int8 => read_i8(reader),
        DataType::Int16 => read_i16(reader),
        DataType::Int32 => read_i32(reader),
        DataType::Int64 => read_i64(reader),
        DataType::UInt8 => read_u8(reader),
        DataType::UInt16 => read_u16(reader),
        DataType::UInt32 => read_u32(reader),
        DataType::UInt64 => read_u64(reader),
        DataType::Float16 => Err(ArrowError::InvalidArgumentError(
            "Float 16 is not yet supported".to_string(),
        )),
        DataType::Float32 => read_f32(reader),
        DataType::Float64 => read_f64(reader),
        DataType::Timestamp(_, _) => read_timestamp64(reader),
        DataType::Date32(_) => read_date32(reader),
        DataType::Date64(_) => unreachable!(),
        DataType::Time32(_) => read_i32(reader),
        DataType::Time64(_) => read_time64(reader),
        DataType::Duration(_) => read_i64(reader),
        DataType::Interval(_) => read_i64(reader),
        DataType::Binary => read_string(reader, length), // TODO we'd need the length of the binary
        DataType::FixedSizeBinary(_) => read_string(reader, length),
        DataType::Utf8 => read_string(reader, length),
        DataType::List(_) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::FixedSizeList(_, _) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::Struct(_) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::Dictionary(_, _) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::Union(_) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::Null => Err(UNSUPPORTED_TYPE_ERR),
        DataType::LargeBinary => Err(UNSUPPORTED_TYPE_ERR),
        DataType::LargeUtf8 => Err(UNSUPPORTED_TYPE_ERR),
        DataType::LargeList(_) => Err(UNSUPPORTED_TYPE_ERR),
        DataType::Decimal(_, _) => Err(UNSUPPORTED_TYPE_ERR),
    }
}

fn read_u8<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_u8()
        .map(|v| vec![v])
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}

fn read_i8<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i8()
        .map(|v| vec![v as u8])
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}

fn read_u16<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_u16::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_i16<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i16::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_u32<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_u32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_i32<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_u64<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_u64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_i64<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_bool<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_u8()
        .map(|v| vec![v])
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_string<R: Read>(reader: &mut R, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0; len];
    reader
        .read_exact(&mut buf)
        .map_err(|e| ArrowError::SqlError(e.to_string()))?;
    Ok(buf)
}
fn read_f32<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_f32::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
fn read_f64<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_f64::<NetworkEndian>()
        .map(|v| v.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}

/// Postgres dates are days since epoch of 01-01-2000, so we add 10957 days
fn read_date32<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i32::<NetworkEndian>()
        .map(|v| { EPOCH_DAYS + v }.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}

fn read_timestamp64<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i64::<NetworkEndian>()
        .map(|v| { EPOCH_MICROS + v }.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}

/// we do not support time with time zone as it is 48-bit,
/// time without a zone is 32-bit but arrow only supports 64-bit at microsecond resolution
fn read_time64<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    reader
        .read_i32::<NetworkEndian>()
        .map(|v| { v as i64 }.to_le_bytes().to_vec())
        .map_err(|e| ArrowError::SqlError(e.to_string()))
}
