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

//! Implement [`Record`] for [`Decimal`].

use std::collections::HashMap;

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    data_type::{ByteArray, Decimal},
    errors::Result,
    record::{
        schemas::{DecimalSchema, I32Schema, I64Schema},
        types::{downcast, Value},
        Reader, Record,
    },
    schema::types::{ColumnPath, Type},
};

// [`Decimal`] corresponds to the [Decimal logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal).
impl Record for Decimal {
    type Schema = DecimalSchema;
    type Reader = impl Reader<Item = Self>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        Value::parse(schema, repetition).and_then(downcast)
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        match *schema {
            DecimalSchema::Int32 { precision, scale } => DecimalReader::Int32 {
                reader: i32::reader(
                    &I32Schema, path, def_level, rep_level, paths, batch_size,
                ),
                precision,
                scale,
            },
            DecimalSchema::Int64 { precision, scale } => DecimalReader::Int64 {
                reader: i64::reader(
                    &I64Schema, path, def_level, rep_level, paths, batch_size,
                ),
                precision,
                scale,
            },
            DecimalSchema::Array {
                ref byte_array_schema,
                precision,
                scale,
            } => DecimalReader::Array {
                reader: <Vec<u8>>::reader(
                    byte_array_schema,
                    path,
                    def_level,
                    rep_level,
                    paths,
                    batch_size,
                ),
                precision,
                scale,
            },
        }
    }
}

pub enum DecimalReader {
    Int32 {
        reader: <i32 as Record>::Reader,
        precision: u8,
        scale: u8,
    },
    Int64 {
        reader: <i64 as Record>::Reader,
        precision: u8,
        scale: u8,
    },
    Array {
        reader: <Vec<u8> as Record>::Reader,
        precision: u32,
        scale: u32,
    },
}

impl Reader for DecimalReader {
    type Item = Decimal;

    #[inline]
    fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
        match self {
            DecimalReader::Int32 {
                reader,
                precision,
                scale,
            } => reader
                .read(def_level, rep_level)
                .map(|bytes| Decimal::from_i32(bytes, *precision as i32, *scale as i32)),
            DecimalReader::Int64 {
                reader,
                precision,
                scale,
            } => reader
                .read(def_level, rep_level)
                .map(|bytes| Decimal::from_i64(bytes, *precision as i32, *scale as i32)),
            DecimalReader::Array {
                reader,
                precision,
                scale,
            } => reader.read(def_level, rep_level).map(|bytes| {
                Decimal::from_bytes(
                    ByteArray::from(bytes),
                    *precision as i32,
                    *scale as i32,
                )
            }),
        }
    }

    #[inline]
    fn advance_columns(&mut self) -> Result<()> {
        match self {
            DecimalReader::Int32 { reader, .. } => reader.advance_columns(),
            DecimalReader::Int64 { reader, .. } => reader.advance_columns(),
            DecimalReader::Array { reader, .. } => reader.advance_columns(),
        }
    }

    #[inline]
    fn has_next(&self) -> bool {
        match self {
            DecimalReader::Int32 { reader, .. } => reader.has_next(),
            DecimalReader::Int64 { reader, .. } => reader.has_next(),
            DecimalReader::Array { reader, .. } => reader.has_next(),
        }
    }

    #[inline]
    fn current_def_level(&self) -> i16 {
        match self {
            DecimalReader::Int32 { reader, .. } => reader.current_def_level(),
            DecimalReader::Int64 { reader, .. } => reader.current_def_level(),
            DecimalReader::Array { reader, .. } => reader.current_def_level(),
        }
    }

    #[inline]
    fn current_rep_level(&self) -> i16 {
        match self {
            DecimalReader::Int32 { reader, .. } => reader.current_rep_level(),
            DecimalReader::Int64 { reader, .. } => reader.current_rep_level(),
            DecimalReader::Array { reader, .. } => reader.current_rep_level(),
        }
    }
}
