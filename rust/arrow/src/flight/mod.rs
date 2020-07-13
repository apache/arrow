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

//! Utilities to assist with reading and writing Arrow data as Flight messages

use std::convert::TryFrom;

use flight::{FlightData, SchemaResult};

use crate::datatypes::{Schema, SchemaRef};
use crate::error::{ArrowError, Result};
use crate::ipc::{convert, reader, writer};
use crate::record_batch::RecordBatch;

/// Convert a `RecordBatch` to `FlightData` by getting the header and body as bytes
impl From<&RecordBatch> for FlightData {
    fn from(batch: &RecordBatch) -> Self {
        let (header, body) = writer::record_batch_to_bytes(batch);
        Self {
            flight_descriptor: None,
            app_metadata: vec![],
            data_header: header,
            data_body: body,
        }
    }
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
impl From<&Schema> for SchemaResult {
    fn from(schema: &Schema) -> Self {
        Self {
            schema: writer::schema_to_bytes(schema),
        }
    }
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
impl From<&Schema> for FlightData {
    fn from(schema: &Schema) -> Self {
        let schema = writer::schema_to_bytes(schema);
        Self {
            flight_descriptor: None,
            app_metadata: vec![],
            data_header: schema,
            data_body: vec![],
        }
    }
}

/// Try convert `FlightData` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> Result<Self> {
        convert::schema_from_bytes(&data.data_header[..]).ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert flight data to Arrow schema".to_string(),
            )
        })
    }
}

/// Try convert `SchemaResult` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> Result<Self> {
        convert::schema_from_bytes(&data.schema[..]).ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert schema result to Arrow schema".to_string(),
            )
        })
    }
}

/// Convert a FlightData message to a RecordBatch
pub fn flight_data_to_batch(
    data: &FlightData,
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    // check that the data_header is a record batch message
    let message = crate::ipc::get_root_as_message(&data.data_header[..]);
    let dictionaries_by_field = Vec::new();
    let batch_header = message.header_as_record_batch().ok_or_else(|| {
        ArrowError::ParseError(
            "Unable to convert flight data header to a record batch".to_string(),
        )
    })?;
    reader::read_record_batch(
        &data.data_body,
        batch_header,
        schema,
        &dictionaries_by_field,
    )
}

// TODO: add more explicit conversion that expoess flight descriptor and metadata options
