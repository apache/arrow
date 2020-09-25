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

use crate::{FlightData, SchemaResult};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::ipc::{convert, reader, writer, writer::IpcWriteOptions};
use arrow::record_batch::RecordBatch;

/// Convert a `RecordBatch` to `FlightData` by converting the header and body to bytes
///
/// Note: This implicitly uses the default `IpcWriteOptions`. To configure options,
/// use `flight_data_from_arrow_batch()`
impl From<&RecordBatch> for FlightData {
    fn from(batch: &RecordBatch) -> Self {
        let options = IpcWriteOptions::default();
        flight_data_from_arrow_batch(batch, &options)
    }
}

/// Convert a `RecordBatch` to `FlightData` by converting the header and body to bytes
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> FlightData {
    let data = writer::record_batch_to_bytes(batch, &options);
    FlightData {
        flight_descriptor: None,
        app_metadata: vec![],
        data_header: data.ipc_message,
        data_body: data.arrow_data,
    }
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
///
/// Note: This implicitly uses the default `IpcWriteOptions`. To configure options,
/// use `flight_schema_from_arrow_schema()`
impl From<&Schema> for SchemaResult {
    fn from(schema: &Schema) -> Self {
        let options = IpcWriteOptions::default();
        flight_schema_from_arrow_schema(schema, &options)
    }
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
pub fn flight_schema_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> SchemaResult {
    SchemaResult {
        schema: writer::schema_to_bytes(schema, &options).ipc_message,
    }
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
///
/// Note: This implicitly uses the default `IpcWriteOptions`. To configure options,
/// use `flight_data_from_arrow_schema()`
impl From<&Schema> for FlightData {
    fn from(schema: &Schema) -> Self {
        let options = writer::IpcWriteOptions::default();
        flight_data_from_arrow_schema(schema, &options)
    }
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
pub fn flight_data_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> FlightData {
    let schema = writer::schema_to_bytes(schema, &options);
    FlightData {
        flight_descriptor: None,
        app_metadata: vec![],
        data_header: schema.ipc_message,
        data_body: vec![],
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
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    // check that the data_header is a record batch message
    let message = arrow::ipc::get_root_as_message(&data.data_header[..]);
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
