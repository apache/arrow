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

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and values
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> Vec<FlightData> {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, &options)
        .expect("DictionaryTracker configured above to not error on replacement");

    encoded_dictionaries
        .into_iter()
        .chain(std::iter::once(encoded_batch))
        .map(|data| FlightData {
            flight_descriptor: None,
            app_metadata: vec![],
            data_header: data.ipc_message,
            data_body: data.arrow_data,
        })
        .collect()
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
pub fn flight_schema_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> SchemaResult {
    let data_gen = writer::IpcDataGenerator::default();
    let schema_bytes = data_gen.schema_to_bytes(schema, &options);

    SchemaResult {
        schema: schema_bytes.ipc_message,
    }
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
pub fn flight_data_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> FlightData {
    let data_gen = writer::IpcDataGenerator::default();
    let schema = data_gen.schema_to_bytes(schema, &options);
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
        convert::schema_from_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight data to Arrow schema: {}",
                err
            ))
        })
    }
}

/// Try convert `SchemaResult` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> Result<Self> {
        convert::schema_from_bytes(&data.schema[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert schema result to Arrow schema: {}",
                err
            ))
        })
    }
}

/// Convert a FlightData message to a RecordBatch
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: SchemaRef,
) -> Option<Result<RecordBatch>> {
    // check that the data_header is a record batch message
    let res = arrow::ipc::root_as_message(&data.data_header[..]);

    // Catch error.
    if let Err(err) = res {
        return Some(Err(ArrowError::ParseError(format!(
            "Unable to get root as message: {:?}",
            err
        ))));
    }

    let message = res.unwrap();

    let dictionaries_by_field = Vec::new();

    message
        .header_as_record_batch()
        .ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert flight data header to a record batch".to_string(),
            )
        })
        .map_or_else(
            |err| Some(Err(err)),
            |batch| {
                Some(reader::read_record_batch(
                    &data.data_body,
                    batch,
                    schema,
                    &dictionaries_by_field,
                ))
            },
        )
}

// TODO: add more explicit conversion that exposes flight descriptor and metadata options
