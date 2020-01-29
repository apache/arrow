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

use flight::FlightData;

use crate::ipc::writer;
use crate::record_batch::RecordBatch;

/// Convert a `RecordBatch` to `FlightData by getting the header and body as bytes
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

// TODO: add more explicit conversion that expoess flight descriptor and metadata options
