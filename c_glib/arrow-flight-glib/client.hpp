/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/flight/api.h>

#include <arrow-flight-glib/client.h>

GAFLIGHT_EXTERN
GAFlightStreamReader *
gaflight_stream_reader_new_raw(arrow::flight::FlightStreamReader *flight_reader,
                               gboolean is_owner);

GAFLIGHT_EXTERN
GAFlightStreamWriter *
gaflight_stream_writer_new_raw(
  std::shared_ptr<arrow::flight::FlightStreamWriter> *flight_writer);

GAFLIGHT_EXTERN
GAFlightMetadataReader *
gaflight_metadata_reader_new_raw(arrow::flight::FlightMetadataReader *flight_reader);

GAFLIGHT_EXTERN
arrow::flight::FlightMetadataReader *
gaflight_metadata_reader_get_raw(GAFlightMetadataReader *reader);

GAFLIGHT_EXTERN
arrow::flight::FlightCallOptions *
gaflight_call_options_get_raw(GAFlightCallOptions *options);

GAFLIGHT_EXTERN
arrow::flight::FlightClientOptions *
gaflight_client_options_get_raw(GAFlightClientOptions *options);

GAFLIGHT_EXTERN
GAFlightDoPutResult *
gaflight_do_put_result_new_raw(arrow::flight::FlightClient::DoPutResult *flight_result);

GAFLIGHT_EXTERN
std::shared_ptr<arrow::flight::FlightClient>
gaflight_client_get_raw(GAFlightClient *client);

GAFLIGHT_EXTERN
GAFlightClient *
gaflight_client_new_raw(std::shared_ptr<arrow::flight::FlightClient> *flight_client);
